/**
 * Nexum client — connect, authenticate, subscribe, call reducers.
 *
 * Usage:
 *   const client = new NexumClient("ws://localhost:9337");
 *   await client.connect();
 *   await client.authenticate("token");
 *   await client.attach(0n);
 *   const view = client.subscribe<PlayerRow>("players", 32);
 *   await client.callReducer("move_player", { dx: 1, dy: 0 });
 */

import { ByteReader, ByteWriter } from "./codec";
import { ClientMsgKind, ServerMsgKind, PROTOCOL_VERSION } from "./protocol";

export class NexumError extends Error {
  constructor(message: string, public code?: number) {
    super(message);
  }
}

export interface SubscriptionUpdate {
  tag: number;
  seq: bigint;
  rowId?: bigint;
  row?: Record<string, unknown>;
  rows?: Array<{ rid: bigint; values: Record<string, unknown> }>;
}

type EventHandler = (data: any) => void;

export class NexumClient {
  private ws: WebSocket | null = null;
  private nextRequestId = 1;
  private pendingCalls = new Map<number, {
    resolve: (value: any) => void;
    reject: (err: Error) => void;
  }>();
  private subscriptions = new Map<number, EventHandler>();
  private eventHandlers: EventHandler[] = [];
  private connected_ = false;
  private authenticated_ = false;
  private attachedWorld_: bigint | null = null;

  constructor(public url: string) {}

  get connected() { return this.connected_; }
  get authenticated() { return this.authenticated_; }
  get attachedWorld() { return this.attachedWorld_; }

  // ─── Connection ───────────────────────────────────────────────────────

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);
      this.ws.binaryType = "arraybuffer";
      this.ws.onopen = () => {
        this.sendHandshake();
      };
      this.ws.onmessage = (ev) => this.handleMessage(new Uint8Array(ev.data as ArrayBuffer));
      this.ws.onerror = (e) => reject(new NexumError("connection failed"));
      this.ws.onclose = () => { this.connected_ = false; };
    });
  }

  async close(): Promise<void> {
    this.ws?.close();
    this.connected_ = false;
  }

  // ─── Authentication ───────────────────────────────────────────────────

  async authenticate(token: string): Promise<void> {
    const w = new ByteWriter(64 + token.length);
    w.u16(ClientMsgKind.Authenticate);
    w.str(token);
    this.sendRaw(w.buffer);
    // AuthResult arrives asynchronously via handleMessage
  }

  async attach(worldId: bigint): Promise<void> {
    const w = new ByteWriter(16);
    w.u16(ClientMsgKind.AttachWorld);
    w.u64(worldId);
    this.sendRaw(w.buffer);
    this.attachedWorld_ = worldId;
  }

  // ─── Reducer calls ────────────────────────────────────────────────────

  callReducer(name: string, args: Record<string, unknown>): Promise<any> {
    const requestId = this.nextRequestId++;
    const w = new ByteWriter(128);
    w.u16(ClientMsgKind.CallReducer);
    w.u64(BigInt(requestId));
    w.str(name);
    // args encoding
    const keys = Object.keys(args).sort();
    w.u64(BigInt(keys.length));
    for (const key of keys) {
      w.str(key);
      writeValueToWriter(w, args[key]);
    }
    this.sendRaw(w.buffer);

    return new Promise((resolve, reject) => {
      this.pendingCalls.set(requestId, { resolve, reject });
    });
  }

  sendInput(commands: Array<{ kind: string; payload?: number }>, tick?: bigint) {
    const w = new ByteWriter(256);
    w.u16(ClientMsgKind.InputFrame);
    w.u64(tick ?? 0n);
    w.u64(BigInt(commands.length));
    for (const cmd of commands) {
      w.u64(0n); // source (gateway stamps)
      w.str(cmd.kind);
      if (cmd.payload !== undefined) {
        w.u8(1); writeValueToWriter(w, cmd.payload);
      } else {
        w.u8(0);
      }
    }
    this.sendRaw(w.buffer);
  }

  fireAndForget(name: string, args: Record<string, unknown>) {
    const requestId = this.nextRequestId++;
    const w = new ByteWriter(128);
    w.u16(ClientMsgKind.CallReducer);
    w.u64(BigInt(requestId));
    w.str(name);
    const keys = Object.keys(args).sort();
    w.u64(BigInt(keys.length));
    for (const key of keys) {
      w.str(key);
      writeValueToWriter(w, args[key]);
    }
    this.sendRaw(w.buffer);
  }

  // ─── Subscriptions ────────────────────────────────────────────────────

  subscribe(table: string, limit = 32): Map<bigint, Record<string, unknown>> {
    const view = new Map<bigint, Record<string, unknown>>();
    const subId = this.nextRequestId++;
    const w = new ByteWriter(128);
    w.u16(ClientMsgKind.Subscribe);
    w.u64(BigInt(subId));
    w.str(table);       // table name
    w.u64(0n);          // predicate count
    w.u8(0);            // no order
    w.u8(1);            // has limit
    w.u64(BigInt(limit));
    this.sendRaw(w.buffer);

    this.subscriptions.set(subId, (data: SubscriptionUpdate[]) => {
      for (const u of data) {
        switch (u.tag) {
          case 1: // Insert
            if (u.rowId !== undefined && u.row) view.set(u.rowId, u.row);
            break;
          case 2: // Update
            if (u.rowId !== undefined && u.row) view.set(u.rowId, u.row);
            break;
          case 3: // Delete
            if (u.rowId !== undefined) view.delete(u.rowId);
            break;
        }
      }
    });

    return view;
  }

  onEvent(handler: EventHandler) {
    this.eventHandlers.push(handler);
  }

  // ─── Internal ─────────────────────────────────────────────────────────

  private sendRaw(data: Uint8Array) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new NexumError("not connected");
    }
    // Frame: [length u32][payload][crc32 u32]
    const frame = new Uint8Array(4 + data.length + 4);
    const dv = new DataView(frame.buffer);
    dv.setUint32(0, data.length, true);
    frame.set(data, 4);
    dv.setUint32(4 + data.length, crc32(data), true);
    this.ws.send(frame.buffer);
  }

  private sendHandshake() {
    const w = new ByteWriter(64);
    w.u16(ClientMsgKind.Handshake);
    w.u16(PROTOCOL_VERSION);
    w.str("nexum-ts-sdk");
    this.sendRaw(w.buffer);
    this.connected_ = true;
  }

  private handleMessage(data: Uint8Array) {
    if (data.length < 4) return;
    const r = new ByteReader(data.subarray(0, data.length - 4)); // strip CRC
    const kind = r.u16();

    switch (kind as ServerMsgKind) {
      case ServerMsgKind.HandshakeResponse:
        break; // already connected
      case ServerMsgKind.AuthResult:
        this.authenticated_ = r.u8() !== 0;
        break;
      case ServerMsgKind.AttachResult:
        break; // attach confirmed via attached_world field
      case ServerMsgKind.SubscriptionDelta:
        this.handleSubscriptionDelta(r);
        break;
      case ServerMsgKind.ReducerResult: {
        const requestId = Number(r.u64());
        const ok = r.u8() !== 0;
        const pending = this.pendingCalls.get(requestId);
        if (pending) {
          this.pendingCalls.delete(requestId);
          if (ok) pending.resolve(undefined);
          else pending.reject(new NexumError("reducer call failed"));
        }
        break;
      }
      default:
        break;
    }
  }

  private handleSubscriptionDelta(r: ByteReader) {
    const subId = Number(r.u64());
    const handler = this.subscriptions.get(subId);
    if (!handler) return;

    const updates: any[] = [];
    const count = r.u64();
    for (let i = 0; i < count; i++) {
      const tag = r.u8();
      const seq = r.u64();
      switch (tag) {
        case 1: case 2: { // Insert / Update
          const rid = r.u64();
          const nv = r.u64();
          const row: Record<string, unknown> = {};
          for (let k = 0; k < nv; k++) {
            const value = readValueFromReader(r);
            row[`col${k}`] = value;
          }
          updates.push({ tag, seq, rowId: rid, row });
          break;
        }
        case 3: { // Delete
          const rid = r.u64();
          updates.push({ tag, seq, rowId: rid });
          break;
        }
      }
    }
    handler(updates);
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────

function writeValueToWriter(w: ByteWriter, v: unknown) {
  if (typeof v === "number") {
    w.u8(4); // I64 tag
    w.i64(BigInt(v));
  } else if (typeof v === "bigint") {
    w.u8(8); // U64 tag
    w.u64(v);
  } else if (typeof v === "string") {
    w.u8(11); // String tag
    w.str(v);
  } else if (typeof v === "boolean") {
    w.u8(0); // Bool tag
    w.u8(v ? 1 : 0);
  }
}

function readValueFromReader(r: ByteReader): unknown {
  const tag = r.u8();
  switch (tag) {
    case 0: return r.u8() !== 0;
    case 4: return r.i64();
    case 8: return r.u64();
    case 10: return r.f64();
    case 11: return r.str();
    default: return undefined;
  }
}

function crc32(data: Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc ^= data[i];
    for (let j = 0; j < 8; j++) {
      if (crc & 1) crc = (crc >>> 1) ^ 0xEDB88320;
      else crc >>>= 1;
    }
  }
  return ~crc >>> 0;
}
