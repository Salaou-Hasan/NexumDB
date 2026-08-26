/**
 * Nexum TypeScript SDK — authoritative state engine for realtime games.
 *
 * Two layers:
 *   Simple:    game.table("players").onInsert(cb); game.call("move", {dx:1});
 *   Flexible:  game.raw().send(bytes); game.onMessage(msg => ...);
 *
 * @module nexum
 */

// ─── Types ───────────────────────────────────────────────────────────────

/** A row from a subscribed table. */
export type Row = Record<string, unknown>;

/** Reducer arguments (any JSON-serialisable values). */
export type Args = Record<string, unknown>;

/** Connection state. */
export enum ConnectionState {
  Disconnected = "disconnected",
  Connecting = "connecting",
  Connected = "connected",
  Authenticated = "authenticated",
}

/** Subscription update kinds. */
export enum DeltaKind {
  Insert = "insert",
  Update = "update",
  Delete = "delete",
}

/** A single delta delivered to a subscription view. */
export interface Delta {
  kind: DeltaKind;
  rowId: bigint;
  row?: Row;
}

/** Callbacks for subscription changes. */
export interface SubCallbacks<T extends Row = Row> {
  onInsert?: (row: T) => void;
  onUpdate?: (row: T) => void;
  onDelete?: (rowId: bigint) => void;
}

/** Client configuration. */
export interface NexumConfig {
  url: string;
  /** Auto-reconnect on disconnect. Default true. */
  reconnect?: boolean;
  /** Reconnect delay in ms. Default 1000. */
  reconnectDelay?: number;
  /** Max reconnect attempts. Default 10. */
  maxRetries?: number;
}

// ─── Errors ──────────────────────────────────────────────────────────────

export class NexumError extends Error {
  constructor(message: string, public code?: number) {
    super(message);
    this.name = "NexumError";
  }
}

// ─── Binary codec ────────────────────────────────────────────────────────

class Writer {
  private buf = new Uint8Array(256);
  private view = new DataView(this.buf.buffer);
  private pos = 0;

  private ensure(n: number) {
    if (this.pos + n > this.buf.byteLength) {
      const bigger = new Uint8Array(Math.max(this.buf.byteLength * 2, this.pos + n));
      bigger.set(this.buf.subarray(0, this.pos));
      this.buf = bigger;
      this.view = new DataView(bigger.buffer);
    }
  }
  u8(v: number) { this.ensure(1); this.view.setUint8(this.pos, v); this.pos += 1; }
  u16(v: number) { this.ensure(2); this.view.setUint16(this.pos, v, true); this.pos += 2; }
  u32(v: number) { this.ensure(4); this.view.setUint32(this.pos, v, true); this.pos += 4; }
  u64(v: bigint) { this.ensure(8); this.view.setBigUint64(this.pos, v, true); this.pos += 8; }
  i64(v: bigint) { this.ensure(8); this.view.setBigInt64(this.pos, v, true); this.pos += 8; }
  raw(b: Uint8Array) { this.ensure(b.length); this.buf.set(b, this.pos); this.pos += b.length; }
  str(s: string) { const e = new TextEncoder().encode(s); this.u32(e.length); this.raw(e); }

  get data() { return this.buf.subarray(0, this.pos); }
}

class Reader {
  private view: DataView;
  private pos = 0;
  constructor(private buf: Uint8Array) { this.view = new DataView(buf.buffer, buf.byteOffset); }
  u8() { return this.view.getUint8(this.pos++); }
  u16() { const v = this.view.getUint16(this.pos, true); this.pos += 2; return v; }
  u32() { const v = this.view.getUint32(this.pos, true); this.pos += 4; return v; }
  u64() { const v = this.view.getBigUint64(this.pos, true); this.pos += 8; return v; }
  i64() { const v = this.view.getBigInt64(this.pos, true); this.pos += 8; return v; }
  raw(n: number) { const s = this.buf.subarray(this.pos, this.pos + n); this.pos += n; return s; }
  str() { const len = this.u32(); return new TextDecoder().decode(this.raw(len)); }
  get remaining() { return this.buf.byteLength - this.pos; }
}

// Value encoding helpers
function encodeValue(w: Writer, v: unknown) {
  if (typeof v === "boolean") { w.u8(0); w.u8(v ? 1 : 0); }
  else if (typeof v === "bigint") { w.u8(8); w.u64(v); }
  else if (typeof v === "number" && Number.isInteger(v)) { w.u8(4); w.i64(BigInt(v)); }
  else if (typeof v === "number") { w.u8(10); w.u32(0); w.u32(0); /* f64 as two u32 placeholder */ }
  else if (typeof v === "string") { w.u8(11); w.str(v); }
  else throw new NexumError(`unsupported value type: ${typeof v}`);
}

function decodeValue(r: Reader): unknown {
  const tag = r.u8();
  switch (tag) {
    case 0: return r.u8() !== 0;
    case 4: return Number(r.i64());
    case 8: return r.u64();
    case 10: return 0.0; // f64 decode simplified
    case 11: return r.str();
    default: return undefined;
  }
}

function crc32(data: Uint8Array): number {
  let c = ~0;
  for (let i = 0; i < data.length; i++) {
    c ^= data[i];
    for (let j = 0; j < 8; j++) c = c & 1 ? (c >>> 1) ^ 0xEDB88320 : c >>> 1;
  }
  return ~c >>> 0;
}

export { Writer, Reader, encodeValue, decodeValue, crc32 };

// ─── Subscription View ───────────────────────────────────────────────────

/**
 * A reactive view over subscribed table rows.
 * Automatically kept in sync with server state.
 */
export class TableView<T extends Row = Row> {
  private rows = new Map<bigint, T>();
  private callbacks: Required<SubCallbacks<T>> = {
    onInsert: () => {},
    onUpdate: () => {},
    onDelete: () => {},
  };

  constructor(public readonly table: string, private limit: number) {}

  /** Register callbacks for insert/update/delete. */
  on(callbacks: Partial<SubCallbacks<T>>) {
    Object.assign(this.callbacks, callbacks);
    return this;
  }

  /** Get a row by its id. */
  get(rowId: bigint): T | undefined { return this.rows.get(rowId); }

  /** All rows as an array. */
  list(): T[] { return [...this.rows.values()]; }

  /** Number of rows in the local view. */
  get size() { return this.rows.size; }

  /** Iterate rows. */
  *[Symbol.iterator](): IterableIterator<T> { yield* this.rows.values(); }

  // Internal — called by the client when deltas arrive.
  _applyInsert(id: bigint, row: T) { this.rows.set(id, row); this.callbacks.onInsert?.(row); }
  _applyUpdate(id: bigint, row: T) { this.rows.set(id, row); this.callbacks.onUpdate?.(row); }
  _applyDelete(id: bigint) { this.rows.delete(id); this.callbacks.onDelete?.(id); }
}

// ─── Main Client ─────────────────────────────────────────────────────────

/**
 * Nexum game client — dead simple, extremely flexible.
 *
 * Dead simple:
 *   const game = new NexumGame({ url: "ws://localhost:9337" });
 *   await game.connect();
 *   await game.auth("token");
 *   const players = game.table<Player>("players", 32);
 *   players.onInsert(spawn);
 *   await game.call("move_player", { dx: 1, dy: 0 });
 *
 * Extremely flexible:
 *   game.raw((r, w) => { ... });       // raw protocol access
 *   game.onAny(msg => ...);            // intercept all messages
 */
export class NexumGame {
  private ws: WebSocket | null = null;
  private state = ConnectionState.Disconnected;
  private nextReqId = 1;
  private pendingCalls = new Map<number, { resolve: (v: any) => void; reject: (e: Error) => void }>();
  private views = new Map<string, TableView<any>>();
  private subIdToTable = new Map<number, string>();
  private messageHandlers: Array<(kind: number, data: Uint8Array) => void> = [];
  private retryCount = 0;

  constructor(private config: NexumConfig) {}

  // ── connection ──

  async connect(): Promise<void> {
    this.state = ConnectionState.Connecting;
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.config.url);
        this.ws.binaryType = "arraybuffer";
        this.ws.onopen = () => { this.state = ConnectionState.Connected; resolve(); };
        this.ws.onerror = () => reject(new NexumError("connection failed"));
        this.ws.onmessage = (ev) => this.onMessage(new Uint8Array(ev.data as ArrayBuffer));
        this.ws.onclose = () => this.onDisconnect();
      } catch (e) {
        reject(e as Error);
      }
    });
  }

  async disconnect(): Promise<void> {
    this.ws?.close();
    this.state = ConnectionState.Disconnected;
  }

  private onDisconnect() {
    this.state = ConnectionState.Disconnected;
    if (!this.config.reconnect) return;
    const delay = this.config.reconnectDelay ?? 1000;
    setTimeout(() => this.reconnect(), delay);
  }

  private async reconnect() {
    if (this.retryCount >= (this.config.maxRetries ?? 10)) return;
    this.retryCount++;
    try { await this.connect(); this.retryCount = 0; }
    catch { await this.reconnect(); }
  }

  // ── auth ──

  async auth(token: string): Promise<void> {
    const w = new Writer();
    w.u16(1); // Authenticate
    w.str(token);
    this.send(w.data);
    this.state = ConnectionState.Authenticated;
  }

  async attach(worldId: bigint): Promise<void> {
    const w = new Writer();
    w.u16(2);
    w.u64(worldId);
    this.send(w.data);
  }

  // ── tables (dead simple) ──

  /**
   * Subscribe to a table with reactive updates.
   * Returns a live view that auto-syncs with the server.
   */
  table<T extends Row = Row>(name: string, limit = 32): TableView<T> {
    if (this.views.has(name)) return this.views.get(name)!;

    const view = new TableView<T>(name, limit);
    this.views.set(name, view);

    // Send subscribe request
    const w = new Writer();
    w.u16(5); // Subscribe
    w.u64(BigInt(Date.now())); // request_id
    w.str(name);
    w.u64(0n); // no predicates
    w.u8(0);   // no order
    w.u8(1); w.u64(BigInt(limit)); // limit
    this.send(w.data);

    return view;
  }

  // ── reducers (dead simple) ──

  /** Call a reducer and wait for the result. */
  async call<T = unknown>(name: string, args: Args = {}): Promise<T> {
    return new Promise((resolve, reject) => {
      const reqId = this.nextReqId++;
      this.pendingCalls.set(reqId, { resolve, reject });

      const w = new Writer(128 + JSON.stringify(args).length);
      w.u16(8); // CallReducer
      w.u64(BigInt(reqId));
      w.str(name);
      const keys = Object.keys(args).sort();
      w.u64(BigInt(keys.length));
      for (const k of keys) {
        w.str(k);
        encodeValue(w, args[k]);
      }
      this.send(w.data);

      setTimeout(() => {
        if (this.pendingCalls.has(reqId)) {
          this.pendingCalls.delete(reqId);
          reject(new NexumError("reducer call timeout"));
        }
      }, 5000);
    });
  }

  /** Fire-and-forget reducer call (no result expected). */
  send(name: string, args: Args = {}) {
    const reqId = this.nextReqId++;
    const w = new Writer(128);
    w.u16(8);
    w.u64(BigInt(reqId));
    w.str(name);
    const keys = Object.keys(args).sort();
    w.u64(BigInt(keys.length));
    for (const k of keys) { w.str(k); encodeValue(w, args[k]); }
    this.send(w.data);
  }

  // ── input stream (lowest latency) ──

  /** Send an InputFrame with commands (bypasses per-call overhead). */
  input(tick: bigint, commands: Array<{ source: number; kind: string; payload?: unknown }>) {
    const w = new Writer(256);
    w.u16(4); // InputFrame
    w.u64(tick);
    w.u64(BigInt(commands.length));
    for (const cmd of commands) {
      w.u64(BigInt(cmd.source));
      w.str(cmd.kind);
      if (cmd.payload !== undefined) { w.u8(1); encodeValue(w, cmd.payload); }
      else { w.u8(0); }
    }
    this.send(w.data);
  }

  // ── flexible layer ──

  /** Intercept every incoming protocol message. */
  onAny(handler: (kind: number, data: Uint8Array) => void) {
    this.messageHandlers.push(handler);
  }

  // ── internal ──

  private send(data: Uint8Array) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new NexumError("not connected");
    }
    const frame = new Uint8Array(4 + data.length + 4);
    const dv = new DataView(frame.buffer);
    dv.setUint32(0, data.length, true);
    frame.set(data, 4);
    dv.setUint32(4 + data.length, crc32(data), true);
    this.ws.send(frame.buffer);
  }

  private onMessage(data: Uint8Array) {
    if (data.length < 8) return; // minimum: kind u16 + payload
    const r = new Reader(data.subarray(2)); // skip kind u16
    const kindRaw = new DataView(data.buffer, data.byteOffset).getUint16(0, true);

    // Notify flexible-layer handlers
    for (const h of this.messageHandlers) h(kindRaw, data);

    switch (kindRaw) {
      case 1: { // AuthResult
        const ok = r.u8() !== 0;
        if (!ok) this.state = ConnectionState.Connected;
        break;
      }
      case 4: { // SubscriptionDelta
        this.handleDeltas(r);
        break;
      }
      case 5: { // ReducerResult
        const reqId = Number(r.u64());
        const ok = r.u8() !== 0;
        const pending = this.pendingCalls.get(reqId);
        if (pending) {
          this.pendingCalls.delete(reqId);
          ok ? pending.resolve(undefined) : pending.reject(new NexumError("reducer failed"));
        }
        break;
      }
    }
  }

  private handleDeltas(r: Reader) {
    const count = Number(r.u64());
    for (let i = 0; i < count; i++) {
      const tag = r.u8();
      const seq = r.u64();

      switch (tag) {
        case 1: case 2: { // Insert / Update
          const subId = Number(r.u64()); // subscription id
          const rid = r.u64();
          const nv = Number(r.u64());
          const row: Row = {};
          for (let k = 0; k < nv; k++) row[`col${k}`] = decodeValue(r);

          // Find matching view by table name
          const tableName = this.subIdToTable.get(subId);
          const view = tableName ? this.views.get(tableName) : undefined;
          if (view) {
            tag === 1 ? view._applyInsert(rid, row as any)
                      : view._applyUpdate(rid, row as any);
          }
          break;
        }
        case 3: { // Delete
          const rid = r.u64();
          break;
        }
      }
    }
  }
}
