/**
 * Binary codec for the Nexum wire protocol.
 * All integers little-endian. Strings are [len u32][utf8].
 */

import { ColumnType } from "./protocol";

// ─── Writer ──────────────────────────────────────────────────────────────

export class ByteWriter {
  private buf: DataView;
  private arr: Uint8Array;
  private offset = 0;

  constructor(initialCapacity = 256) {
    this.arr = new Uint8Array(initialCapacity);
    this.buf = new DataView(this.arr.buffer);
  }

  private ensure(extra: number) {
    if (this.offset + extra > this.arr.byteLength) {
      const newSize = Math.max(this.arr.byteLength * 2, this.offset + extra);
      const newArr = new Uint8Array(newSize);
      newArr.set(this.arr.subarray(0, this.offset));
      this.arr = newArr;
      this.buf = new DataView(this.arr.buffer);
    }
  }

  u8(v: number) { this.ensure(1); this.buf.setUint8(this.offset, v); this.offset += 1; }
  u16(v: number) { this.ensure(2); this.buf.setUint16(this.offset, v, true); this.offset += 2; }
  u32(v: number) { this.ensure(4); this.buf.setUint32(this.offset, v, true); this.offset += 4; }
  u64(v: bigint) { this.ensure(8); this.buf.setBigUint64(this.offset, v, true); this.offset += 8; }
  i64(v: bigint) { this.ensure(8); this.buf.setBigInt64(this.offset, v, true); this.offset += 8; }
  f64(v: number) { this.ensure(8); this.buf.setFloat64(this.offset, v, true); this.offset += 8; }
  bytes(b: Uint8Array) { this.ensure(b.length); this.arr.set(b, this.offset); this.offset += b.length; }

  str(s: string) {
    const encoded = new TextEncoder().encode(s);
    this.u32(encoded.length);
    this.bytes(encoded);
  }

  get buffer(): Uint8Array {
    return this.arr.subarray(0, this.offset);
  }
}

// ─── Reader ──────────────────────────────────────────────────────────────

export class ByteReader {
  private view: DataView;
  private offset = 0;

  constructor(public data: Uint8Array) {
    this.view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }

  u8(): number { const v = this.view.getUint8(this.offset); this.offset += 1; return v; }
  u16(): number { const v = this.view.getUint16(this.offset, true); this.offset += 2; return v; }
  u32(): number { const v = this.view.getUint32(this.offset, true); this.offset += 4; return v; }
  u64(): bigint { const v = this.view.getBigUint64(this.offset, true); this.offset += 8; return v; }
  i64(): bigint { const v = this.view.getBigInt64(this.offset, true); this.offset += 8; return v; }
  f64(): number { const v = this.view.getFloat64(this.offset, true); this.offset += 8; return v; }

  bytes(n: number): Uint8Array {
    const slice = this.data.subarray(this.offset, this.offset + n);
    this.offset += n;
    return slice;
  }

  str(): string {
    const len = this.u32();
    return new TextDecoder().decode(this.bytes(len));
  }

  get remaining(): number { return this.data.byteLength - this.offset; }
}

// ─── Value encode/decode ────────────────────────────────────────────────

export type NexumValue =
  | boolean | number | bigint | string | Uint8Array;

export function writeValue(w: ByteWriter, v: unknown): void {
  if (typeof v === "boolean") {
    w.u8(ColumnType.Bool); w.u8(v ? 1 : 0);
  } else if (typeof v === "number" && Number.isInteger(v)) {
    w.u8(ColumnType.I64); w.i64(BigInt(v));
  } else if (typeof v === "bigint") {
    w.u8(ColumnType.U64); w.u64(v);
  } else if (typeof v === "string") {
    w.u8(ColumnType.String); w.str(v);
  } else {
    throw new Error(`unsupported value type: ${typeof v}`);
  }
}

export function readValue(r: ByteReader): NexumValue {
  const tag = r.u8();
  switch (tag as ColumnType) {
    case ColumnType.Bool: return r.u8() !== 0;
    case ColumnType.I64: return r.i64();
    case ColumnType.U64: return r.u64();
    case ColumnType.F64: return r.f64();
    case ColumnType.String: return r.str();
    default: throw new Error(`unsupported column type tag: ${tag}`);
  }
}
