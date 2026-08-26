# Nexum Schema Definition Format v1

A Nexum schema describes every table, reducer, and system in your game.
It is the single source of truth for:

- Server-side table creation (`TableSchema` registration)
- Client-side type generation (TypeScript interfaces, C# classes)
- Reducer proxy generation (typed call methods per language)
- Subscription view types (reactive row bindings)
- Validation rules (column types, PK constraints)

## Format

JSON file conventionally named `nexum.schema.json`.

## Example — arena game

```json
{
  "$schema": "https://nexum.dev/schema/v1",
  "name": "arena",
  "version": 1,
  "tables": {
    "players": {
      "columns": {
        "id":        { "type": "u64",   "primary_key": true },
        "x":         { "type": "i64" },
        "y":         { "type": "i64" },
        "hp":        { "type": "i64",   "default": 100 },
        "max_hp":    { "type": "i64",   "default": 100 },
        "alive":     { "type": "i64",   "default": 1 },
        "score":     { "type": "i64",   "default": 0 },
        "cooldown":  { "type": "i64",   "default": 0 },
        "facing":    { "type": "i64",   "default": 0 },
        "ammo":      { "type": "i64",   "default": 10 },
        "connected": { "type": "i64",   "default": 1 }
      },
      "indexes": {
        "pos": { "columns": ["x", "y"], "unique": false }
      }
    },
    "units": {
      "columns": {
        "id":    { "type": "u64", "primary_key": true },
        "owner": { "type": "u64" },
        "x":     { "type": "i64" },
        "y":     { "type": "i64" }
      }
    }
  },
  "reducers": {
    "move_player": {
      "params": {
        "dx": "i64",
        "dy": "i64"
      },
      "description": "Move one cell in a direction"
    },
    "fire_weapon": {
      "params": {},
      "wasm_module": "fire_weapon.wasm",
      "description": "Fire at the cell you are facing"
    },
    "gather": {
      "params": { "kind": "u64" },
      "description": "Gather resources at current position"
    }
  },
  "systems": [
    { "name": "cooldown_tick", "priority": 0 },
    { "name": "movement_stream", "priority": 5 }
  ]
}
```

## Type mapping

| Schema type | Rust | TypeScript | C# |
|---|---|---|---|
| `bool` | `bool` | `boolean` | `bool` |
| `u8` | `u8` | `number` | `byte` |
| `u16` | `u16` | `number` | `ushort` |
| `u32` | `u32` | `number` | `uint` |
| `u64` | `u64` | `bigint` | `ulong` |
| `i8` | `i8` | `number` | `sbyte` |
| `i16` | `i16` | `number` | `short` |
| `i32` | `i32` | `number` | `int` |
| `i64` | `i64` | `bigint` | `long` |
| `f32` | `f32` | `number` | `float` |
| `f64` | `f64` | `number` | `double` |
| `string` | `String` | `string` | `string` |
| `bytes` | `Vec<u8>` | `Uint8Array` | `byte[]` |

## Code generation targets

```
nexum generate --schema nexum.schema.json --target rust
  → src/schema_gen.rs       (TableSchema builder calls)
  → src/reducers_gen.rs     (ReducerDefinition registrations)

nexum generate --schema nexum.schema.json --target typescript
  → src/types.ts            (interfaces: Player, Unit)
  → src/client.ts           (typed NexumClient subclass)
  → src/views.ts            (reactive subscription views)

nexum generate --schema nexum.schema.json --target csharp
  → Schema/PlayerRow.cs     (POCO classes)
  → NexumClient.cs          (typed client methods)
```

## What the generated TypeScript looks like

```typescript
// AUTO-GENERATED from nexum.schema.json — do not edit

export interface PlayerRow {
  id: bigint;
  x: bigint;
  y: bigint;
  hp: bigint;
  maxHp: bigint;
  alive: bigint;
  score: bigint;
  cooldown: bigint;
  facing: bigint;
  ammo: bigint;
  connected: bigint;
}

export interface MovePlayerArgs {
  dx: bigint;
  dy: bigint;
}

export interface GatherArgs {
  kind: bigint;
}

// Typed client with auto-generated reducer proxies
export class ArenaClient extends NexumClient {
  subscribePlayers(limit = 32): SubscriptionView<PlayerRow> {
    return this.subscribe<PlayerRow>("players", { limit });
  }

  movePlayer(dx: bigint, dy: bigint): Promise<void> {
    return this.callReducer("move_player", { dx, dy });
  }

  fireWeapon(): Promise<number> {
    return this.callReducer("fire_weapon", {});
  }

  gather(kind: bigint): Promise<bigint> {
    return this.callReducer("gather", { kind });
  }

  // Input stream (lower latency, no correlation)
  sendMove(dx: number, dy: number): void {
    this.sendInput({
      commands: [{ kind: "mv", payload: (dx + 1) * 3 + (dy + 1) }]
    });
  }
}
```

## What the generated C# looks like

```csharp
// AUTO-GENERATED from nexum.schema.json — do not edit

[System.Serializable]
public class PlayerRow
{
    public ulong Id;
    public long X;
    public long Y;
    public long Hp;
    public long MaxHp;
    public long Alive;
    public long Score;
    public long Cooldown;
    public long Facing;
    public long Ammo;
    public long Connected;
}

public class ArenaClient : NexumClient
{
    public SubscriptionView<PlayerRow> SubscribePlayers(int limit = 32)
    {
        return Subscribe<PlayerRow>("players", limit);
    }

    public Task CallMovePlayer(long dx, long dy)
    {
        return CallReducerAsync("move_player", new { dx, dy });
    }

    public Task<long> FireWeapon()
    {
        return CallReducerAsync<long>("fire_weapon");
    }

    public void SendMove(int dx, int dy)
    {
        SendInput(new[] {
            new InputCommand { Kind = "mv", Payload = (dx + 1) * 3 + (dy + 1) }
        });
    }
}
```
