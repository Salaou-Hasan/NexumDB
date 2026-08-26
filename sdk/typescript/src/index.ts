// Nexum TypeScript SDK — authoritative state engine for realtime games
//
// Dead simple:
//   const game = new NexumGame({ url: "ws://localhost:9337" });
//   await game.connect();
//   await game.auth("token");
//   const players = game.table<Player>("players", 32);
//   players.onInsert(spawn);
//   await game.call("move_player", { dx: 1, dy: 0 });
//
// Extremely flexible:
//   game.onAny((kind, data) => { ... });

export { NexumGame, TableView } from "./nexum";
export { NexumError } from "./nexum";
export type { NexumConfig, Row, Args, Delta, SubCallbacks, ConnectionState } from "./nexum";
