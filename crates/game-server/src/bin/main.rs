//! The game-server binary: `server` or `client` subcommands.
//!
//! ```text
//! cargo run -p game-server -- server [--port 9337] [--partitions 1] [--hz 20] [--seed 42] [--persist DIR]
//! cargo run -p game-server -- client [--name alice] [--port 9337] [--auto SECONDS]
//! ```

use game_server::{run_client, run_server, ClientArgs, ServerArgs};

const HELP: &str = "usage: game-server <server|client> [options]

server:
  --config FILE    production config file (key = value, # comments)
  --port N         TCP listen port (overrides config)
  --partitions N   arena partitions; 1 = one shared world
  --hz N           logical ticks per second
  --seed N         deterministic world seed
  --players N      maximum players per game
  --workers N      logical workers
  --persist DIR    enable WAL durability into DIR (recovery on restart)
  --stop-after N   cleanly shut down after N server-loop iterations
  --stop-file FILE shut down cleanly when FILE appears
  --quiet          log level -> error; suppress per-event log lines

client:
  --name NAME     player token: alice | bob | carol | dave (default alice)
  --addr HOST     server host (default 127.0.0.1)
  --port N        server port (default 9337)
  --auto SECONDS  run a scripted player for SECONDS (default: interactive)
  --quiet         suppress setup chatter

controls (interactive): w/a/s/d move · f fire · r reload · x respawn · q quit";

fn print_help() -> ! {
    println!("{HELP}");
    std::process::exit(0);
}

fn usage<T>() -> T {
    eprintln!("{HELP}");
    std::process::exit(2);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(usage::<String>);
    match command.as_str() {
        "server" => run_server(parse_server(&mut args)),
        "client" => run_client(parse_client(&mut args)).map(|_| ()),
        "--help" | "-h" | "help" => print_help(),
        other => {
            eprintln!("unknown command: {other}");
            usage::<Result<(), Box<dyn std::error::Error + Send + Sync>>>()
        }
    }
    .unwrap_or_else(|error| {
        eprintln!("error: {error}");
        std::process::exit(1);
    });
}

fn parse_server(args: &mut impl Iterator<Item = String>) -> ServerArgs {
    let mut server = ServerArgs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => server.config = Some(args.next().unwrap_or_else(usage::<String>).into()),
            "--port" => server.port = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<u16>())),
            "--partitions" => server.partitions = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<usize>())),
            "--hz" => server.hz = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<u32>())),
            "--seed" => server.seed = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<u64>())),
            "--players" => server.max_players = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<usize>())),
            "--workers" => server.workers = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<usize>())),
            "--persist" => server.persist = Some(args.next().unwrap_or_else(usage::<String>).into()),
            "--stop-after" => server.stop_after = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<u64>())),
            "--stop-file" => server.stop_file = Some(args.next().unwrap_or_else(usage::<String>).into()),
            "--quiet" => server.quiet = true,
            "--help" | "-h" => print_help(),
            other => {
                eprintln!("unknown server option: {other}");
                usage::<()>();
            }
        }
    }
    server
}

fn parse_client(args: &mut impl Iterator<Item = String>) -> ClientArgs {
    let mut client = ClientArgs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => client.name = args.next().unwrap_or_else(usage::<String>),
            "--addr" => client.addr = args.next().unwrap_or_else(usage::<String>),
            "--port" => client.port = args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<u16>()),
            "--auto" => client.auto_seconds = Some(args.next().unwrap_or_else(usage::<String>).parse().unwrap_or_else(|_| usage::<u64>())),
            "--quiet" => client.quiet = true,
            "--help" | "-h" => print_help(),
            other => {
                eprintln!("unknown client option: {other}");
                usage::<()>();
            }
        }
    }
    client
}
