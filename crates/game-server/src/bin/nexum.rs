//! `nexum` — the unified CLI for the Nexum authoritative state engine.
//!
//! ```text
//! nexum init [name]     Scaffold a new project
//! nexum start           Start an authoritative game server
//! nexum --version       Print version
//! nexum --help          Print help
//! ```

use std::path::Path;
use std::process;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--version") | Some("-V") | Some("version") => cmd_version(),
        Some("--help") | Some("-h") | Some("help") | None => cmd_help(),
        Some("init") => cmd_init(&args[1..]),
        Some("start") => cmd_start(&args[1..]),
        Some(unknown) => {
            eprintln!("error: unknown command '{unknown}'");
            eprintln!("run 'nexum --help' for usage");
            process::exit(1);
        }
    }
}

fn cmd_version() {
    println!("nexum {VERSION}");
}

fn cmd_help() {
    print!(
        "nexum {VERSION} - authoritative state engine for realtime games\n\
\n\
USAGE:\n\
\n\
    nexum <COMMAND> [OPTIONS]\n\
\n\
COMMANDS:\n\
\n\
    init [name]     Scaffold a new Nexum project in ./<name>/\n\
    start           Start an authoritative game server\n\
\n\
FLAGS:\n\
\n\
    -h, --help       Print this help\n\
    -V, --version    Print version\n\
\n\
START OPTIONS:\n\
\n\
    --config FILE    Server configuration file (key = value)\n\
    --port PORT      Listen port (default 9337)\n\
    --ticks N        Run N ticks then exit (0 = infinite)\n\
    --lobbies N      Number of concurrent lobbies (default 1)\n"
    );
}

// ----------------------------------------------------------------- init

fn cmd_init(args: &[String]) {
    let project_name = args.first().cloned().unwrap_or_else(|| "my-game".into());
    let dir = Path::new(&project_name);

    if dir.exists() {
        eprintln!("error: directory '{project_name}' already exists");
        process::exit(1);
    }

    for d in ["reducers", "schema", "client"] {
        std::fs::create_dir_all(dir.join(d)).unwrap_or_else(|e| {
            eprintln!("error: cannot create {project_name}/{d}: {e}");
            process::exit(1);
        });
    }

    write_file(
        &dir.join("nexum.conf"),
        "# Nexum server configuration\nport = 9337\nmax_connections = 10000\ntick_rate_hz = 20\nseed = 42\n",
    );
    write_file(
        &dir.join("reducers").join("mod.rs"),
        "// Your reducers here.\n// Reducers are authoritative state transitions.\n",
    );
    write_file(
        &dir.join("schema").join("mod.rs"),
        "// Your table schemas here.\n",
    );
    write_file(
        &dir.join("client").join("main.rs"),
        "// Your client code here.\n",
    );
    write_file(
        &dir.join("README.md"),
        &format!(
            "# {project_name}\n\nA Nexum-powered realtime game.\n\n## Start\n\n```\nnexum start --config {project_name}/nexum.conf\n```"
        ),
    );

    println!("initialized Nexum project in ./{project_name}/");
    println!();
    println!("next steps:");
    println!("  1. define your schema in {project_name}/schema/");
    println!("  2. implement reducers in {project_name}/reducers/");
    println!("  3. start the server:");
    println!("     nexum start --config {project_name}/nexum.conf");
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|e| {
            eprintln!("error: cannot create {}: {e}", parent.display());
            process::exit(1);
        });
    }
    std::fs::write(path, contents).unwrap_or_else(|e| {
        eprintln!("error: cannot write {}: {e}", path.display());
        process::exit(1);
    });
}

// ----------------------------------------------------------------- start

fn cmd_start(args: &[String]) {
    let mut config_path: Option<String> = None;
    let mut port: Option<u16> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                config_path = args.get(i).cloned();
            }
            "--port" => {
                i += 1;
                port = args.get(i).and_then(|v| v.parse().ok());
            }
            other => {
                eprintln!("error: unknown start option '{other}'");
                process::exit(1);
            }
        }
        i += 1;
    }

    println!("starting Nexum server...");
    if let Some(p) = port {
        println!("  port: {p}");
    }
    if let Some(cfg) = &config_path {
        println!("  config: {cfg}");
    }
    println!();
    println!("NOTE: full server delegation arrives with Phase 28.");
    println!("For now, use:");
    println!();
    println!("  cargo run --release -p game-server -- server");
    println!();
}
