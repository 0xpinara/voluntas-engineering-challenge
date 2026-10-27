use std::{path::PathBuf, sync::Arc, time::Duration};

use clap::{Parser, ValueEnum};
use engineering_challenge::collector::{Collector, Config, Msg};
use engineering_challenge::venue::{Venue, kalshi::Kalshi, polymarket::Polymarket};
use engineering_challenge::{log, output::Writer, verify};

/// Order book collector for Kalshi and Polymarket. Writes SBE (challenge schema) to --output,
/// logs and one JSON stats line per interval to stderr.
#[derive(Parser)]
#[command(version)]
enum Cli {
    /// Collect every live book.
    Run(Run),
    /// Replay output files in order, rebuild every book and check each snapshot against the replay.
    Verify {
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Print every message as text.
        #[arg(long)]
        dump: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum VenueKind {
    Kalshi,
    Polymarket,
}

#[derive(clap::Args)]
struct Run {
    #[arg(long, value_enum, default_value = "kalshi")]
    venue: VenueKind,
    /// Output path. strftime fields rotate files, e.g. 'data/kalshi-%Y%m%dT%H.sbe'.
    #[arg(short, long, default_value = "/dev/stdout")]
    output: String,
    /// Collect only these symbols (comma separated) instead of discovering every live one.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Override websocket endpoints (comma separated; replica slots alternate between them).
    #[arg(long, value_delimiter = ',')]
    ws_url: Vec<String>,
    /// Use Kalshi's demo environment.
    #[arg(long)]
    demo: bool,
    /// Redundant sockets per shard.
    #[arg(long, default_value_t = 2)]
    replicas: usize,
    /// Symbols per socket.
    #[arg(long, default_value_t = 2500)]
    shard_size: usize,
    #[arg(long, default_value = "60s", value_parser = dur)]
    snapshot_every: Duration,
    #[arg(long, default_value = "1s", value_parser = dur)]
    takeover_after: Duration,
    #[arg(long, default_value = "5s", value_parser = dur)]
    stale_after: Duration,
    #[arg(long, default_value = "30s", value_parser = dur)]
    sync_timeout: Duration,
    #[arg(long, default_value = "1h", value_parser = dur)]
    recycle_every: Duration,
    #[arg(long, default_value = "10s", value_parser = dur)]
    stats_every: Duration,
}

fn dur(s: &str) -> Result<Duration, String> {
    let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: f64 = n.parse().map_err(|_| format!("bad duration {s:?}"))?;
    let scale = match unit {
        "ms" => 1e-3,
        "" | "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        _ => return Err(format!("bad unit in {s:?}: use ms, s, m or h")),
    };
    Ok(Duration::from_secs_f64(n * scale))
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Verify { paths, dump } => verify::run(&paths, dump),
        Cli::Run(args) => {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let rt = tokio::runtime::Runtime::new()?;
            match args.venue {
                VenueKind::Kalshi => collect(
                    rt,
                    Arc::new(Kalshi::new(args.demo, args.ws_url.clone())?),
                    args,
                ),
                VenueKind::Polymarket => collect(
                    rt,
                    Arc::new(Polymarket::new(
                        args.ws_url.clone(),
                        args.symbols.is_empty(),
                    )),
                    args,
                ),
            }
        }
    }
}

fn collect<V: Venue>(rt: tokio::runtime::Runtime, venue: Arc<V>, a: Run) -> anyhow::Result<()> {
    anyhow::ensure!(
        a.replicas >= 1 && a.shard_size >= 1,
        "--replicas and --shard-size must be positive"
    );
    let cfg = Config {
        replicas: a.replicas,
        shard_size: a.shard_size,
        snapshot_every: a.snapshot_every,
        takeover_after: a.takeover_after,
        stale_after: a.stale_after,
        sync_timeout: a.sync_timeout,
        recycle_every: a.recycle_every,
        stats_every: a.stats_every,
    };
    let (tx, rx) = crossbeam::channel::unbounded();
    let collector = Collector::new(
        venue.clone(),
        rt.handle().clone(),
        tx.clone(),
        cfg,
        Writer::spawn(a.output)?,
    );
    if a.symbols.is_empty() {
        let tx = tx.clone();
        rt.spawn(async move {
            loop {
                match venue.clone().discover(tx.clone()).await {
                    Err(e) if e.is::<crossbeam::channel::SendError<Msg>>() => return, // collector exited
                    Err(e) => log!("discovery failed: {e:#}; restarting in 10s"),
                    Ok(()) => {}
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    } else {
        tx.send(Msg::Live(
            a.symbols.iter().map(|s| s.as_str().into()).collect(),
        ))?;
    }
    rt.spawn(async move {
        signal().await;
        log!("signal received: closing websockets and output (again to force quit)");
        let _ = tx.send(Msg::Shutdown);
        signal().await;
        std::process::exit(130);
    });
    let result = collector.run(rx);
    rt.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
