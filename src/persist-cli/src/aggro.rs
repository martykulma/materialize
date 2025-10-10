use std::{sync::Arc, time::Duration};

use axum::http::Version;
use bytes::{Bytes, BytesMut};
use mz_dyncfg::ConfigUpdates;
use mz_ore::{metrics::MetricsRegistry, now::SYSTEM_TIME, url::SensitiveUrl};
use mz_persist::{
    cfg::ConsensusConfig,
    location::{Consensus, SeqNo, VersionedData},
    mem::MemConsensus,
};
use mz_persist_client::{cfg::PersistConfig, metrics::Metrics};
use tokio::{
    runtime::Handle,
    select,
    sync::watch::{self, Receiver},
};

/// An aggressive consensus client that is interested in testng the metal of even the most
/// seasoned consonsensus implementations.

#[derive(Debug, Clone, clap::Parser)]
pub struct Args {
    /// Consensus to use, defaults to an in-memory implementation that will knock your socks off.
    #[clap(long)]
    consensus_uri: Option<SensitiveUrl>,

    #[clap(long)]
    /// The total number of keys (shards) to operate on.
    num_keys: u64,

    #[clap(long)]
    /// The total number of concurrent clients executing.
    num_clients: usize,

    #[clap(long)]
    /// The total numer of concurrent GC tasks.
    num_gc_tasks: usize,

    #[clap(long)]
    /// Use PG tuned queries.
    use_pg_tuned_queries: bool,
}

#[derive(Debug)]
pub struct Aggro {
    consensus: Arc<dyn Consensus>,
    shutdown_rx: Receiver<bool>,
    bytes_len: usize,
}

struct AggroCasStats {
    attempts: u64,
    errors: u64,
    head_time: Duration,
    head_err_time: Duration,
    cas_time: Duration,
    cas_err_time: Duration,
}

impl Aggro {
    pub async fn run(&mut self) {
        while !self.shutdown_rx.has_changed().is_err() {
            // TODO: need to a have a set of keys to pick from!
            let key = "abc";
            let data = random_bytes(self.bytes_len);
            let _stats = self.cas(key, &data).await;
        }
    }

    async fn cas(&self, key: &str, data: &Bytes) -> AggroCasStats {
        let mut attempts = 1;
        let mut errors = 0;
        let mut head_time = Duration::default();
        let mut cas_time = Duration::default();
        let mut head_err_time = Duration::default();
        let mut cas_err_time = Duration::default();

        loop {
            // we need to start the timing here
            let head_start = tokio::time::Instant::now();
            match self.consensus.head(key).await {
                Ok(latest) => {
                    head_time += head_start.elapsed();
                    let current_seqno = latest.map(|data| data.seqno);
                    let seqno = current_seqno.map(|n| n.next()).unwrap_or(SeqNo::minimum());
                    let new_data = VersionedData {
                        seqno,
                        data: data.clone(),
                    };
                    let cas_start = tokio::time::Instant::now();
                    match self
                        .consensus
                        .compare_and_set(key, current_seqno, new_data)
                        .await
                    {
                        Ok(cas_result) => {
                            cas_time += cas_start.elapsed();
                            match cas_result {
                                mz_persist::location::CaSResult::Committed => break,
                                mz_persist::location::CaSResult::ExpectationMismatch => continue,
                            }
                        }
                        Err(err) => {
                            cas_err_time += cas_start.elapsed();
                            errors += 1;
                            println!("compare_and_set: error = {err:?}")
                        }
                    }
                }
                Err(err) => {
                    head_err_time += head_start.elapsed();
                    errors += 1;
                    println!("head: error = {err:?}")
                }
            }
            attempts += 1;
        }
        AggroCasStats {
            attempts,
            errors,
            head_time,
            head_err_time,
            cas_time,
            cas_err_time,
        }
    }
}

struct AggroGc {
    consensus: Arc<dyn Consensus>,
    shutdown_rx: Receiver<bool>,
    period: Duration,
    // TODO: this is a min,max, but min must be > 0
    gc_diffs_range: (usize, usize),
}

const SCAN_ALL: usize = usize::MAX;
const RECENT_LIVE_DIFFS_LIMIT: usize = 30 * 128;

impl AggroGc {
    pub async fn run(&mut self) {
        loop {
            let timer = tokio::time::sleep(self.period);
            select! {
                _ = timer => {
                    self.gc().await
                },
                res = self.shutdown_rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }
        }
    }

    async fn gc(&self) {
        let key = "abc";
        let mut attempts = 0;
        let mut errors = 0;
        let mut truncated = 0;
        let mut scan_time = Duration::default();
        let mut scan_err_time = Duration::default();
        let mut truncate_time = Duration::default();
        let mut truncate_err_time = Duration::default();
        let mut retry_delay = Duration::from_millis(2);
        loop {
            // there is a fetch_all_live_diffs (this does everything, starting at 0)
            // there is also a fetch_recent_live_diffs (this may skip ahead if there are a lot of diffs)
            // this second option tries to scan  STATE_VERSIONS_RECENT_LIVE_DIFFS_LIMIT before jumping
            // 30 * 128 (38400) by default
            // see src/persist-client/src/internal/state_versions.rs

            // for some cases we SCAN_ALL (e.g. usize max)
            // usize::MAX;

            // fetch_and_update_state
            // src/persist-client/src/internal/apply.rs
            // scan for all fetch_all_live_diffs_gt_seqno

            // TODO: this is the most naive scan and doesn't capture any of the optimizations
            // implemented in the code
            s
            match self.consensus.scan(key, SeqNo::minimum(), SCAN_ALL).await {
                Ok(scan_result) => {
                    let keep = rand::random_range(self.gc_diffs_range.0..self.gc_diffs_range.1);
                    if keep >= scan_result.len() {
                        return;
                    }
                    let truncate_point = scan_result.get(keep-1).unwrap();
                    let truncate_seqno = truncate_point.seqno;
                    let res = self.consensus.truncate(key, truncate_seqno).await;
                    match res {
                        Ok(n) => {
                            truncated = n;
                            break;
                        }
                        Err(_) => {
                            errors += 1;
                            tokio::time::sleep(retry_delay).await;
                            retry_delay *= 2;
                        }
                    }
                }
                Err(_) => {
                    errors += 1;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay *= 2;
                }
            }
            attempts += 1;
        }
    }
}

fn random_bytes(len: usize) -> Bytes {
    let mut buf = BytesMut::with_capacity(len);
    rand::fill(&mut buf[..]);
    buf.freeze()
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    // Persist internally has a bunch of sanity check assertions. If
    // maelstrom tickles one of these, we very much want to bubble this
    // up into a process exit with non-0 status. It's surprisingly
    // tricky to be confident that we're not accidentally swallowing
    // panics in async tasks (in fact there was a bug that did exactly
    // this at one point), so abort on any panics to be extra sure.
    mz_ore::panic::install_enhanced_handler();

    let config =
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone());
    {
        let mut updates = ConfigUpdates::default();
        updates.add(
            &mz_persist::postgres::USE_POSTGRES_TUNED_QUERIES,
            args.use_pg_tuned_queries,
        );
        config.apply_from(&updates);
    }
    let metrics = Arc::new(Metrics::new(&config, &MetricsRegistry::new()));

    let consensus = match &args.consensus_uri {
        None => Arc::new(MemConsensus::default()),
        Some(consensus_uri) => {
            let cfg = ConsensusConfig::try_from(
                consensus_uri,
                Box::new(config.clone()),
                metrics.postgres_consensus.clone(),
                Arc::clone(&config.configs),
            )
            .expect("consensus_uri should be valid");
            loop {
                match cfg.clone().open().await {
                    Ok(x) => break x,
                    Err(err) => {
                        tracing::info!("failed to open consensus, trying again: {}", err);
                    }
                }
            }
        }
    };

    let mut client_handles = Vec::with_capacity(args.num_clients);
    let mut gc_handles = Vec::with_capacity(args.num_gc_tasks);

    let (tx, rx) = watch::channel(false);

    for _ in 0..args.num_clients {
        let mut aggro = Aggro {
            consensus: Arc::clone(&consensus),
            shutdown_rx: rx.clone(),
            bytes_len: 123,
        };
        client_handles.push(Handle::current().spawn(async move { aggro.run().await }));
    }
    for _ in 0..args.num_gc_tasks {
        let mut aggro = Aggro {
            consensus: Arc::clone(&consensus),
            shutdown_rx: rx.clone(),
            bytes_len: 123,
        };
        gc_handles.push(Handle::current().spawn(async move { aggro.run().await }));
    }

    drop(tx);

    Ok(())
}
