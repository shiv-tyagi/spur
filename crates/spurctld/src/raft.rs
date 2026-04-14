//! Raft-based consensus for spurctld HA.
//!
//! When `controller.peers` is configured, spurctld forms a Raft cluster
//! for automatic leader election and state replication. When peers is empty,
//! single-node mode is used (no Raft).
//!
//! The Raft log entries are `WalOperation` values — the same operations
//! used by the existing WAL. This means the state machine's `apply()` calls
//! the same `replay_entry()` used for WAL recovery.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{LogState, RaftLogReader, Snapshot, SnapshotMeta};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, Raft, RaftSnapshotBuilder, StorageError,
    StoredMembership, Vote,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use spur_core::wal::WalOperation;

// ─── Type Config ─────────────────────────────────────────────────

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub SpurTypeConfig:
        D = WalOperation,
        R = ClientResponse,
        Node = BasicNode,
);

pub type SpurRaft = Raft<SpurTypeConfig>;

/// Response returned after a Raft write is committed.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientResponse;

// ─── Combined Storage (v1 RaftStorage trait) ─────────────────────

/// Combined Raft storage: log + state machine + snapshots.
///
/// Uses in-memory storage. Production would back this with the existing
/// FileWalStore + SnapshotStore.
#[derive(Debug, Default)]
pub struct SpurStore {
    inner: RwLock<StoreInner>,
}

#[derive(Debug, Default)]
struct StoreInner {
    // Vote
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,

    // Log
    last_purged: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<SpurTypeConfig>>,

    // State machine
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    applied_count: u64,
}

// ── RaftLogReader ────────────────────────────────────────────────

impl RaftLogReader<SpurTypeConfig> for Arc<SpurStore> {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<SpurTypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

// ── RaftSnapshotBuilder ──────────────────────────────────────────

impl RaftSnapshotBuilder<SpurTypeConfig> for Arc<SpurStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<SpurTypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read();
        let data = serde_json::json!({
            "applied_count": inner.applied_count,
        });
        let bytes = serde_json::to_vec(&data).unwrap_or_default();
        let snap_id = inner.applied_count.to_string();

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.last_membership.clone(),
                snapshot_id: snap_id,
            },
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

// ── RaftStorage (v1 unified trait) ───────────────────────────────

impl openraft::RaftStorage<SpurTypeConfig> for Arc<SpurStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<SpurTypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read();
        let last = inner.log.iter().next_back().map(|(_, e)| e.log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.write().vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().vote.clone())
    }

    async fn append_to_log<I: IntoIterator<Item = Entry<SpurTypeConfig>> + Send>(
        &mut self,
        entries: I,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write();
        let keys: Vec<_> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write();
        let keys: Vec<_> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.read();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<SpurTypeConfig>],
    ) -> Result<Vec<ClientResponse>, StorageError<NodeId>> {
        let mut inner = self.inner.write();
        let mut results = Vec::new();

        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Normal(_op) => {
                    debug!(index = entry.log_id.index, "raft: applying WalOperation");
                    inner.applied_count += 1;
                    // TODO: Full ClusterManager integration — route through replay_entry()
                }
                EntryPayload::Membership(mem) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                }
                EntryPayload::Blank => {}
            }
            results.push(ClientResponse);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        info!(last_applied = ?meta.last_log_id, "installed snapshot");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SpurTypeConfig>>, StorageError<NodeId>> {
        let mut builder = self.clone();
        let snap = builder.build_snapshot().await?;
        Ok(Some(snap))
    }
}

// ─── Network ─────────────────────────────────────────────────────

/// Raft network factory using gRPC.
pub struct SpurNetwork {
    peers: BTreeMap<NodeId, String>,
}

impl openraft::RaftNetworkFactory<SpurTypeConfig> for Arc<SpurNetwork> {
    type Network = SpurNetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        let addr = self
            .peers
            .get(&target)
            .cloned()
            .unwrap_or_else(|| format!("unknown-{}", target));
        SpurNetworkConnection { target, addr }
    }
}

/// Connection to a single Raft peer.
pub struct SpurNetworkConnection {
    target: NodeId,
    addr: String,
}

impl openraft::RaftNetwork<SpurTypeConfig> for SpurNetworkConnection {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<SpurTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        // TODO: Implement gRPC transport to peers
        debug!(target: "raft", node = self.target, addr = %self.addr, "append_entries");
        Err(openraft::error::RPCError::Unreachable(
            openraft::error::Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "raft gRPC transport not yet implemented",
            )),
        ))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<SpurTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        debug!(target: "raft", node = self.target, "install_snapshot");
        Err(openraft::error::RPCError::Unreachable(
            openraft::error::Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "raft gRPC transport not yet implemented",
            )),
        ))
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        debug!(target: "raft", node = self.target, "vote");
        Err(openraft::error::RPCError::Unreachable(
            openraft::error::Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "raft gRPC transport not yet implemented",
            )),
        ))
    }
}

// ─── Raft Startup ────────────────────────────────────────────────

/// Handle to the running Raft node.
pub struct RaftHandle {
    pub raft: SpurRaft,
    pub node_id: NodeId,
}

/// Start a Raft node with the given ID and peer list.
pub async fn start_raft(node_id: NodeId, peers: &[String]) -> anyhow::Result<RaftHandle> {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    let config = Arc::new(config.validate().map_err(|e| anyhow::anyhow!("{e}"))?);

    let store = Arc::new(SpurStore::default());

    // Build peer map
    let mut peer_map = BTreeMap::new();
    for (i, addr) in peers.iter().enumerate() {
        peer_map.insert(i as u64 + 1, addr.clone());
    }
    let network = Arc::new(SpurNetwork {
        peers: peer_map.clone(),
    });

    // Use the Adaptor to convert v1 RaftStorage to v2 split traits
    let (log_store, state_machine) = openraft::storage::Adaptor::new(store);

    let raft = Raft::new(node_id, config, network, log_store, state_machine).await?;

    // Bootstrap cluster membership on node 1
    if node_id == 1 {
        let mut members = BTreeMap::new();
        for (id, addr) in &peer_map {
            members.insert(*id, BasicNode::new(addr.clone()));
        }
        if let Err(e) = raft.initialize(members).await {
            debug!("raft initialize: {e} (may be already initialized)");
        }
    }

    info!(node_id, "raft node created");
    Ok(RaftHandle { raft, node_id })
}
