use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use spur_core::job::Job;
use spur_core::node::Node;
use thiserror::Error;
use tracing::debug;

const JOBS_TABLE: TableDefinition<u32, &[u8]> = TableDefinition::new("jobs");
const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Snapshot store using redb for fast embedded persistence.
pub struct SnapshotStore {
    db: Database,
}

impl SnapshotStore {
    pub fn open(path: &Path) -> Result<Self, SnapshotError> {
        let db = Database::create(path)?;
        debug!(path = %path.display(), "snapshot store opened");
        Ok(Self { db })
    }

    /// Save a full snapshot of jobs and nodes.
    pub fn save(&self, jobs: &[Job], nodes: &[Node], wal_sequence: u64) -> Result<(), SnapshotError> {
        let txn = self.db.begin_write()?;

        {
            let mut jobs_table = txn.open_table(JOBS_TABLE)?;
            // Clear existing
            // redb doesn't have a clear, so we write over
            for job in jobs {
                let bytes = serde_json::to_vec(job)
                    .map_err(|e| SnapshotError::Serialization(e.to_string()))?;
                jobs_table.insert(job.job_id, bytes.as_slice())?;
            }
        }

        {
            let mut nodes_table = txn.open_table(NODES_TABLE)?;
            for node in nodes {
                let bytes = serde_json::to_vec(node)
                    .map_err(|e| SnapshotError::Serialization(e.to_string()))?;
                nodes_table.insert(node.name.as_str(), bytes.as_slice())?;
            }
        }

        {
            let mut meta = txn.open_table(META_TABLE)?;
            let seq_bytes = wal_sequence.to_le_bytes();
            meta.insert("wal_sequence", seq_bytes.as_slice())?;
        }

        txn.commit()?;
        debug!(jobs = jobs.len(), nodes = nodes.len(), wal_sequence, "snapshot saved");
        Ok(())
    }

    /// Load jobs from the latest snapshot.
    pub fn load_jobs(&self) -> Result<Vec<Job>, SnapshotError> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(JOBS_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut jobs = Vec::new();
        for result in table.iter()? {
            let (_, value) = result?;
            let job: Job = serde_json::from_slice(value.value())
                .map_err(|e| SnapshotError::Serialization(e.to_string()))?;
            jobs.push(job);
        }
        Ok(jobs)
    }

    /// Load nodes from the latest snapshot.
    pub fn load_nodes(&self) -> Result<Vec<Node>, SnapshotError> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(NODES_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut nodes = Vec::new();
        for result in table.iter()? {
            let (_, value) = result?;
            let node: Node = serde_json::from_slice(value.value())
                .map_err(|e| SnapshotError::Serialization(e.to_string()))?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    /// Get the WAL sequence number at which this snapshot was taken.
    pub fn wal_sequence(&self) -> Result<u64, SnapshotError> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(META_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => return Err(e.into()),
        };

        match table.get("wal_sequence")? {
            Some(value) => {
                let bytes: [u8; 8] = value.value().try_into().unwrap_or([0; 8]);
                Ok(u64::from_le_bytes(bytes))
            }
            None => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spur_core::job::JobSpec;
    use spur_core::resource::ResourceSet;
    use tempfile::NamedTempFile;

    #[test]
    fn test_snapshot_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        let jobs = vec![
            Job::new(1, JobSpec { name: "job1".into(), user: "alice".into(), ..Default::default() }),
            Job::new(2, JobSpec { name: "job2".into(), user: "bob".into(), ..Default::default() }),
        ];
        let nodes = vec![
            Node::new("node001".into(), ResourceSet { cpus: 64, memory_mb: 256_000, ..Default::default() }),
        ];

        store.save(&jobs, &nodes, 42).unwrap();

        let loaded_jobs = store.load_jobs().unwrap();
        assert_eq!(loaded_jobs.len(), 2);

        let loaded_nodes = store.load_nodes().unwrap();
        assert_eq!(loaded_nodes.len(), 1);
        assert_eq!(loaded_nodes[0].name, "node001");

        assert_eq!(store.wal_sequence().unwrap(), 42);
    }
}
