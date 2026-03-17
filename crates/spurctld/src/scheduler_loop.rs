use std::sync::Arc;

use tracing::{debug, info};

use spur_sched::backfill::BackfillScheduler;
use spur_sched::traits::{ClusterState, Scheduler};

use crate::cluster::ClusterManager;

/// Main scheduler loop. Runs periodically, matching pending jobs to available nodes.
pub async fn run(cluster: Arc<ClusterManager>) {
    let interval_secs = cluster.config.scheduler.interval_secs.max(1) as u64;
    let max_jobs = cluster.config.scheduler.max_jobs_per_cycle as usize;

    let mut scheduler = BackfillScheduler::new(max_jobs);

    info!(
        interval_secs,
        max_jobs,
        plugin = scheduler.name(),
        "scheduler loop started"
    );

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let pending = cluster.pending_jobs();
        if pending.is_empty() {
            continue;
        }

        let nodes = cluster.get_nodes();
        let partitions = cluster.get_partitions();

        if nodes.is_empty() {
            debug!("no nodes registered, skipping scheduling cycle");
            continue;
        }

        let cluster_state = ClusterState {
            nodes: &nodes,
            partitions: &partitions,
        };

        let assignments = scheduler.schedule(&pending, &cluster_state);

        for assignment in assignments {
            // Compute allocated resources per node
            let job = match cluster.get_job(assignment.job_id) {
                Some(j) => j,
                None => continue,
            };

            let per_node_cpus = if let Some(tpn) = job.spec.tasks_per_node {
                tpn * job.spec.cpus_per_task
            } else {
                (job.spec.num_tasks / job.spec.num_nodes.max(1)) * job.spec.cpus_per_task
            };

            let resources = spur_core::resource::ResourceSet {
                cpus: per_node_cpus * assignment.nodes.len() as u32,
                memory_mb: job.spec.memory_per_node_mb.unwrap_or(0)
                    * assignment.nodes.len() as u64,
                ..Default::default()
            };

            if let Err(e) = cluster.start_job(
                assignment.job_id,
                assignment.nodes.clone(),
                resources,
            ) {
                debug!(
                    job_id = assignment.job_id,
                    error = %e,
                    "failed to start job"
                );
            }
        }
    }
}
