use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tonic::{Request, Response, Status};

use spur_proto::proto::slurm_accounting_server::{SlurmAccounting, SlurmAccountingServer};
use spur_proto::proto::*;

use crate::db;

pub struct AccountingService {
    pool: PgPool,
}

#[tonic::async_trait]
impl SlurmAccounting for AccountingService {
    async fn record_job_start(
        &self,
        request: Request<RecordJobStartRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let start_time = req
            .start_time
            .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or_default())
            .unwrap_or_else(Utc::now);

        let (memory_mb, cpus) = req
            .resources
            .as_ref()
            .map(|r| (r.memory_mb as i64, r.cpus as i32))
            .unwrap_or((0, 1));

        db::record_job_start(
            &self.pool,
            req.job_id as i32,
            &req.user,
            &req.account,
            &req.partition,
            1, // num_nodes — simplified
            cpus,
            1,
            memory_mb,
            start_time,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(()))
    }

    async fn record_job_end(
        &self,
        request: Request<RecordJobEndRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let end_time = req
            .end_time
            .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or_default())
            .unwrap_or_else(Utc::now);

        let state_str = match req.final_state {
            3 => "COMPLETED",
            4 => "FAILED",
            5 => "CANCELLED",
            6 => "TIMEOUT",
            7 => "NODE_FAIL",
            _ => "UNKNOWN",
        };

        db::record_job_end(
            &self.pool,
            req.job_id as i32,
            state_str,
            req.exit_code,
            end_time,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(()))
    }

    async fn get_job_history(
        &self,
        request: Request<GetJobHistoryRequest>,
    ) -> Result<Response<GetJobHistoryResponse>, Status> {
        let req = request.into_inner();

        let start_after = req
            .start_after
            .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or_default());
        let start_before = req
            .start_before
            .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or_default());

        let states: Vec<String> = req
            .states
            .iter()
            .filter_map(|s| match *s {
                3 => Some("COMPLETED".into()),
                4 => Some("FAILED".into()),
                5 => Some("CANCELLED".into()),
                6 => Some("TIMEOUT".into()),
                _ => None,
            })
            .collect();

        let user = if req.user.is_empty() {
            None
        } else {
            Some(req.user.as_str())
        };
        let account = if req.account.is_empty() {
            None
        } else {
            Some(req.account.as_str())
        };

        let records = db::get_job_history(
            &self.pool,
            user,
            account,
            start_after,
            start_before,
            &states,
            req.limit,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let jobs = records
            .iter()
            .map(|r| JobInfo {
                job_id: r.job_id as u32,
                name: r.name.clone(),
                user: r.user_name.clone(),
                uid: 0,
                partition: r.partition.clone(),
                account: r.account.clone(),
                state: match r.state.as_str() {
                    "COMPLETED" => JobState::JobCompleted as i32,
                    "FAILED" => JobState::JobFailed as i32,
                    "CANCELLED" => JobState::JobCancelled as i32,
                    "TIMEOUT" => JobState::JobTimeout as i32,
                    "RUNNING" => JobState::JobRunning as i32,
                    "PENDING" => JobState::JobPending as i32,
                    _ => JobState::JobCompleted as i32,
                },
                state_reason: String::new(),
                submit_time: Some(datetime_to_proto(r.submit_time)),
                start_time: r.start_time.map(datetime_to_proto),
                end_time: r.end_time.map(datetime_to_proto),
                time_limit: None,
                run_time: match (r.start_time, r.end_time) {
                    (Some(s), Some(e)) => Some(prost_types::Duration {
                        seconds: (e - s).num_seconds(),
                        nanos: 0,
                    }),
                    _ => None,
                },
                num_nodes: r.num_nodes as u32,
                num_tasks: r.num_tasks as u32,
                cpus_per_task: 1,
                nodelist: r.nodelist.clone(),
                work_dir: String::new(),
                command: String::new(),
                exit_code: r.exit_code,
                stdout_path: String::new(),
                stderr_path: String::new(),
                resources: None,
                priority: 0,
                qos: String::new(),
                array_job_id: 0,
                array_task_id: 0,
            })
            .collect();

        Ok(Response::new(GetJobHistoryResponse { jobs }))
    }

    async fn get_usage(
        &self,
        request: Request<GetUsageRequest>,
    ) -> Result<Response<GetUsageResponse>, Status> {
        let req = request.into_inner();

        let since = req
            .since
            .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or_default())
            .unwrap_or_else(|| Utc::now() - chrono::Duration::days(30));

        let user = if req.user.is_empty() {
            None
        } else {
            Some(req.user.as_str())
        };
        let account = if req.account.is_empty() {
            None
        } else {
            Some(req.account.as_str())
        };

        let records = db::get_usage(&self.pool, user, account, since)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut cpu_hours = std::collections::HashMap::new();
        let mut gpu_hours = std::collections::HashMap::new();
        let mut job_count = std::collections::HashMap::new();

        for r in &records {
            let key = format!("{}:{}", r.user_name, r.account);
            *cpu_hours.entry(key.clone()).or_insert(0.0) += r.cpu_seconds as f64 / 3600.0;
            *gpu_hours.entry(key.clone()).or_insert(0.0) += r.gpu_seconds as f64 / 3600.0;
            *job_count.entry(key).or_insert(0u64) += r.job_count;
        }

        Ok(Response::new(GetUsageResponse {
            cpu_hours,
            gpu_hours,
            job_count,
        }))
    }
}

pub async fn serve(addr: SocketAddr, pool: PgPool) -> anyhow::Result<()> {
    let service = AccountingService { pool };

    tonic::transport::Server::builder()
        .add_service(SlurmAccountingServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

fn datetime_to_proto(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}
