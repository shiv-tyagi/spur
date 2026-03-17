use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tracing::info;

/// Connect to PostgreSQL and return a connection pool.
pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;

    info!("connected to database");
    Ok(pool)
}

/// Run database migrations (create tables if they don't exist).
pub async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS jobs (
    job_id          INTEGER PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    user_name       TEXT NOT NULL,
    uid             INTEGER NOT NULL DEFAULT 0,
    account         TEXT NOT NULL DEFAULT '',
    partition_name  TEXT NOT NULL DEFAULT '',
    qos             TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL DEFAULT 'PENDING',
    exit_code       INTEGER NOT NULL DEFAULT 0,
    num_nodes       INTEGER NOT NULL DEFAULT 1,
    num_tasks       INTEGER NOT NULL DEFAULT 1,
    cpus_per_task   INTEGER NOT NULL DEFAULT 1,
    memory_mb       BIGINT NOT NULL DEFAULT 0,
    nodelist        TEXT NOT NULL DEFAULT '',
    submit_time     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    start_time      TIMESTAMPTZ,
    end_time        TIMESTAMPTZ,
    time_limit_min  INTEGER,
    work_dir        TEXT NOT NULL DEFAULT '',
    script_hash     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS accounts (
    name            TEXT PRIMARY KEY,
    description     TEXT NOT NULL DEFAULT '',
    organization    TEXT NOT NULL DEFAULT '',
    parent_account  TEXT,
    fairshare_weight INTEGER NOT NULL DEFAULT 1,
    max_running_jobs INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    name            TEXT NOT NULL,
    account         TEXT NOT NULL REFERENCES accounts(name),
    admin_level     TEXT NOT NULL DEFAULT 'none',
    default_account TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, account)
);

CREATE TABLE IF NOT EXISTS usage (
    user_name       TEXT NOT NULL,
    account         TEXT NOT NULL,
    period_start    TIMESTAMPTZ NOT NULL,
    period_end      TIMESTAMPTZ NOT NULL,
    cpu_seconds     BIGINT NOT NULL DEFAULT 0,
    gpu_seconds     BIGINT NOT NULL DEFAULT 0,
    job_count       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_name, account, period_start)
);

CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_name);
CREATE INDEX IF NOT EXISTS idx_jobs_account ON jobs(account);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_submit_time ON jobs(submit_time);
CREATE INDEX IF NOT EXISTS idx_jobs_start_time ON jobs(start_time);
CREATE INDEX IF NOT EXISTS idx_usage_period ON usage(period_start, period_end);
"#;

/// Record a job start in the database.
pub async fn record_job_start(
    pool: &PgPool,
    job_id: i32,
    user: &str,
    account: &str,
    partition: &str,
    num_nodes: i32,
    num_tasks: i32,
    cpus_per_task: i32,
    memory_mb: i64,
    start_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO jobs (job_id, user_name, account, partition_name, num_nodes, num_tasks, cpus_per_task, memory_mb, start_time, state)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'RUNNING')
        ON CONFLICT (job_id) DO UPDATE SET
            start_time = $9,
            state = 'RUNNING'
        "#,
    )
    .bind(job_id)
    .bind(user)
    .bind(account)
    .bind(partition)
    .bind(num_nodes)
    .bind(num_tasks)
    .bind(cpus_per_task)
    .bind(memory_mb)
    .bind(start_time)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record a job completion in the database.
pub async fn record_job_end(
    pool: &PgPool,
    job_id: i32,
    state: &str,
    exit_code: i32,
    end_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE jobs SET state = $2, exit_code = $3, end_time = $4
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .bind(state)
    .bind(exit_code)
    .bind(end_time)
    .execute(pool)
    .await?;

    // Update usage table
    update_usage(pool, job_id, end_time).await?;

    Ok(())
}

/// Update usage accounting for a completed job.
async fn update_usage(pool: &PgPool, job_id: i32, end_time: DateTime<Utc>) -> anyhow::Result<()> {
    // Get job details
    let row = sqlx::query(
        "SELECT user_name, account, start_time, num_tasks, cpus_per_task FROM jobs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(());
    };

    let user: String = row.get("user_name");
    let account: String = row.get("account");
    let start_time: DateTime<Utc> = row.get("start_time");
    let num_tasks: i32 = row.get("num_tasks");
    let cpus_per_task: i32 = row.get("cpus_per_task");

    let duration_secs = (end_time - start_time).num_seconds().max(0);
    let cpu_seconds = duration_secs * (num_tasks as i64) * (cpus_per_task as i64);

    // Truncate to hourly period for aggregation
    let period_start = start_time
        .date_naive()
        .and_hms_opt(start_time.hour(), 0, 0)
        .unwrap()
        .and_utc();
    let period_end = period_start + chrono::Duration::hours(1);

    sqlx::query(
        r#"
        INSERT INTO usage (user_name, account, period_start, period_end, cpu_seconds, job_count)
        VALUES ($1, $2, $3, $4, $5, 1)
        ON CONFLICT (user_name, account, period_start) DO UPDATE SET
            cpu_seconds = usage.cpu_seconds + $5,
            job_count = usage.job_count + 1
        "#,
    )
    .bind(&user)
    .bind(&account)
    .bind(period_start)
    .bind(period_end)
    .bind(cpu_seconds)
    .execute(pool)
    .await?;

    Ok(())
}

/// Job record returned from history queries.
#[derive(Debug)]
pub struct JobRecord {
    pub job_id: i32,
    pub name: String,
    pub user_name: String,
    pub account: String,
    pub partition: String,
    pub state: String,
    pub exit_code: i32,
    pub num_nodes: i32,
    pub num_tasks: i32,
    pub nodelist: String,
    pub submit_time: DateTime<Utc>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

/// Query job history.
pub async fn get_job_history(
    pool: &PgPool,
    user: Option<&str>,
    account: Option<&str>,
    start_after: Option<DateTime<Utc>>,
    start_before: Option<DateTime<Utc>>,
    states: &[String],
    limit: u32,
) -> anyhow::Result<Vec<JobRecord>> {
    // Build dynamic query
    let mut query = String::from(
        "SELECT job_id, name, user_name, account, partition_name, state, exit_code, \
         num_nodes, num_tasks, nodelist, submit_time, start_time, end_time \
         FROM jobs WHERE 1=1",
    );
    let mut params: Vec<String> = Vec::new();
    let mut idx = 1;

    if let Some(u) = user {
        if !u.is_empty() {
            query.push_str(&format!(" AND user_name = ${}", idx));
            params.push(u.to_string());
            idx += 1;
        }
    }
    if let Some(a) = account {
        if !a.is_empty() {
            query.push_str(&format!(" AND account = ${}", idx));
            params.push(a.to_string());
            idx += 1;
        }
    }
    if let Some(after) = start_after {
        query.push_str(&format!(" AND start_time >= ${}", idx));
        params.push(after.to_rfc3339());
        idx += 1;
    }
    if let Some(before) = start_before {
        query.push_str(&format!(" AND start_time <= ${}", idx));
        params.push(before.to_rfc3339());
        idx += 1;
    }
    if !states.is_empty() {
        let placeholders: Vec<String> = states
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", idx + i))
            .collect();
        query.push_str(&format!(" AND state IN ({})", placeholders.join(",")));
        for s in states {
            params.push(s.clone());
        }
    }

    query.push_str(" ORDER BY submit_time DESC");
    let effective_limit = if limit > 0 { limit } else { 1000 };
    query.push_str(&format!(" LIMIT {}", effective_limit));

    // Execute with dynamic params — using raw query for simplicity
    // In production, use sqlx query builder or sea-query
    let mut q = sqlx::query(&query);
    for p in &params {
        q = q.bind(p);
    }

    let rows = q.fetch_all(pool).await?;

    let records = rows
        .iter()
        .map(|row| JobRecord {
            job_id: row.get("job_id"),
            name: row.get("name"),
            user_name: row.get("user_name"),
            account: row.get("account"),
            partition: row.get("partition_name"),
            state: row.get("state"),
            exit_code: row.get("exit_code"),
            num_nodes: row.get("num_nodes"),
            num_tasks: row.get("num_tasks"),
            nodelist: row.get("nodelist"),
            submit_time: row.get("submit_time"),
            start_time: row.get("start_time"),
            end_time: row.get("end_time"),
        })
        .collect();

    Ok(records)
}

/// Get usage data for fair-share calculation.
pub async fn get_usage(
    pool: &PgPool,
    user: Option<&str>,
    account: Option<&str>,
    since: DateTime<Utc>,
) -> anyhow::Result<Vec<UsageRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT user_name, account, SUM(cpu_seconds) as total_cpu_seconds,
               SUM(gpu_seconds) as total_gpu_seconds, SUM(job_count) as total_jobs,
               period_start
        FROM usage
        WHERE period_start >= $1
          AND ($2::text IS NULL OR user_name = $2)
          AND ($3::text IS NULL OR account = $3)
        GROUP BY user_name, account, period_start
        ORDER BY period_start
        "#,
    )
    .bind(since)
    .bind(user)
    .bind(account)
    .fetch_all(pool)
    .await?;

    let records = rows
        .iter()
        .map(|row| UsageRecord {
            user_name: row.get("user_name"),
            account: row.get("account"),
            cpu_seconds: row.get::<i64, _>("total_cpu_seconds"),
            gpu_seconds: row.get::<i64, _>("total_gpu_seconds"),
            job_count: row.get::<i64, _>("total_jobs") as u64,
            period_start: row.get("period_start"),
        })
        .collect();

    Ok(records)
}

#[derive(Debug)]
pub struct UsageRecord {
    pub user_name: String,
    pub account: String,
    pub cpu_seconds: i64,
    pub gpu_seconds: i64,
    pub job_count: u64,
    pub period_start: DateTime<Utc>,
}

use chrono::Timelike;
