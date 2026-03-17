use anyhow::Result;
use clap::Parser;

/// Run a parallel job (interactive or allocation-based).
#[derive(Parser, Debug)]
#[command(name = "srun", about = "Run a parallel job")]
pub struct SrunArgs {
    /// Job name
    #[arg(short = 'J', long)]
    pub job_name: Option<String>,

    /// Partition
    #[arg(short = 'p', long)]
    pub partition: Option<String>,

    /// Number of nodes
    #[arg(short = 'N', long, default_value = "1")]
    pub nodes: u32,

    /// Number of tasks
    #[arg(short = 'n', long, default_value = "1")]
    pub ntasks: u32,

    /// CPUs per task
    #[arg(short = 'c', long, default_value = "1")]
    pub cpus_per_task: u32,

    /// Time limit
    #[arg(short = 't', long)]
    pub time: Option<String>,

    /// GRES
    #[arg(long)]
    pub gres: Vec<String>,

    /// GPUs
    #[arg(short = 'G', long)]
    pub gpus: Option<String>,

    /// Controller address
    #[arg(long, env = "SPUR_CONTROLLER_ADDR", default_value = "http://localhost:6817")]
    pub controller: String,

    /// Command and arguments
    #[arg(trailing_var_arg = true)]
    pub command: Vec<String>,
}

pub async fn main() -> Result<()> {
    let args = SrunArgs::try_parse_from(std::env::args())?;

    if args.command.is_empty() {
        eprintln!("srun: no command specified");
        std::process::exit(1);
    }

    // MVP: srun submits as a batch job and waits for completion,
    // streaming stdout/stderr. Full interactive I/O forwarding is post-MVP.
    eprintln!(
        "srun: interactive mode not yet implemented. Use sbatch for batch jobs."
    );
    eprintln!(
        "srun: would run: {:?} on {} node(s) with {} task(s)",
        args.command, args.nodes, args.ntasks
    );

    // TODO: Phase 5 - implement interactive job submission with gRPC streaming
    // 1. Submit job to controller
    // 2. Wait for allocation
    // 3. Stream stdio via gRPC bidirectional stream

    std::process::exit(1);
}
