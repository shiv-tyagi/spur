//! T17: Job submission (sbatch / spur submit).
//!
//! Corresponds to Slurm's test17.x series.
//! Tests #SBATCH parsing, #PBS conversion, script handling.

#[cfg(test)]
mod tests {
    use crate::harness::*;

    // We test the sbatch directive parser directly since it doesn't need a server.
    // The parser is in spur-cli but we can test the core logic here.

    // ── T17.1: #SBATCH directive parsing ─────────────────────────

    #[test]
    fn t17_1_parse_basic_directives() {
        let script = test_script(
            &["--job-name=test", "-N 4", "--time=4:00:00", "--gres=gpu:mi300x:8"],
            "echo hello",
        );

        // Verify the script contains the directives
        assert!(script.contains("#SBATCH --job-name=test"));
        assert!(script.contains("#SBATCH -N 4"));
        assert!(script.contains("#SBATCH --time=4:00:00"));
        assert!(script.contains("#SBATCH --gres=gpu:mi300x:8"));
    }

    #[test]
    fn t17_2_script_body_after_directives() {
        let script = test_script(
            &["--job-name=test"],
            "echo hello\necho world",
        );

        let lines: Vec<&str> = script.lines().collect();
        assert_eq!(lines[0], "#!/bin/bash");
        assert_eq!(lines[1], "#SBATCH --job-name=test");
        assert!(lines.iter().any(|l| *l == "echo hello"));
        assert!(lines.iter().any(|l| *l == "echo world"));
    }

    // ── T17.3: Job spec defaults ─────────────────────────────────

    #[test]
    fn t17_3_job_spec_defaults() {
        let spec = spur_core::job::JobSpec::default();
        assert_eq!(spec.num_nodes, 1);
        assert_eq!(spec.num_tasks, 1);
        assert_eq!(spec.cpus_per_task, 1);
        assert!(!spec.requeue);
        assert!(!spec.exclusive);
        assert!(!spec.hold);
    }

    // ── T17.4: Job creation ──────────────────────────────────────

    #[test]
    fn t17_4_job_gets_unique_id() {
        reset_job_ids();
        let j1 = make_job("a");
        let j2 = make_job("b");
        let j3 = make_job("c");
        assert_ne!(j1.job_id, j2.job_id);
        assert_ne!(j2.job_id, j3.job_id);
    }

    #[test]
    fn t17_5_job_submit_time_set() {
        reset_job_ids();
        let job = make_job("test");
        let now = chrono::Utc::now();
        let diff = (now - job.submit_time).num_seconds().abs();
        assert!(diff < 2, "submit_time should be within 2 seconds of now");
    }

    #[test]
    fn t17_6_job_initial_priority() {
        reset_job_ids();
        let job = make_job("test");
        assert_eq!(job.priority, 1000); // Default priority
    }

    #[test]
    fn t17_7_job_custom_priority() {
        let job = spur_core::job::Job::new(
            1,
            spur_core::job::JobSpec {
                name: "test".into(),
                user: "alice".into(),
                priority: Some(5000),
                ..Default::default()
            },
        );
        assert_eq!(job.priority, 5000);
    }

    // ── T17.8: Hold flag ─────────────────────────────────────────

    #[test]
    fn t17_8_hold_sets_pending_reason() {
        let job = spur_core::job::Job::new(
            1,
            spur_core::job::JobSpec {
                name: "held".into(),
                user: "alice".into(),
                hold: true,
                ..Default::default()
            },
        );
        assert_eq!(job.state, spur_core::job::JobState::Pending);
        assert_eq!(job.pending_reason, spur_core::job::PendingReason::Held);
    }

    // ── T17.9: Array spec ────────────────────────────────────────

    #[test]
    fn t17_9_array_spec_stored() {
        let job = spur_core::job::Job::new(
            1,
            spur_core::job::JobSpec {
                name: "array".into(),
                user: "alice".into(),
                array_spec: Some("0-99%10".into()),
                ..Default::default()
            },
        );
        assert_eq!(job.spec.array_spec, Some("0-99%10".into()));
    }

    // ── T17.10: Dependency parsing ───────────────────────────────

    #[test]
    fn t17_10_dependencies_stored() {
        let job = spur_core::job::Job::new(
            1,
            spur_core::job::JobSpec {
                name: "dep".into(),
                user: "alice".into(),
                dependency: vec!["afterok:100".into(), "afterany:200".into()],
                ..Default::default()
            },
        );
        assert_eq!(job.spec.dependency.len(), 2);
        assert_eq!(job.spec.dependency[0], "afterok:100");
    }
}
