//! T05: Job queue viewing (squeue / spur queue).
//!
//! Corresponds to Slurm's test5.x series.
//! Tests job filtering, state matching, output formatting.

#[cfg(test)]
mod tests {
    use spur_core::job::*;
    use crate::harness::*;

    // ── T05.1: Job state classification ──────────────────────────

    #[test]
    fn t05_1_terminal_states() {
        assert!(JobState::Completed.is_terminal());
        assert!(JobState::Failed.is_terminal());
        assert!(JobState::Cancelled.is_terminal());
        assert!(JobState::Timeout.is_terminal());
        assert!(JobState::NodeFail.is_terminal());
    }

    #[test]
    fn t05_2_active_states() {
        assert!(JobState::Running.is_active());
        assert!(JobState::Completing.is_active());
        assert!(JobState::Suspended.is_active());
    }

    #[test]
    fn t05_3_pending_not_terminal_or_active() {
        assert!(!JobState::Pending.is_terminal());
        assert!(!JobState::Pending.is_active());
    }

    // ── T05.4: Job filtering ─────────────────────────────────────

    #[test]
    fn t05_4_filter_by_state() {
        reset_job_ids();
        let mut jobs = vec![
            make_job("pending1"),
            make_job("pending2"),
            make_job("running1"),
        ];
        jobs[2].transition(JobState::Running).unwrap();

        let pending: Vec<_> = jobs.iter()
            .filter(|j| j.state == JobState::Pending)
            .collect();
        assert_eq!(pending.len(), 2);

        let running: Vec<_> = jobs.iter()
            .filter(|j| j.state == JobState::Running)
            .collect();
        assert_eq!(running.len(), 1);
    }

    #[test]
    fn t05_5_filter_by_user() {
        reset_job_ids();
        let mut j1 = make_job("a");
        j1.spec.user = "alice".into();
        let mut j2 = make_job("b");
        j2.spec.user = "bob".into();
        let mut j3 = make_job("c");
        j3.spec.user = "alice".into();

        let jobs = vec![j1, j2, j3];
        let alice_jobs: Vec<_> = jobs.iter()
            .filter(|j| j.spec.user == "alice")
            .collect();
        assert_eq!(alice_jobs.len(), 2);
    }

    #[test]
    fn t05_6_filter_by_partition() {
        reset_job_ids();
        let mut j1 = make_job("a");
        j1.spec.partition = Some("gpu".into());
        let mut j2 = make_job("b");
        j2.spec.partition = Some("cpu".into());
        let mut j3 = make_job("c");
        j3.spec.partition = Some("gpu".into());

        let jobs = vec![j1, j2, j3];
        let gpu_jobs: Vec<_> = jobs.iter()
            .filter(|j| j.spec.partition.as_deref() == Some("gpu"))
            .collect();
        assert_eq!(gpu_jobs.len(), 2);
    }

    // ── T05.7: Pending reason ────────────────────────────────────

    #[test]
    fn t05_7_pending_reason_displayed() {
        reset_job_ids();
        let job = make_job("test");
        assert_eq!(job.pending_reason.display(), "Priority");
    }

    // ── T05.8: Sort by priority ──────────────────────────────────

    #[test]
    fn t05_8_sort_by_priority() {
        reset_job_ids();
        let mut j1 = make_job("low");
        j1.priority = 100;
        let mut j2 = make_job("high");
        j2.priority = 5000;
        let mut j3 = make_job("mid");
        j3.priority = 1000;

        let mut jobs = vec![j1, j2, j3];
        jobs.sort_by(|a, b| b.priority.cmp(&a.priority));

        assert_eq!(jobs[0].spec.name, "high");
        assert_eq!(jobs[1].spec.name, "mid");
        assert_eq!(jobs[2].spec.name, "low");
    }
}
