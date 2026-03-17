use chrono::{DateTime, Duration, Utc};

/// Calculate fair-share factor for a user/account.
///
/// fair_share_factor = target_share / max(decayed_actual_usage, epsilon)
///
/// The decay uses a half-life model where older usage matters less.
pub fn fair_share_factor(
    target_share: f64,
    usage_records: &[(DateTime<Utc>, f64)], // (timestamp, cpu_hours)
    halflife_days: u32,
    now: DateTime<Utc>,
) -> f64 {
    let halflife = Duration::days(halflife_days as i64);
    let decay_rate = 2.0_f64.ln() / halflife.num_seconds() as f64;

    let decayed_usage: f64 = usage_records
        .iter()
        .map(|(time, usage)| {
            let age = (now - *time).num_seconds().max(0) as f64;
            usage * (-decay_rate * age).exp()
        })
        .sum();

    let epsilon = 0.001; // Avoid division by zero
    target_share / decayed_usage.max(epsilon)
}

/// Calculate effective job priority.
///
/// effective_priority = base_priority * fair_share_factor * age_factor * partition_tier
pub fn effective_priority(
    base_priority: u32,
    fair_share: f64,
    age_minutes: i64,
    partition_tier: u32,
) -> u32 {
    let age_factor = 1.0 + (age_minutes as f64 / 10080.0).min(1.0); // Max age bonus at 7 days
    let raw = base_priority as f64
        * fair_share.min(10.0) // Cap fair-share boost
        * age_factor
        * partition_tier.max(1) as f64;

    (raw as u32).max(1) // Never zero
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fair_share_no_usage() {
        let factor = fair_share_factor(1.0, &[], 14, Utc::now());
        // With no usage, factor should be very high (share / epsilon)
        assert!(factor > 100.0);
    }

    #[test]
    fn test_fair_share_heavy_usage() {
        let now = Utc::now();
        let records: Vec<(DateTime<Utc>, f64)> = (0..14)
            .map(|d| (now - Duration::days(d), 100.0))
            .collect();

        let factor = fair_share_factor(1.0, &records, 14, now);
        // Heavy recent usage → low factor
        assert!(factor < 1.0);
    }

    #[test]
    fn test_effective_priority() {
        let p = effective_priority(1000, 1.0, 0, 1);
        assert_eq!(p, 1000);

        // With age bonus
        let p = effective_priority(1000, 1.0, 10080, 1);
        assert_eq!(p, 2000);

        // With partition tier
        let p = effective_priority(1000, 1.0, 0, 2);
        assert_eq!(p, 2000);
    }
}
