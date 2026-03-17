//! T39: GPU/GRES scheduling tests.
//!
//! Corresponds to Slurm's test39.x series.
//! Tests GRES parsing, GPU matching in scheduler, resource satisfaction.

#[cfg(test)]
mod tests {
    use spur_core::resource::*;

    // ── T39.1: GRES parsing ─────────────────────────────────────

    #[test]
    fn t39_1_parse_gpu_full() {
        let (name, gtype, count) = parse_gres("gpu:mi300x:8").unwrap();
        assert_eq!(name, "gpu");
        assert_eq!(gtype.unwrap(), "mi300x");
        assert_eq!(count, 8);
    }

    #[test]
    fn t39_2_parse_gpu_no_type() {
        let (name, gtype, count) = parse_gres("gpu:4").unwrap();
        assert_eq!(name, "gpu");
        assert!(gtype.is_none());
        assert_eq!(count, 4);
    }

    #[test]
    fn t39_3_parse_gpu_bare() {
        let (name, gtype, count) = parse_gres("gpu").unwrap();
        assert_eq!(name, "gpu");
        assert!(gtype.is_none());
        assert_eq!(count, 1);
    }

    // ── T39.4: GPU resource matching ─────────────────────────────

    #[test]
    fn t39_4_node_satisfies_gpu_request() {
        let node = ResourceSet {
            cpus: 128,
            memory_mb: 512_000,
            gpus: (0..8).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 192_000,
                peer_gpus: vec![],
                link_type: GpuLinkType::XGMI,
            }).collect(),
            ..Default::default()
        };

        // Request 4 mi300x GPUs
        let req = ResourceSet {
            cpus: 64,
            memory_mb: 256_000,
            gpus: (0..4).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 0,
                peer_gpus: vec![],
                link_type: GpuLinkType::PCIe,
            }).collect(),
            ..Default::default()
        };

        assert!(node.can_satisfy(&req));
    }

    #[test]
    fn t39_5_node_rejects_too_many_gpus() {
        let node = ResourceSet {
            cpus: 128,
            memory_mb: 512_000,
            gpus: (0..4).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 192_000,
                peer_gpus: vec![],
                link_type: GpuLinkType::XGMI,
            }).collect(),
            ..Default::default()
        };

        // Request 8 GPUs but node only has 4
        let req = ResourceSet {
            cpus: 64,
            memory_mb: 256_000,
            gpus: (0..8).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 0,
                peer_gpus: vec![],
                link_type: GpuLinkType::PCIe,
            }).collect(),
            ..Default::default()
        };

        assert!(!node.can_satisfy(&req));
    }

    #[test]
    fn t39_6_wrong_gpu_type_rejected() {
        let node = ResourceSet {
            cpus: 128,
            memory_mb: 512_000,
            gpus: (0..8).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 192_000,
                peer_gpus: vec![],
                link_type: GpuLinkType::XGMI,
            }).collect(),
            ..Default::default()
        };

        // Request h100 GPUs on a mi300x node
        let req = ResourceSet {
            cpus: 64,
            memory_mb: 256_000,
            gpus: vec![GpuResource {
                device_id: 0,
                gpu_type: "h100".into(),
                memory_mb: 0,
                peer_gpus: vec![],
                link_type: GpuLinkType::PCIe,
            }],
            ..Default::default()
        };

        assert!(!node.can_satisfy(&req));
    }

    #[test]
    fn t39_7_any_gpu_type_matches() {
        let node = ResourceSet {
            cpus: 128,
            memory_mb: 512_000,
            gpus: (0..8).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 192_000,
                peer_gpus: vec![],
                link_type: GpuLinkType::XGMI,
            }).collect(),
            ..Default::default()
        };

        // Request "any" GPU type
        let req = ResourceSet {
            cpus: 64,
            memory_mb: 256_000,
            gpus: (0..4).map(|i| GpuResource {
                device_id: i,
                gpu_type: "any".into(),
                memory_mb: 0,
                peer_gpus: vec![],
                link_type: GpuLinkType::PCIe,
            }).collect(),
            ..Default::default()
        };

        assert!(node.can_satisfy(&req));
    }

    // ── T39.8: GPU counts ────────────────────────────────────────

    #[test]
    fn t39_8_gpu_count() {
        let r = ResourceSet {
            gpus: (0..8).map(|i| GpuResource {
                device_id: i,
                gpu_type: "mi300x".into(),
                memory_mb: 0,
                peer_gpus: vec![],
                link_type: GpuLinkType::XGMI,
            }).collect(),
            ..Default::default()
        };
        assert_eq!(r.total_gpus(), 8);
        let counts = r.gpu_counts();
        assert_eq!(counts.get("mi300x"), Some(&8));
    }
}
