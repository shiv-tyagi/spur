variable "rocm_version" {
  type        = string
  default     = "6.4"
  description = "ROCm APT repo version for amdgpu-dkms and userspace packages"
}

variable "k8s_version" {
  type        = string
  default     = "1.36"
  description = "Kubernetes minor version for kubeadm/kubelet/kubectl"
}

variable "output_dir" {
  type        = string
  default     = "/var/lib/spur-ci/images"
  description = "Directory where final qcow2 images are placed"
}

variable "disk_size" {
  type        = string
  default     = "60G"
  description = "Disk size for the built image"
}

variable "build_password" {
  type        = string
  default     = "packer-build-temp"
  sensitive   = true
  description = "Temporary SSH password for the build VM (never baked into final image)"
}

variable "memory" {
  type        = number
  default     = 8192
  description = "RAM in MB for the build VM"
}

variable "cpus" {
  type        = number
  default     = 8
  description = "vCPUs for the build VM"
}
