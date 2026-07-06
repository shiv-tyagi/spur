packer {
  required_plugins {
    qemu = {
      version = ">= 1.1.0"
      source  = "github.com/hashicorp/qemu"
    }
  }
}

source "qemu" "base" {
  iso_url      = "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
  iso_checksum = "file:https://cloud-images.ubuntu.com/noble/current/SHA256SUMS"
  disk_image   = true

  disk_size      = var.disk_size
  format         = "qcow2"
  accelerator    = "kvm"
  headless       = true
  net_device     = "virtio-net"
  disk_interface = "virtio"

  qemuargs = [
    ["-cdrom", "${path.root}/output/cidata.iso"],
    ["-cpu", "host"],
    ["-smp", "${var.cpus}"],
  ]

  ssh_username = "ci"
  ssh_password = var.build_password
  ssh_timeout  = "5m"

  shutdown_command  = "sudo shutdown -P now"
  output_directory  = "${var.output_dir}/base-build"
  vm_name           = "spur-ci-base.qcow2"
  disk_compression  = false
  memory            = var.memory
  cpus              = var.cpus
}

build {
  sources = ["source.qemu.base"]

  provisioner "shell" {
    script           = "${path.root}/scripts/provision-base.sh"
    environment_vars = ["ROCM_VERSION=${var.rocm_version}"]
  }

  provisioner "shell" {
    inline = [
      "sudo cloud-init clean --logs",
      "sudo truncate -s 0 /etc/machine-id",
      "sudo rm -f /etc/ssh/ssh_host_*",
      "sudo apt-get clean",
      "sudo rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*",
    ]
  }

  post-processor "shell-local" {
    inline = [
      "mv '${var.output_dir}/base-build/spur-ci-base.qcow2' '${var.output_dir}/spur-ci-base.qcow2'",
      "rm -rf '${var.output_dir}/base-build'",
    ]
  }
}
