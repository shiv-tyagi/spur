# spur-ci-infra

Configuration for the CI servers used by Spur CI.

## What this is

This repo holds Ansible scripts (plus supporting Packer image builds and helper
scripts) that provision and configure the CI host servers.

The Packer scripts (`packer/`) build the VM images that the e2e jobs boot into.

## How CI runs

The GitHub Actions runners run on the host machines. For an e2e job, a runner
spins up a VM on its host, passes GPUs through to that VM, and the e2e tests run
inside the VM.

## Usage

Clone the repo and run the Ansible playbooks as your own user. You need SSH
access to the target servers.
