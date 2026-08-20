# Provider-neutral Linux VM baseline

This module intentionally does not create or mutate a provider VM. It validates
security invariants and renders cloud-init for an Ubuntu 24.04 node. The selected
VPS provider must pass the sensitive `cloud_init` output as `user_data` while
creating a new server; do not run it against an existing host.

The template creates dedicated application/data/backup paths, an SSH allowlist,
UFW default-deny ingress, Docker hardening, Chrony time synchronization, and
fail-closed systemd units. It does not clone the repository, install private
credentials, enable live autotrade, or open application/database ports.

Validation only:

```bash
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
```

A real plan requires `ssh_public_key`, `operator_cidr`, and `vault_address`.
Never commit `*.tfvars`, Terraform state, AppRole SecretID, or rendered outputs.