# V1 Linux infrastructure runbook

## Safety boundary

This runbook prepares a **new Ubuntu 24.04 VM only**. It does not authorize
access to any deleted/old VM, live exchange credentials, or real orders. The
Terraform module is provider-neutral and renders cloud-init; it never connects
to or mutates an existing server. Keep the Compose project exactly
`funding_arbitrage_v1` and the checkout exactly `/opt/funding-arbitrage-v1`.

## 1. Render and review cloud-init

Use Terraform 1.15.9 or the pinned CI action. Supply only a public SSH key, the
operator's narrow CIDR, and an HTTPS Vault origin. Keep state local and encrypted
or use a locked remote backend; public keys are allowed, AppRole SecretIDs and
private keys are not.

```bash
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
terraform -chdir=infra/terraform plan \
  -var='ssh_public_key=ssh-ed25519 AAAA...' \
  -var='operator_cidr=203.0.113.10/32' \
  -var='vault_address=https://vault.example.net:8200'
```

Review `cloud_init_sha256`. Pass `cloud_init` as provider `user_data` only while
creating the new VM. Cloud-init disables root/password SSH, allows SSH only from
the chosen CIDR, enables UFW, Docker hardening, Chrony, and dedicated paths. It
does not clone or start the application.

## 2. Install immutable application and Vault Agent

Clone the repository into `/opt/funding-arbitrage-v1`, checkout an immutable
reviewed commit SHA, and verify `git status --short` is empty. Install an official
Vault binary with its vendor checksum/signature and the CA certificate at
`/etc/funding-v1/vault-ca.crt`.

Create a dedicated Vault policy from `infra/vault/funding-v1-policy.hcl`. The
AppRole must have read-only access to the four exact KV paths, renewable tokens,
`token_num_uses=0`, no list/write capability, and a short SecretID TTL. Deliver a
response-wrapped SecretID out of band to `/etc/funding-v1/approle-secret-id`
mode `0600`; install the non-secret RoleID separately. Vault Agent deletes the
SecretID after reading it and exits on missing keys.

Copy `.env.live.example` to `.env.live`, keep `LIVE_AUTOTRADE=false`, and replace
only non-secret host-specific placeholders. Vault renders runtime/exchange and
Telegram overlays under `secrets/exchange/`. Never put API keys into `.env.live`,
Terraform state, CI secrets, command history, or chat.

## 3. Preflight and safe start

```bash
sudo systemctl start vault-agent-funding.service
sudo -u funding test -s secrets/exchange/runtime.env
sudo bash scripts/host_preflight.sh
sudo systemctl start funding-arbitrage-v1.service
sudo systemctl status funding-arbitrage-v1.service --no-pager
```

The host preflight requires UTC + synchronized Chrony, at least 3 GiB RAM and
10 GiB free disk, exact Compose scope, private secret-file permissions, and no
public listeners on 5432/9108/9109. The mTLS control plane remains loopback-only.

Start in `SHADOW`, then `PAPER`. Limited Live remains blocked until acceptance
GATE-001/002 and the protected GitHub `limited-live-approval` environment pass.
The manual CI gate emits configuration approval only and never submits orders.

## 4. Rollback

Create an encrypted backup before migrations or image changes. Roll back only to
an immutable signed image digest. Stop the app before a database restore. Never
run global `docker system prune`, broad container deletion, or commands against
projects other than `funding_arbitrage_v1`.