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
defers `root:funding` file creation until after the fixed-UID service account
exists, and does not clone or start the application.

## 2. Install immutable application and Vault Agent

An infrastructure administrator, not the restricted `fundingops` account, must
perform this one-time bootstrap. Clone the repository into
`/opt/funding-arbitrage-v1`, checkout an immutable reviewed commit SHA, and
verify `git status --short` is empty. Install an official Vault binary with its
vendor checksum/signature and the CA certificate at
`/etc/funding-v1/vault-ca.crt`.

Install the Vault inputs with exact service-readable ownership; Terraform does
not carry either value:

```bash
sudo install -o root -g funding -m 0640 /secure/path/vault-ca.crt \
  /etc/funding-v1/vault-ca.crt
sudo install -o root -g funding -m 0640 /secure/path/approle-role-id \
  /etc/funding-v1/approle-role-id
```

Create a dedicated Vault policy from `infra/vault/funding-v1-policy.hcl`. The
AppRole must have read-only access to the exact KV paths, renewable tokens,
`token_num_uses=0`, `secret_id_num_uses=0`, no list/write capability, and a
bounded reusable SecretID lifetime covering planned reboots. Install the
non-secret RoleID separately.
Enter the SecretID interactively so it never appears in command arguments or
shell history, and encrypt it for this host:

```bash
sudo install -d -m 0700 -o root -g root /etc/credstore.encrypted
sudo systemd-ask-password -n 'Vault AppRole SecretID' | sudo systemd-creds encrypt \
  --name=approle-secret-id - /etc/credstore.encrypted/funding-v1-approle-secret-id
sudo chmod 0600 /etc/credstore.encrypted/funding-v1-approle-secret-id
```

Vault Agent receives the decrypted value through a read-only systemd credential
under `/run/credentials/`. Rotate and revoke the reusable SecretID on schedule
and after any suspected compromise. The Agent exits on missing keys.

Copy `.env.live.example` to `.env.live`, keep `LIVE_AUTOTRADE=false`, and replace
only non-secret host-specific placeholders. Vault renders runtime/exchange and
Telegram overlays under `secrets/exchange/`. Never put API keys into `.env.live`,
Terraform state, CI secrets, command history, or chat.

## 3. Preflight and safe start

```bash
sudo /usr/local/sbin/funding-v1-control start-vault
sudo -u funding test -s secrets/exchange/runtime.env
sudo /usr/local/sbin/funding-v1-control preflight
sudo /usr/local/sbin/funding-v1-control start
sudo /usr/local/sbin/funding-v1-control status
```

`fundingops` has passwordless sudo only for those fixed wrapper actions. It has
no Docker-group membership, arbitrary root shell, unrestricted `systemctl`, or
write access to the root-owned checkout.

The application container uses `restart: "no"`; only the Vault-gated systemd
unit may start it. Each Vault start clears only the five generated destinations,
waits for a fresh render, and the application unit runs preflight itself. If
Vault stops or authentication fails, systemd stops the application and leaves it
off for explicit operator inspection and restart.

The host preflight requires UTC + synchronized Chrony, at least 6 GiB RAM and
10 GiB free disk, exact Compose scope, private secret-file permissions, runtime
UID readability, and no public listeners on 5432/9108/9109. The mTLS control
plane remains loopback-only.

Start in `SHADOW`, then `PAPER`. Limited Live remains blocked until acceptance
GATE-001/002 and the protected GitHub `limited-live-approval` environment pass.
The manual CI gate emits configuration approval only and never submits orders.

## 4. Rollback

Create an encrypted backup before migrations or image changes. Roll back only to
an immutable signed image digest. Stop the app before a database restore. Never
run global `docker system prune`, broad container deletion, or commands against
projects other than `funding_arbitrage_v1`.
