# Vault Agent integration

The checked-in Agent uses AppRole auto-auth and renders mode `0600` files owned
by the fixed runtime UID `10001`. The application and Alertmanager containers use
that same UID, so the files remain readable without group/world permissions.
Missing secrets or keys terminate the Agent, and the application systemd unit
refuses to start until every required rendered file exists.

Create a dedicated AppRole with `infra/vault/funding-v1-policy.hcl`, renewable
tokens (`token_num_uses=0`), a bounded reusable SecretID lifetime covering the
planned reboot window, `secret_id_num_uses=0`, and no write/list permissions.
Provision the SecretID out
of band and immediately encrypt it for this host with `systemd-creds`; the only
persistent copy is
`/etc/credstore.encrypted/funding-v1-approle-secret-id` mode `0600`. The service
receives a read-only plaintext credential below `/run/credentials/`, so Vault
Agent must keep `remove_secret_id_file_after_reading = false`. Rotate and revoke
the SecretID on schedule and after any suspected host compromise. Never place it
in Terraform state, GitHub Actions, command arguments, shell history, or this
repository. Install the Vault CA at `/etc/funding-v1/vault-ca.crt` and pin an
HTTPS `VAULT_ADDR`.

Every Vault Agent start removes only the five generated destinations and waits
up to 60 seconds for a fresh, UID-10001, mode-0600 render. The trading container
has no Docker restart policy and is bound to the Vault unit, so reboot, revoked
credentials, or failed renewal cannot bypass the secret gate.

The `credential-policy` secret contains a validated JSON document in key
`document`. Exchange API keys must be dedicated-subaccount, read+trade only,
withdraw/transfer disabled, and restricted to the VM's exact egress IP.

Vault Agent only renders configuration. It never enables `LIVE_AUTOTRADE`,
changes `LIVE_ARMED`, or submits an order.
