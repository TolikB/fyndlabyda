# Vault Agent integration

The checked-in Agent uses AppRole auto-auth and renders only root/`funding`
readable files. The SecretID is deleted after first read. Missing secrets or keys
terminate the Agent, and the application systemd unit refuses to start until all
rendered files exist.

Create a dedicated AppRole with `infra/vault/funding-v1-policy.hcl`, renewable
tokens (`token_num_uses=0`), a short SecretID TTL, and no write/list permissions.
Deliver the wrapped SecretID out of band to `/etc/funding-v1/approle-secret-id`
mode `0600`; never place it in Terraform state, GitHub Actions, shell history, or
this repository. Install the Vault CA at `/etc/funding-v1/vault-ca.crt` and pin an
HTTPS `VAULT_ADDR`.

The `credential-policy` secret contains a validated JSON document in key
`document`. Exchange API keys must be dedicated-subaccount, read+trade only,
withdraw/transfer disabled, and restricted to the VM's exact egress IP.

Vault Agent only renders configuration. It never enables `LIVE_AUTOTRADE`,
changes `LIVE_ARMED`, or submits an order.