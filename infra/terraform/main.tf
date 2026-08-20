locals {
  cloud_init = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    app_dir                 = var.app_dir
    backup_root             = var.backup_root
    data_dir                = var.data_dir
    operator_cidr           = var.operator_cidr
    project_name            = var.project_name
    ssh_port                = var.ssh_port
    ssh_public_key          = trimspace(var.ssh_public_key)
    vault_address           = var.vault_address
    vault_namespace         = var.vault_namespace
    vault_agent_hcl         = file("${path.module}/../vault/agent.hcl")
    vault_runtime_template  = file("${path.module}/../vault/runtime.env.ctmpl")
    vault_policy_template   = file("${path.module}/../vault/credential-policy.json.ctmpl")
    vault_telegram_template = file("${path.module}/../vault/telegram.ctmpl")
    vault_telegram_bot      = file("${path.module}/../vault/telegram-bot-token.ctmpl")
    vault_telegram_chat     = file("${path.module}/../vault/telegram-chat-id.ctmpl")
  })
}

resource "terraform_data" "host_policy" {
  input = {
    app_dir           = var.app_dir
    backup_root       = var.backup_root
    cloud_init_sha256 = sha256(local.cloud_init)
    data_dir          = var.data_dir
    operator_cidr     = var.operator_cidr
    project_name      = var.project_name
    ssh_port          = var.ssh_port
    vault_address     = var.vault_address
  }

  lifecycle {
    precondition {
      condition     = length(setintersection(toset([var.app_dir, var.data_dir, var.backup_root]), toset(["/", "/opt", "/srv", "/var", "/var/backups"]))) == 0
      error_message = "Dedicated paths must never resolve to a broad system directory."
    }

    precondition {
      condition     = var.app_dir != var.data_dir && var.app_dir != var.backup_root && var.data_dir != var.backup_root
      error_message = "Application, data, and backup paths must be distinct."
    }
  }
}