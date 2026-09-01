pid_file = "/run/vault-agent-funding/vault-agent.pid"
exit_after_auth = false

vault {
  retry {
    num_retries = 5
  }
}

auto_auth {
  method {
    type       = "approle"
    mount_path = "auth/approle"
    config = {
      role_id_file_path                   = "/etc/funding-v1/approle-role-id"
      secret_id_file_path                 = "/run/credentials/vault-agent-funding.service/approle-secret-id"
      remove_secret_id_file_after_reading = false
    }
  }
}

template_config {
  exit_on_retry_failure         = true
  static_secret_render_interval = "5m"
  max_connections_per_host      = 4
}

template {
  source               = "/etc/funding-v1/runtime.env.ctmpl"
  destination          = "${app_dir}/secrets/exchange/runtime.env"
  perms                = "0600"
  backup               = false
  error_on_missing_key = true
}

template {
  source               = "/etc/funding-v1/credential-policy.json.ctmpl"
  destination          = "${app_dir}/secrets/exchange/credential-policy.json"
  perms                = "0600"
  backup               = false
  error_on_missing_key = true
}

template {
  source               = "/etc/funding-v1/telegram.ctmpl"
  destination          = "${app_dir}/secrets/exchange/telegram.env"
  perms                = "0600"
  backup               = false
  error_on_missing_key = true
}
template {
  source               = "/etc/funding-v1/telegram-bot-token.ctmpl"
  destination          = "${app_dir}/secrets/exchange/telegram-bot-token"
  perms                = "0600"
  backup               = false
  error_on_missing_key = true
}

template {
  source               = "/etc/funding-v1/telegram-chat-id.ctmpl"
  destination          = "${app_dir}/secrets/exchange/telegram-chat-id"
  perms                = "0600"
  backup               = false
  error_on_missing_key = true
}
