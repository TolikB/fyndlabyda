variable "ssh_public_key" {
  description = "Operator SSH public key installed by cloud-init; private keys never enter Terraform."
  type        = string

  validation {
    condition     = can(regex("^ssh-(ed25519|rsa) [A-Za-z0-9+/=]+(?: .*)?$", trimspace(var.ssh_public_key)))
    error_message = "ssh_public_key must be a valid OpenSSH ed25519 or RSA public key."
  }
}

variable "operator_cidr" {
  description = "Single trusted operator CIDR allowed to reach SSH. Public-wide CIDRs are forbidden."
  type        = string

  validation {
    condition = (
      can(cidrhost(var.operator_cidr, 0)) &&
      (
        can(regex("^[0-9.]+/32$", var.operator_cidr)) ||
        can(regex("^[0-9A-Fa-f:]+/128$", var.operator_cidr))
      )
    )
    error_message = "operator_cidr must be one exact IPv4 /32 or IPv6 /128 operator address."
  }
}

variable "ssh_port" {
  description = "SSH port exposed only to operator_cidr."
  type        = number
  default     = 22

  validation {
    condition     = var.ssh_port >= 1 && var.ssh_port <= 65535
    error_message = "ssh_port must be between 1 and 65535."
  }
}

variable "project_name" {
  description = "Exact Docker Compose project boundary."
  type        = string
  default     = "funding_arbitrage_v1"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9_-]{2,62}$", var.project_name))
    error_message = "project_name must be a lowercase Compose-safe identifier."
  }
}

variable "app_dir" {
  description = "Dedicated application checkout and runtime directory."
  type        = string
  default     = "/opt/funding-arbitrage-v1"

  validation {
    condition = (
      startswith(var.app_dir, "/opt/") &&
      var.app_dir == trimsuffix(var.app_dir, "/") &&
      alltrue([
        for segment in split("/", trimprefix(var.app_dir, "/opt/")) :
        length(segment) > 0 &&
        !contains([".", ".."], segment) &&
        can(regex("^[A-Za-z0-9._-]+$", segment))
      ])
    )
    error_message = "app_dir must be a dedicated child of /opt."
  }
}

variable "data_dir" {
  description = "Dedicated persistent data mount."
  type        = string
  default     = "/srv/funding-arbitrage-v1"

  validation {
    condition = (
      startswith(var.data_dir, "/srv/") &&
      var.data_dir == trimsuffix(var.data_dir, "/") &&
      alltrue([
        for segment in split("/", trimprefix(var.data_dir, "/srv/")) :
        length(segment) > 0 &&
        !contains([".", ".."], segment) &&
        can(regex("^[A-Za-z0-9._-]+$", segment))
      ])
    )
    error_message = "data_dir must be a dedicated child of /srv."
  }
}

variable "backup_root" {
  description = "Dedicated encrypted backup root containing an identity marker."
  type        = string
  default     = "/var/backups/funding-arbitrage-v1"

  validation {
    condition = (
      startswith(var.backup_root, "/var/backups/") &&
      var.backup_root == trimsuffix(var.backup_root, "/") &&
      alltrue([
        for segment in split("/", trimprefix(var.backup_root, "/var/backups/")) :
        length(segment) > 0 &&
        !contains([".", ".."], segment) &&
        can(regex("^[A-Za-z0-9._-]+$", segment))
      ])
    )
    error_message = "backup_root must be a dedicated child of /var/backups."
  }
}

variable "vault_address" {
  description = "TLS-only external Vault address."
  type        = string

  validation {
    condition     = can(regex("^https://[^/[:space:]]+(?::[0-9]+)?$", var.vault_address))
    error_message = "vault_address must be an HTTPS origin without a path."
  }
}

variable "vault_namespace" {
  description = "Optional Vault Enterprise namespace."
  type        = string
  default     = ""
}
