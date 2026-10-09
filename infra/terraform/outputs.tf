output "cloud_init" {
  description = "Provider-neutral cloud-init. Pass it as user_data when creating a new Ubuntu 24.04 VM."
  value       = local.cloud_init
  sensitive   = true
}

output "cloud_init_sha256" {
  description = "Immutable fingerprint for operator review before provisioning."
  value       = terraform_data.host_policy.output.cloud_init_sha256
}

output "compose_project_name" {
  value = terraform_data.host_policy.output.project_name
}