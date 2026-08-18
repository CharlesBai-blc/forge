# Burst worker instances (FR-21, FR-22, tdd.md §4.10). The burst
# controller drives this module: instance_count is the desired count
# and enroll_token is a one-time token for the instance created by the
# current apply (existing instances ignore user_data changes).

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "instance_count" {
  description = "Desired burst instance count; the controller passes -var instance_count=n"
  type        = number
  default     = 0
}

variable "instance_type" {
  description = "Instance type (tdd.md §9 OQ4: Graviton spot)"
  type        = string
  default     = "c7g.2xlarge"
}

variable "spot" {
  description = "Use spot instances; set false for on-demand fallback"
  type        = bool
  default     = true
}

variable "control_plane_url" {
  description = "Control plane URL reachable from the instances (FR-3 enrollment)"
  type        = string
}

variable "enroll_token" {
  description = "One-time burst enrollment token for the newest instance (FR-21)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "agent_download_url" {
  description = "URL of a linux/arm64 forge-agent binary the instances bootstrap from"
  type        = string
}
