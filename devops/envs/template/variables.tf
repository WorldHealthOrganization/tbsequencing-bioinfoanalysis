variable "project_name" {
  type    = string
  default = "fdx"
}

variable "module_name" {
  type    = string
  default = "bio"
}

variable "environment" {
  type        = string
  description = "the name of your environment (dev, uat, prod)"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "low_cost_implementation" {
  type    = bool
  default = true
}

variable "gh_action_roles" {
  type    = bool
  default = false
}

variable "github_org_name" {
  type    = string
  default = ""
}

variable "github_repo_prefix" {
  type    = string
  default = ""
}
