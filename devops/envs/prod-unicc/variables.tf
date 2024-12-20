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
