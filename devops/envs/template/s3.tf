module "s3_for_fsx" {
  source       = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//s3?ref=s3-v1.9"
  s3_buckets   = local.s3_bucket_names
  environment  = var.environment
  project_name = var.project_name
  module_name  = var.module_name
  tags         = local.tags
}

locals {
  s3_bucket_names = {
    fsx-export = {
      enable_versioning   = true
      bucket_acl          = false
      enable_cors         = false
      enable_policy       = false
      bucket_owner_acl    = false
      cors_rule           = []
      policy              = null
      enable_notification = false
    }
    glue-logs-bucket = {
      enable_versioning   = false
      bucket_acl          = false
      enable_cors         = false
      enable_policy       = false
      bucket_owner_acl    = false
      cors_rule           = []
      policy              = null
      enable_notification = false
    }
    glue-scripts = {
      enable_versioning   = true
      bucket_acl          = false
      enable_cors         = false
      enable_policy       = false
      bucket_owner_acl    = false
      policy              = null
      cors_rule           = []
      enable_notification = false
    }
  }
}

data "aws_iam_policy_document" "fsx_policy" {
  statement {
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    actions = [
      "s3:PutObject"
    ]
    resources = [
      format("%s/*", module.s3_for_fsx.bucket_arn["fsx-export"]),
    ]
  }
}
