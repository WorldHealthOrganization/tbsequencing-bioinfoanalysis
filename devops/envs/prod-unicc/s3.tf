module "s3_for_fsx" {
  source       = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//s3?ref=s3-v1.3"
  s3_buckets   = local.s3_bucket_names
  environment  = var.environment
  project_name = var.project_name
  module_name  = var.module_name
  tags         = local.tags
}

locals {
  s3_bucket_names = {
    fsx-export = {
      enable_versioning = true
      bucket_acl        = false
      enable_cors       = false
      enable_policy     = false
      bucket_owner_acl  = false
      cors_rule         = []
      policy            = null
    }
    glue-logs-bucket = {
      enable_versioning = false
      bucket_acl        = false
      enable_cors       = false
      enable_policy     = false
      bucket_owner_acl  = false
      cors_rule         = []
      policy            = null
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
      "arn:aws:s3:::${local.prefix}-fsx-export/*",
    ]
  }
}
