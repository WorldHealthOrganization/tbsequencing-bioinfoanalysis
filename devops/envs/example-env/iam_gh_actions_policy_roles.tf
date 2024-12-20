data "aws_iam_policy_document" "glue-scripts-s3" {
  count = var.gh_action_roles ? 1 : 0
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      format("%s/*", module.s3_for_fsx.bucket_arn["glue-scripts"]),
      module.s3_for_fsx.bucket_arn["glue-scripts"]
    ]
  }
}

data "aws_iam_policy_document" "get-resources-by-tag" {
  count = var.gh_action_roles ? 1 : 0
  statement {
    effect = "Allow"
    actions = [
      "tag:GetResources",
    ]
    resources = [
      "*"
    ]
  }
}

data "aws_iam_openid_connect_provider" "gh" {
  count = var.gh_action_roles ? 1 : 0
  url   = "https://token.actions.githubusercontent.com"
}

data "aws_iam_policy_document" "oidc_policy" {
  for_each = local.repo_mappings
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type = "Federated"
      identifiers = [
        data.aws_iam_openid_connect_provider.gh[0].arn
      ]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = each.value.repos
    }
  }
}

locals {
  repo_mappings = var.gh_action_roles ? {
    "my-github-actions-copy-glue" = {
      repos = [
        "repo:${var.github_org_name}/${var.github_repo_prefix}-bioinfoanalysis:environment:${var.environment}",
      ]
    },
  } : {}

  policies_gh = var.gh_action_roles ? [
    {
      name        = "glue-scripts-s3"
      description = ""
      policy      = data.aws_iam_policy_document.glue-scripts-s3[0].json
    },
    {
      name        = "glue-get-resources-by-tag"
      description = ""
      policy      = data.aws_iam_policy_document.get-resources-by-tag[0].json
    }
  ] : []

  policy_mapping_gh = var.gh_action_roles ? {
    glue-s3 = {
      role   = module.roles-gh-actions[0].role_name["my-github-actions-copy-glue"]
      policy = module.policies-gh-actions[0].policy_arn["glue-scripts-s3"]
    },
    glue-get-resources = {
      role   = module.roles-gh-actions[0].role_name["my-github-actions-copy-glue"]
      policy = module.policies-gh-actions[0].policy_arn["glue-get-resources-by-tag"]
    }

  } : {}

  roles_gh = var.gh_action_roles ? [
    {
      name                    = "my-github-actions-copy-glue"
      instance_profile_enable = null
      custom_trust_policy     = data.aws_iam_policy_document.oidc_policy["my-github-actions-copy-glue"].json
    }
  ] : []
}

module "policies-gh-actions" {
  count        = var.gh_action_roles ? 1 : 0
  source       = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//iam_policy?ref=iam_policy-v1.0"
  aws_region   = local.aws_region
  environment  = var.environment
  project_name = var.project_name
  module_name  = var.module_name
  policies     = local.policies_gh
}

module "policy_mapping-gh-actions" {
  count      = var.gh_action_roles ? 1 : 0
  source     = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//iam_policy_mapping?ref=iam_policy_mapping-v1.0"
  aws_region = local.aws_region
  roles      = local.policy_mapping_gh
}

module "roles-gh-actions" {
  count        = var.gh_action_roles ? 1 : 0
  source       = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//iam_role?ref=iam_role-v1.0"
  aws_region   = local.aws_region
  environment  = var.environment
  project_name = var.project_name
  module_name  = var.module_name
  roles        = local.roles_gh
}
