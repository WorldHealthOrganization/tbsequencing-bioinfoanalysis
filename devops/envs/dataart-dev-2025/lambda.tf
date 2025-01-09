module "bioanalysis-QueryRDS" {
  source = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//lambda?ref=lambda-v1.0"
  # source = "terraform-aws-modules/lambda/aws"
  # version = "~> 4.7.1"

  function_name          = "${local.prefix}-QueryRDS"
  description            = "QueryRDS lambda function"
  handler                = "lambda_function.lambda_handler"
  runtime                = "python3.10"
  source_path            = "../../../cfn/lambda/QueryRDS"
  timeout                = 900
  attach_network_policy  = true
  memory_size            = 512
  vpc_subnet_ids         = var.low_cost_implementation ? [data.aws_subnets.public-a[0].ids[0], data.aws_subnets.public-b[0].ids[0]] : [data.aws_subnets.private-a[0].ids[0], data.aws_subnets.private-b[0].ids[0]]
  vpc_security_group_ids = [data.aws_security_group.postgres.id]

  role_path   = "/tf-managed/"
  policy_path = "/tf-managed/"

  attach_policy_jsons    = true
  policy_jsons           = [data.aws_iam_policy_document.lambda_policy.json]
  number_of_policy_jsons = 1
}


data "aws_iam_policy_document" "lambda_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "rds-db:connect"
    ]
    resources = [
      "arn:aws:rds-db:${local.aws_region}:${local.account_id}:dbuser:*/rdsiamuser"
    ]
  }
}
