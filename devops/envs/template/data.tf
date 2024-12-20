data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "db_host" {
  name = "/${var.environment}/db_host"
}

data "aws_ssm_parameter" "db_name" {
  name = "/${var.environment}/db_name"
}

data "aws_ssm_parameter" "db_port" {
  name = "/${var.environment}/db_port"
}

data "aws_ssm_parameter" "rds_credentials_secret_arn" {
  name = "/${var.environment}/rds_credentials_secret_arn"
}

data "aws_subnets" "private-a" {
  count = var.low_cost_implementation ? 0 : 1

  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-private-${local.aws_region}a"]
  }
}

data "aws_subnets" "private-b" {
  count = var.low_cost_implementation ? 0 : 1

  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-private-${local.aws_region}b"]
  }
}

data "aws_subnets" "public-a" {
  count = var.low_cost_implementation ? 1 : 0

  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-public-${local.aws_region}a"]
  }
}

data "aws_subnets" "public-b" {
  count = var.low_cost_implementation ? 1 : 0

  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-public-${local.aws_region}b"]
  }
}

data "aws_security_group" "batch-compute" {
  filter {
    name   = "tag:Label"
    values = ["${var.project_name}-main-${var.environment}-batch-compute"]
  }
}

data "aws_security_group" "postgres" {
  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-postgresql"]
  }
}

data "aws_security_group" "glue" {
  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-glue"]
  }
}

data "aws_iam_role" "fargate_execution" {
  name = "${var.project_name}-ncbi-${var.environment}-fargate-execution"
}

data "aws_iam_role" "fargate_task" {
  name = "${var.project_name}-ncbi-${var.environment}-fargate-task"
}

data "aws_iam_policy" "rds_iam_access" {
  arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.project_name}-main-${var.environment}-rds_access"
}
