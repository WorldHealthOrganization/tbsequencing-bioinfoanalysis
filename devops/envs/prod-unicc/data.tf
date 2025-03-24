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
  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-private-${local.aws_region}a"]
  }
}
data "aws_subnets" "private-b" {
  filter {
    name   = "tag:Name"
    values = ["${var.project_name}-main-${var.environment}-private-${local.aws_region}b"]
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

data "aws_iam_role" "ecs_task_role" {
  name = "${var.project_name}-ncbi-${var.environment}-ecs_fargate_task_execution_role"
}

data "aws_ssm_parameter" "static_files_bucket_name" {
  name = "/${var.environment}/static_files_bucket_name"
}

data "aws_ssm_parameter" "sequence_data_bucket_name" {
  name = "/${var.environment}/sequence_data_bucket_name"
}
