#batch Job Definitions
module "batch_job_definition_ec2" {
  source                = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//batch_job_definition_ec2?ref=batch_job_definition_ec2-v2.0"
  batch_job_definitions = local.batch_job_definitions
}

module "batch_job_definition_fargate" {
  source                        = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//batch_job_definition_fargate?ref=batch_job_definition_fargate-v2.2"
  project_name                  = var.project_name
  module_name                   = "main"
  environment                   = var.environment
  batch_job_fargate_definitions = local.batch_job_fargate_definitions
}

locals {
  batch_job_definitions = {
    "${local.prefix}-Bwa" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-bwa:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "4"
      container_memory = "4096"
    }
    "${local.prefix}-BioPython" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-biopython:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "1024"
    }
    "${local.prefix}-Samtools" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-samtools:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "4"
      container_memory = "8192"
    }
    "${local.prefix}-Bcftools" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-bcftools:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "2048"
    }
    "${local.prefix}-Kraken" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-kraken:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "4"
      container_memory = "24576"
    }
    "${local.prefix}-Gatk" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-gatk:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "4096"
    }
    "${local.prefix}-Freebayes" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-freebayes:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "8192"
    }
    "${local.prefix}-Sratools" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-sra-tools:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "4096"
    }
    "${local.prefix}-Delly" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-delly:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "2"
      container_memory = "4096"
    }
    "${local.prefix}-Mosdepth" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-mosdepth:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "1024"
    }
    "${local.prefix}-Bedtools" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-bedtools:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "1"
      container_memory = "2048"
    }
    "${local.prefix}-FastQC" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-fastqc:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "2"
      container_memory = "8192"
    }
  }
  batch_job_fargate_definitions = {
    "${local.prefix}-BioPython-fargate" = {
      image = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.project_name}-genomicsworkflow-biopython:latest"
      command = [
        "echo",
        "test",
      ]
      container_vcpu   = "0.25"
      container_memory = "1024"
      taskRoleArn      = data.aws_iam_role.fargate_task.arn
      executionRoleArn = resource.aws_iam_role.fargate-exec.arn
      assignPublicIp   = var.low_cost_implementation ? "ENABLED" : "DISABLED"
    }
  }
}


#Batch ENVS
module "bioanalysis-env-ec2" {
  source               = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//batch_compute_env_ec2?ref=batch_compute_env_ec2-v2.1"
  batch_name           = "${local.prefix}-ec2"
  launch_template_name = "${local.prefix}-ec2"
  environment          = local.prefix
  compute_env_name     = "${local.prefix}-ec2"
  service_role_name    = aws_iam_role.batch_service_role.name
  service_role_arn     = aws_iam_role.batch_service_role.arn
  instance_profile     = aws_iam_instance_profile.aws_iam_instance_profile.arn
  desired_vcpus        = 0
  subnet_ids           = var.low_cost_implementation ? [data.aws_subnets.public-a[0].ids[0], data.aws_subnets.public-b[0].ids[0]] : [data.aws_subnets.private-a[0].ids[0], data.aws_subnets.private-b[0].ids[0]]
  security_group_id    = data.aws_security_group.batch-compute.id
}

module "bioanalysis-env-fargate" {
  source            = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//batch_compute_env_fargate?ref=batch_compute_env_fargate-v2.1"
  compute_env_name  = "${local.prefix}-fargate"
  service_role_name = aws_iam_role.batch_service_role.name
  service_role_arn  = aws_iam_role.batch_service_role.arn
  subnet_ids        = var.low_cost_implementation ? [data.aws_subnets.public-a[0].ids[0], data.aws_subnets.public-b[0].ids[0]] : [data.aws_subnets.private-a[0].ids[0], data.aws_subnets.private-b[0].ids[0]]
  security_group_id = data.aws_security_group.batch-compute.id
}

#batch queue
module "bioanalysis-queue-fargate" {
  source               = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//batch_queue?ref=batch_queue-v2.1"
  queue_name           = "${local.prefix}-fargate"
  compute_environments = module.bioanalysis-env-fargate.arn
}

module "bioanalysis-queue-ec2" {
  source               = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//batch_queue?ref=batch_queue-v2.1"
  queue_name           = "${local.prefix}-ec2"
  compute_environments = module.bioanalysis-env-ec2.batch_compute_environment_arn
}
