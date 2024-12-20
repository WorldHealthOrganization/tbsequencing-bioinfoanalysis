#step function step_functions
#Child Pipeline
module "pipeline_child" {
  source = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//step_functions?ref=step_functions-v1.1"

  name              = "${local.prefix}-${local.pipeline_variant_calling_name}"
  create_role       = true
  use_existing_role = false
  role_name         = format("%s-child-pipeline-role", substr("${local.prefix}", 0, 43))
  type              = "standard"
  definition = templatefile("pipeline_child.json",
    {
      BioPython = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-BioPython"]
      Bwa       = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Bwa"]
      Samtools  = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Samtools"]
      Bcftools  = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Bcftools"]
      Kraken    = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Kraken"]
      Gatk      = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Gatk"]
      Freebayes = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Freebayes"]
      Sratools  = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Sratools"]
      Delly     = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Delly"]
      Mosdepth  = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Mosdepth"]
      Bedtools  = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Bedtools"]
      FastQC    = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-FastQC"]

      getSampleName            = module.bioanalysis-QueryRDS.lambda_function_arn
      getLibraryIdsForSample   = module.bioanalysis-QueryRDS.lambda_function_arn
      getLibraryCountForSample = module.bioanalysis-QueryRDS.lambda_function_arn
      UpdateStatus             = module.bioanalysis-QueryRDS.lambda_function_arn

      sequenceDataBucket = "${var.project_name}-main-${var.environment}-backend-sequence-data"
  })


  logging_configuration = {
    include_execution_data = true
    level                  = "ALL"
  }

  attach_policy_json = true
  policy_json        = data.aws_iam_policy_document.child_pipeline.json

  tags = {
    Name = "${local.prefix}-child-pipeline-module"
  }
}

data "aws_iam_policy_document" "child_pipeline" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "batch:SubmitJob",
      "batch:DescribeJobs",
      "batch:TerminateJob",
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
      "batch:DescribeJobQueues",
      "lambda:InvokeFunction",
      "s3:*"
    ]
    resources = [
      "*",
    ]
  }
}

#master pipeline
module "pipeline_master" {
  source = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//step_functions?ref=step_functions-v1.1"

  name              = "${local.prefix}-${local.pipeline_master_name}"
  create_role       = true
  use_existing_role = false
  role_name         = "${local.prefix}-master-pipeline-role"
  type              = "standard"
  definition = templatefile("pipeline_master.json",
    {
      BioPython                    = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-BioPython"]
      Bwa                          = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Bwa"]
      Samtools                     = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Samtools"]
      Gatk                         = module.batch_job_definition_ec2.batch_job_definition_arn["${local.prefix}-Gatk"]
      GetSamples                   = module.bioanalysis-QueryRDS.lambda_function_arn
      PrepareSamples               = module.bioanalysis-QueryRDS.lambda_function_arn
      UpdateStatus                 = module.bioanalysis-QueryRDS.lambda_function_arn
      WorkflowVariantCallingArn    = module.pipeline_child.state_machine_arn
      WorkflowDataInsertionArn     = module.pipeline_insert_processed_data.state_machine_arn
      WorkflowVariantAnnotationArn = module.pipeline_variant_annotation.state_machine_arn
      WorkflowStatsCalculationArn  = module.pipeline_calculate_statistics.state_machine_arn
      OutputBucket                 = module.s3_for_fsx.bucket_id["fsx-export"]
      LambdaQueryRDS               = module.bioanalysis-QueryRDS.lambda_function_arn
      InstanceProfileRoleArn       = aws_iam_instance_profile.aws_iam_instance_profile.arn
      ServiceRoleArn               = aws_iam_role.batch_service_role.arn
      SecurityGroupId              = data.aws_security_group.batch-compute.id
      SubnetId                     = var.low_cost_implementation ? data.aws_subnets.public-a[0].ids[0] : data.aws_subnets.private-a[0].ids[0]

      Project      = local.prefix
      FleetRoleArn = aws_iam_role.batch_spot_fleet_role.arn
      AccountId    = local.account_id
      Region       = local.aws_region

      FargateQueueArn = module.bioanalysis-queue-fargate.batch_job_queue_arn
      EC2QueueArn     = module.bioanalysis-queue-ec2.batch_job_queue_arn

      DbHost     = data.aws_ssm_parameter.db_host.value
      DbName     = data.aws_ssm_parameter.db_name.value
      DbUser     = "rdsiamuser"
      DbPassword = "RDS"
      DbPort     = "5432"

      GlueGenotypeJobName      = module.glue.glue_job_name["genotype"]
      GlueDellyGenotypeJobName = module.glue.glue_job_name["deletion"]
      GlueDelVariantsJobName   = module.glue.glue_job_name["del_variants"]
      GlueJoinGenotypeJobName  = module.glue.glue_job_name["join_genotype"]
      GlueTaxonomyJobName      = module.glue.glue_job_name["taxonomy_assignment"]
      GlueLocusStatsJobName    = module.glue.glue_job_name["locus_stats"]
      GlueGlobalStatsJobName   = module.glue.glue_job_name["global_stats"]

  })

  logging_configuration = {
    include_execution_data = true
    level                  = "ALL"
  }

  attach_policy_json = true

  policy_json = data.aws_iam_policy_document.master_pipeline.json

  tags = {
    Name = "${local.prefix}-master-pipeline-module"
  }
}

data "aws_iam_policy_document" "master_pipeline" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "batch:SubmitJob",
      "batch:DescribeJobs",
      "batch:TerminateJob",
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
      "batch:DescribeJobQueues",
      "lambda:InvokeFunction",
      "batch:createComputeEnvironment",
      "batch:createJobQueue",
      "batch:deleteComputeEnvironment",
      "batch:deleteJobQueue",
      "batch:describeComputeEnvironments",
      "batch:describeJobQueues",
      "batch:tagResource",
      "batch:updateComputeEnvironment",
      "batch:updateJobQueue",
      "ec2:createLaunchTemplate",
      "ec2:deleteLaunchTemplate",
      "ec2:describeLaunchTemplates",
      "fsx:describeFileSystems",
      "fsx:CreateFileSystem",
      "fsx:TagResource",
      "s3:*",
      "iam:AttachRolePolicy",
      "iam:PutRolePolicy",
      "ec2:CreateNetworkInterface",
      "iam:CreateServiceLinkedRole",
      "ec2:CreateTags",
      "iam:PassRole",
      "states:StartExecution",
      "fsx:DeleteFileSystem",
      "batch:deleteJobQueue",
      "batch:deleteComputeEnvironment",
      "ec2:deleteLaunchTemplate",
      "states:ListExecutions"
    ]
    resources = [
      "*",
    ]
  }
}

# Insert Process data pipeline
module "pipeline_insert_processed_data" {
  source = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//step_functions?ref=step_functions-v1.1"

  name              = "${local.prefix}-${local.pipeline_data_insertion_name}"
  create_role       = true
  use_existing_role = false
  role_name         = format("%s-insert_processed_data-pipeline-role", substr("${local.prefix}", 0, 28))
  type              = "standard"
  definition = templatefile("pipeline_insert_processed_data.json",
    {
      BioPythonFargate = module.batch_job_definition_fargate.batch_job_fargate_definition_arn["${local.prefix}-BioPython-fargate"]
      LambdaQueryRDS   = module.bioanalysis-QueryRDS.lambda_function_arn

      FargateQueueArn = module.bioanalysis-queue-fargate.batch_job_queue_arn

      GlueGenotypeJobName       = module.glue.glue_job_name["genotype"]
      GlueNewVarGenotypeJobName = module.glue.glue_job_name["new_var_genotype"]
      GlueDellyGenotypeJobName  = module.glue.glue_job_name["deletion"]
      GlueDelVariantsJobName    = module.glue.glue_job_name["del_variants"]
      GlueJoinGenotypeJobName   = module.glue.glue_job_name["join_genotype"]
      GlueTaxonomyJobName       = module.glue.glue_job_name["taxonomy_assignment"]
      GlueLocusStatsJobName     = module.glue.glue_job_name["locus_stats"]
      GlueGlobalStatsJobName    = module.glue.glue_job_name["global_stats"]

      WorkflowVariantCallingArn    = "arn:aws:states:${local.aws_region}:${local.account_id}:stateMachine:${local.prefix}-${local.pipeline_variant_calling_name}"
      WorkflowVariantAnnotationArn = "arn:aws:states:${local.aws_region}:${local.account_id}:stateMachine:${local.prefix}-${local.pipeline_variant_annotation_name}"

  })

  logging_configuration = {
    include_execution_data = true
    level                  = "ALL"
  }

  attach_policy_json = true
  policy_json        = data.aws_iam_policy_document.pipeline_insert_processed_data.json

  tags = {
    Name = "${local.prefix}-insert_processed_data-pipeline-module"
  }
}

data "aws_iam_policy_document" "pipeline_insert_processed_data" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "batch:SubmitJob",
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
      "lambda:InvokeFunction",
      "s3:*",
      "iam:AttachRolePolicy",
      "iam:PutRolePolicy",
      "iam:CreateServiceLinkedRole",
      "iam:PassRole",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
      "glue:GetCrawlerMetrics",
      "glue:StartCrawler",
      "glue:GetCrawler",
      "glue:GetCrawlers",
      "states:ListExecutions"
    ]
    resources = [
      "*"
    ]
  }
}

# Variant Annotation pipeline
module "pipeline_variant_annotation" {
  source = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//step_functions?ref=step_functions-v1.1"

  name              = "${local.prefix}-${local.pipeline_variant_annotation_name}"
  create_role       = true
  use_existing_role = false
  role_name         = format("%s-variant_annotation-pipeline-role", substr("${local.prefix}", 0, 30))
  type              = "standard"
  definition = templatefile("pipeline_variant_annotation.json",
    {
      BioPythonFargate       = module.batch_job_definition_fargate.batch_job_fargate_definition_arn["${local.prefix}-BioPython-fargate"]
      InstanceProfileRoleArn = aws_iam_role.ecs_instance_role.arn
      JobRoleArn             = data.aws_iam_role.fargate_task.arn
      SecurityGroupId        = data.aws_security_group.batch-compute.id
      SubnetId1              = var.low_cost_implementation ? data.aws_subnets.public-a[0].ids[0] : data.aws_subnets.private-a[0].ids[0]
      SubnetId2              = var.low_cost_implementation ? data.aws_subnets.public-b[0].ids[0] : data.aws_subnets.private-b[0].ids[0]

      Prefix    = local.prefix
      AccountId = local.account_id
      Region    = local.aws_region
      Project   = var.project_name

      FargateQueueArn = module.bioanalysis-queue-fargate.batch_job_queue_arn
      EC2QueueArn     = module.bioanalysis-queue-ec2.batch_job_queue_arn

      GluePredictResistanceJobName               = module.glue.glue_job_name["predict_resistance"]
      GlueWriteFormattedAnnotationPerGeneJobName = module.glue.glue_job_name["write_formatted_annotations_per_gene"]

      UpdateStatus = module.bioanalysis-QueryRDS.lambda_function_arn

      WorkflowVariantCallingArn = "arn:aws:states:${local.aws_region}:${local.account_id}:stateMachine:${local.prefix}-${local.pipeline_variant_calling_name}"
      WorkflowDataInsertionArn  = "arn:aws:states:${local.aws_region}:${local.account_id}:stateMachine:${local.prefix}-${local.pipeline_data_insertion_name}"

      DbHost     = data.aws_ssm_parameter.db_host.value
      DbName     = data.aws_ssm_parameter.db_name.value
      DbUser     = "rdsiamuser"
      DbPassword = "RDS"
      DbPort     = "5432"

  })

  logging_configuration = {
    include_execution_data = true
    level                  = "ALL"
  }

  attach_policy_json = true
  policy_json        = data.aws_iam_policy_document.pipeline_variant_annotation.json

  tags = {
    Name = "${local.prefix}-variant_annotation-pipeline-module"
  }
}
data "aws_iam_policy_document" "pipeline_variant_annotation" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "batch:SubmitJob",
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
      "s3:*",
      "iam:AttachRolePolicy",
      "iam:PutRolePolicy",
      "iam:CreateServiceLinkedRole",
      "iam:PassRole",
      "efs:createFileSystem",
      "efs:createMountTarget",
      "batch:registerJobDefinition",
      "efs:describeMountTargets",
      "batch:deregisterJobDefinition",
      "efs:deleteMountTarget",
      "efs:deleteFileSystem",
      "elasticfilesystem:CreateFileSystem",
      "elasticfilesystem:createMountTarget",
      "elasticfilesystem:describeMountTargets",
      "elasticfilesystem:deleteMountTarget",
      "elasticfilesystem:deleteFileSystem",
      "elasticfilesystem:tagResource",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
      "lambda:InvokeFunction",
      "states:ListExecutions"
    ]
    resources = [
      "*"
    ]
  }
}


# Calculate statistics pipeline
module "pipeline_calculate_statistics" {
  source = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//step_functions?ref=step_functions-v1.1"

  name              = "${local.prefix}-${local.pipeline_calculate_statistics_name}"
  create_role       = true
  use_existing_role = false
  role_name         = format("%s-calculate_statistics-pipeline-role", substr("${local.prefix}", 0, 29))
  type              = "standard"
  definition = templatefile("pipeline_calculate_statistics.json",
    {
      LambdaQueryRDS   = module.bioanalysis-QueryRDS.lambda_function_arn
      FargateQueueArn  = module.bioanalysis-queue-fargate.batch_job_queue_arn
      BioPythonFargate = module.batch_job_definition_fargate.batch_job_fargate_definition_arn["${local.prefix}-BioPython-fargate"]
  })

  logging_configuration = {
    include_execution_data = true
    level                  = "ALL"
  }

  attach_policy_json = true
  policy_json        = data.aws_iam_policy_document.pipeline_calculate_statistics.json

  tags = {
    Name = "${local.prefix}-calculate_statistics-pipeline-module"
  }
}

data "aws_iam_policy_document" "pipeline_calculate_statistics" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "batch:SubmitJob",
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
      "lambda:InvokeFunction",
      "s3:*",
      "iam:AttachRolePolicy",
      "iam:PutRolePolicy",
      "iam:CreateServiceLinkedRole",
      "iam:PassRole",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
      "states:ListExecutions"
    ]
    resources = [
      "*"
    ]
  }
}
