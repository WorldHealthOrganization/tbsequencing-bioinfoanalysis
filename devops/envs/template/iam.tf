resource "aws_iam_role" "ecs_instance_role" {
  name               = "${local.prefix}-ecs-instance-role"
  assume_role_policy = data.aws_iam_policy_document.assume-tasks.json
}

resource "aws_iam_role" "eventbridge" {
  name               = "${local.prefix}-eb-rule"
  assume_role_policy = data.aws_iam_policy_document.assume-eb.json
}

resource "aws_iam_policy" "eventbridge" {
  policy = data.aws_iam_policy_document.eb.json
}

resource "aws_iam_policy" "fargate-exec" {
  policy = data.aws_iam_policy_document.fargate_execution.json
}

resource "aws_iam_role" "fargate-exec" {
  name               = "${local.prefix}-batch-fargate"
  assume_role_policy = data.aws_iam_policy_document.assume-tasks.json
}

resource "aws_iam_role_policy_attachment" "fargate_exec_attachment" {
  role       = resource.aws_iam_role.fargate-exec.name
  policy_arn = resource.aws_iam_policy.fargate-exec.arn
}

resource "aws_iam_role_policy_attachment" "eb_rule_attachment" {
  role       = resource.aws_iam_role.eventbridge.name
  policy_arn = resource.aws_iam_policy.eventbridge.arn
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role_attachment_batch" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role_attachment_s3" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role_attachment_rds" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = data.aws_iam_policy.rds_iam_access.arn
}


resource "aws_iam_instance_profile" "aws_iam_instance_profile" {
  name = "${local.prefix}-ecs-instance-profile"
  role = aws_iam_role.ecs_instance_role.name
}


// TODO
# Amazon Resource Name (ARN) of the IAM role that allows AWS Batch to make calls to other AWS services on your behalf
resource "aws_iam_role" "batch_service_role" {
  name = "${local.prefix}-batch-service-role"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {
        "Service": [
          "batch.amazonaws.com",
          "delivery.logs.amazonaws.com",
          "logs.amazonaws.com"
          ]
        }
    }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "aws_batch_service_role" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role_policy_attachment" "aws_batch_service_role2" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "aws_batch_service_role3" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role" "batch_spot_fleet_role" {
  name = "${local.prefix}-batch-spot-fleet-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "spotfleet.amazonaws.com"
      }
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "batch_spot_fleet_role_ec2_policies" {
  role       = aws_iam_role.batch_spot_fleet_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

resource "aws_iam_role_policy_attachment" "batch_spot_fleet_role_ecr_policy" {
  role       = aws_iam_role.batch_spot_fleet_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

resource "aws_iam_role_policy_attachment" "batch_spot_fleet_role-attachment" {
  role       = aws_iam_role.batch_spot_fleet_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy" "S3-Access-Batch" {
  name       = "${var.environment}-S3-Access-policy"
  depends_on = [aws_iam_role.batch_service_role]
  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [
      {
        Effect : "Allow",
        Action : [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:RestoreObject",
          "s3:ListBucket",
          "s3:GetObjectVersion",
          "s3:PutObject",
        ],
        Resource : [
          "arn:aws:s3:::*/*"
        ]
      }
    ]
  })
  role = aws_iam_role.batch_service_role.name
}

resource "aws_iam_role_policy" "ECS-Access-Batch" {
  name       = "${var.environment}-ECS-Access-policy"
  depends_on = [aws_iam_role.batch_service_role]
  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [
      {
        Effect : "Allow",
        Action : [
          "ecs:ListClusters",
          "ecs:DeleteCluster"
        ],
        Resource : [
          "arn:aws:ecs:::cluster/*"
        ]
      }
    ]
  })
  role = aws_iam_role.batch_service_role.name
}

resource "aws_iam_role" "glue_role" {
  name = "${local.prefix}-glue-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}


resource "aws_iam_policy" "glue_policy_s3" {
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    actions = [
      "glue:GetConnection"
    ]

    resources = [
      "*",
    ]
  }

  statement {
    actions = [
      "s3:GetObject"
    ]

    resources = [
      "arn:aws:s3:::${module.s3_for_fsx.bucket_id["fsx-export"]}/*",
    ]
  }
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]

    resources = [
      "${module.s3_for_fsx.bucket_arn["glue-logs-bucket"]}/*",
    ]
  }

  statement {
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${module.s3_for_fsx.bucket_arn["glue-scripts"]}/*"
    ]
  }
}

resource "aws_iam_role_policy_attachment" "glue_to_s3_role_attachment" {
  policy_arn = aws_iam_policy.glue_policy_s3.arn
  role       = aws_iam_role.glue_role.name
}

resource "aws_iam_policy" "glue_access_policy" {

  policy = templatefile("./glue_policy.json",
    {
      RDSCredentialsArn = data.aws_ssm_parameter.rds_credentials_secret_arn.value
  })
}

resource "aws_iam_role_policy_attachment" "glue_access_policy" {
  policy_arn = aws_iam_policy.glue_access_policy.arn
  role       = aws_iam_role.glue_role.name
}

resource "aws_iam_role_policy_attachment" "glue_role_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_role.name
}
resource "aws_iam_service_linked_role" "elasticfilesystem" {
  aws_service_name = "elasticfilesystem.amazonaws.com"
}
