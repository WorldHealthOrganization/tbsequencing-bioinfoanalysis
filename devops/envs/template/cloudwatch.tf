resource "aws_cloudwatch_event_rule" "trigger_query" {
  name = "${var.project_name}-${var.module_name}-master-pipeline-schedule"
  # Read: https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html
  schedule_expression = "rate(3 days)"
  state               = "DISABLED"

}

resource "aws_cloudwatch_event_target" "pipeline_master_event_target" {
  target_id = "${var.project_name}-${var.module_name}-master-pipeline-schedule"
  rule      = aws_cloudwatch_event_rule.trigger_query.name
  arn       = module.pipeline_master.state_machine_arn
  role_arn  = resource.aws_iam_role.eventbridge.arn
}

resource "aws_cloudwatch_log_group" "sync" {
  name = "/aws/batch/job/${local.prefix}-BioPython-fargate"
  tags = {
    Name = "/aws/batch/job/${local.prefix}-BioPython-fargate",
  }
}
