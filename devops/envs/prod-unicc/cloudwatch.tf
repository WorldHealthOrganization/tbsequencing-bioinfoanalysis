resource "aws_cloudwatch_event_rule" "trigger_query" {
  name = "${var.project_name}-${var.module_name}-master-pipeline-schedule"

  # Read: https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html
  schedule_expression = "rate(3 days)"
}

resource "aws_cloudwatch_event_target" "pipeline_master_event_target" {
  target_id = "${var.project_name}-${var.module_name}-master-pipeline-schedule"
  rule      = aws_cloudwatch_event_rule.trigger_query.name
  arn       = module.pipeline_master.state_machine_arn
  role_arn  = module.pipeline_master.role_arn
}
