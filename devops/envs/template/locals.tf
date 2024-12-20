locals {
  prefix           = "${var.project_name}-${var.module_name}-${var.environment}"
  account_id       = data.aws_caller_identity.current.account_id
  aws_region       = var.aws_region
  glue_jobs_bucket = "${var.project_name}-main-${var.environment}-glue-scripts"

  pipeline_master_name               = "master-pipeline"
  pipeline_variant_calling_name      = "child-pipeline"
  pipeline_data_insertion_name       = "insert_processed_data-pipeline"
  pipeline_variant_annotation_name   = "variant_annotation-pipeline"
  pipeline_calculate_statistics_name = "calculate_statistics-pipeline"

  tags = {
    Project     = var.project_name
    Module      = var.module_name
    Environment = var.environment
    Terraformed = "true"
  }
}
