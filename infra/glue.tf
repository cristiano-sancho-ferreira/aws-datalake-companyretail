resource "aws_glue_catalog_database" "glue_database_raw" {
  name = var.glue_database_raw
}

resource "aws_glue_catalog_database" "glue_database_lake" {
  name = var.glue_database_analytics
}


resource "aws_glue_job" "job-csancho" {
  name         = var.glue_job_parquet
  role_arn     = aws_iam_role.glue_role.arn
  max_capacity = 10
  glue_version = "2.0"

  command {
    script_location = "s3://${aws_s3_bucket.lake_code.id}/code/job_data_lake_glue.py"
    python_version  = 3
  }

  default_arguments = {
    # ... potentially other arguments ...
    "--environment"    = "dev"
    "--business"       = "datalake"
    "--interfacegroup" = "datalake_centro"
    "--company"        = "companyretail"
    "--job-language"   = "python"
  }

  tags = {
    "projeto" = "companystream"
  }
}

