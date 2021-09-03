
variable "region" {
  default = "us-east-1"
}

variable "company" {
  default = "cenco"
}

variable "business" {
  default = "super"
}

variable "environment" {
  default = "dev"
}


variable "bucket_raw_data" {
  default = "companystream-datalake-dev-raw"
}

variable "bucket_analytics_data" {
  default = "companystream-datalake-dev-lake"
}

variable "bucket_arch_data" {
  default = "companystream-datalake-dev-arch"
}

variable "bucket_code_data" {
  default = "companystream-datalake-dev-code"
}

variable "glue_database_raw" {
  default = "companystream_datalake_dev_raw"
}

variable "glue_database_analytics" {
  default = "companystream_datalake_dev_analytics"
}

variable "glue_job_parquet" {
  default = "job-companystream-datalake"
}

variable "lambda_function_name" {
  default = "companystream-datalake-dev-create-athena"
}
