
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
  default = "companyretail-datalake-dev-raw"
}

variable "bucket_analytics_data" {
  default = "companyretail-datalake-dev-lake"
}

variable "bucket_arch_data" {
  default = "companyretail-datalake-dev-arch"
}

variable "bucket_code_data" {
  default = "companyretail-datalake-dev-code"
}

variable "glue_database_raw" {
  default = "companyretail_datalake_dev_raw"
}

variable "glue_database_analytics" {
  default = "companyretail_datalake_dev_analytics"
}

variable "glue_job_parquet" {
  default = "job-companyretail-datalake"
}

variable "lambda_function_name" {
  default = "companyretail-datalake-dev-create-athena"
}
