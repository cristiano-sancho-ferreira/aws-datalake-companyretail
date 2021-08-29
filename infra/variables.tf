
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
  default = "csancho-datalake-dev-raw"
}

variable "bucket_analytics_data" {
  default = "csancho-datalake-dev-lake"
}

variable "bucket_arch_data" {
  default = "csancho-datalake-dev-arch"
}

variable "bucket_code_data" {
  default = "csancho-datalake-dev-code"
}

variable "glue_database_raw" {
  default = "csancho_datalake_dev_raw"
}

variable "glue_database_analytics" {
  default = "csancho_datalake_dev_analytics"
}
