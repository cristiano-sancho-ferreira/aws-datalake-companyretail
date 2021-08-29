provider "aws" {
  region = var.region
}

# Centralizar o arquivo de controle de estado do terraform
terraform {
  backend "s3" {
    bucket = "terraform-state-sancho"
    key    = "state/cenco/terraform.tfstate"
    region = "us-east-1"
  }
}