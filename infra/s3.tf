# HCL - Hashicorp Configuration Language
# Liguagem declarativa

resource "aws_s3_bucket" "lake_raw" {
  # Parametros de configuracao do recurso escolhido
  bucket = var.bucket_raw_data
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    "projeto" = "companystream"
  }
}

resource "aws_s3_bucket" "lake_analytics" {
  # Parametros de configuracao do recurso escolhido
  bucket = var.bucket_analytics_data
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    "projeto" = "companystream"
  }
}


resource "aws_s3_bucket" "lake_arch" {
  # Parametros de configuracao do recurso escolhido
  bucket = var.bucket_arch_data
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    "projeto" = "companystream"
  }
}

resource "aws_s3_bucket" "lake_code" {
  # Parametros de configuracao do recurso escolhido
  bucket = var.bucket_code_data
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    "projeto" = "companystream"
  }
}



