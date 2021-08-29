# HCL - Hashicorp Configuration Language
# Liguagem declarativa

resource "aws_s3_bucket" "datalake" {
  # Parametros de configuracao do recurso escolhido
  bucket = var.base_bucket_name
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    "curso" = "igti"
  }
}
