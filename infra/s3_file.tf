# HCL - Hashicorp Configuration Language
# Liguagem declarativa

resource "aws_s3_bucket_object" "code_glue" {
  bucket = aws_s3_bucket.lake_code.id
  key    = "code/job_data_lake_glue.py"
  acl    = "private"
  source = "../code/job_data_lake_glue.py"
  etag   = filemd5("../code/job_data_lake_glue.py")
}


# data source
resource "aws_s3_bucket_object" "niv_niv0" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "niv_niv0/col_super_gnx_niv_niv0_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/niv_niv0/col_super_gnx_niv_niv0_20180607_03.dat.gz"
  etag   = filemd5("../data/niv_niv0/col_super_gnx_niv_niv0_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "niv_niv1" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "niv_niv1/col_super_gnx_niv_niv1_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/niv_niv1/col_super_gnx_niv_niv1_20180607_03.dat.gz"
  etag   = filemd5("../data/niv_niv1/col_super_gnx_niv_niv1_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "niv_niv2" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "niv_niv2/col_super_gnx_niv_niv2_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/niv_niv2/col_super_gnx_niv_niv2_20180607_03.dat.gz"
  etag   = filemd5("../data/niv_niv2/col_super_gnx_niv_niv2_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "niv_niv3" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "niv_niv3/col_super_gnx_niv_niv3_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/niv_niv3/col_super_gnx_niv_niv3_20180607_03.dat.gz"
  etag   = filemd5("../data/niv_niv3/col_super_gnx_niv_niv3_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "niv_niv4" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "niv_niv4/col_super_gnx_niv_niv4_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/niv_niv4/col_super_gnx_niv_niv4_20180607_03.dat.gz"
  etag   = filemd5("../data/niv_niv4/col_super_gnx_niv_niv4_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "tienda" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "tienda/col_super_gnx_centro_centro_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/tienda/col_super_gnx_centro_centro_20180607_03.dat.gz"
  etag   = filemd5("../data/tienda/col_super_gnx_centro_centro_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "centro_centro" {
  bucket = aws_s3_bucket.lake_raw.id
  key    = "centro_centro/col_super_gnx_centro_centro_20180607_03.dat.gz"
  acl    = "private"
  source = "../data/centro_centro/col_super_gnx_centro_centro_20180607_03.dat.gz"
  etag   = filemd5("../data/centro_centro/col_super_gnx_centro_centro_20180607_03.dat.gz")
}

resource "aws_s3_bucket_object" "parameters_centro" {
  bucket = aws_s3_bucket.lake_code.id
  key    = "parameters/parameters_centro.json"
  acl    = "private"
  source = "../code/parameters/parameters_centro.json"
  etag   = filemd5("../code/parameters/parameters_centro.json")
}

resource "aws_s3_bucket_object" "parameters_niv" {
  bucket = aws_s3_bucket.lake_code.id
  key    = "parameters/parameters_niv.json"
  acl    = "private"
  source = "../code/parameters/parameters_niv.json"
  etag   = filemd5("../code/parameters/parameters_niv.json")
}

resource "aws_s3_bucket_object" "parameters_vta" {
  bucket = aws_s3_bucket.lake_code.id
  key    = "parameters/parameters_vta.json"
  acl    = "private"
  source = "../code/parameters/parameters_vta.json"
  etag   = filemd5("../code/parameters/parameters_vta.json")
}