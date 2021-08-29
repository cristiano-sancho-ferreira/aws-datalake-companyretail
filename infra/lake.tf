
resource "aws_lakeformation_permissions" "glue_lake_role1" {
  principal                     = aws_iam_role.glue_role.arn
  permissions                   = ["ALL", "ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]
  permissions_with_grant_option = ["ALL", "ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]

  database {
    name = var.glue_database_raw
  }
}

resource "aws_lakeformation_permissions" "glue_lake_role3" {
  principal                     = aws_iam_role.glue_role.arn
  permissions                   = ["ALL"]
  permissions_with_grant_option = ["ALL"]

  table {
    database_name = var.glue_database_raw
    wildcard      = true
  }
}


resource "aws_lakeformation_permissions" "glue_lake_role2" {
  principal                     = aws_iam_role.glue_role.arn
  permissions                   = ["ALL", "ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]
  permissions_with_grant_option = ["ALL", "ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]


  database {
    name = var.glue_database_analytics
  }
}