# =========================================================
# Basic
# =========================================================

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_dynamodb_table" "this" {
    name = var.dynamodb_table
}

# =========================================================
# Locals
# =========================================================

locals {
    region_name = data.aws_region.current.name
    account_id  = data.aws_caller_identity.current.account_id

    name_prefix = "${var.project}-"

    is_debug = !contains(["prod", "production"], lower(var.environment))

    dynamodb_stream_arn = data.aws_dynamodb_table.this.stream_arn
}
