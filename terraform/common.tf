# =========================================================
# Basic
# =========================================================

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

# =========================================================
# Locals
# =========================================================

locals {
    partition   = data.aws_partition.current.partition
    region_name = data.aws_region.current.name
    account_id  = data.aws_caller_identity.current.account_id

    name_prefix = "${var.project}-"

    is_debug = !contains(["prod", "production"], lower(var.environment))
}
