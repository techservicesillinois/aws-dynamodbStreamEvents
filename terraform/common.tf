# =========================================================
# Basic
# =========================================================

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

# =========================================================
# Locals
# =========================================================

locals {
    region_name = data.aws_region.current.name
    account_id  = data.aws_caller_identity.current.account_id

    name_prefix = "${var.project}-"
}
