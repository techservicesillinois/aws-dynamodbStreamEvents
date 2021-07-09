# =========================================================
# Data
# =========================================================

data "aws_iam_policy_document" "this" {
    statement {
        effect    = "Allow"
        actions   = [ "dynamodb:ListStreams" ]
        resources = [ "*" ]
    }

    statement {
        effect    = "Allow"
        actions   = [
            "dynamodb:DescribeStream",
            "dynamodb:GetRecords",
            "dynamodb:GetShardIterator",
        ]
        resources = [ var.dynamodb_stream_arn ]
    }

    statement {
        effect    = "Allow"
        actions   = [ "events:PutEvents" ]
        resources = [
            "arn:aws:events:${local.region_name}:${local.account_id}:event-bus/${var.event_bus_name}"
        ]
        condition {
            test     = "StringEquals"
            variable = "events:source"

            values = [ "dynamodb-streams.aws.illinois.edu" ]
        }
    }
}

# =========================================================
# Modules
# =========================================================

module "this" {
    source = "terraform-aws-modules/lambda/aws"

    function_name = "${local.name_prefix}dynamodbStreamEvents"
    description   = "Send DynamoDB Stream Records to EventBridge"
    handler       = "dynamodb_stream_events.handler"
    runtime       = "python3.8"
    timeout       = 10

    environment_variables = {
        EVENT_BUS_NAME       = var.event_bus_name
        EVENT_DETAILTYPE_FMT = var.event_detailtype_fmt
        LOGGING_LEVEL        = contains(["prod", "production"], lower(var.environment)) ? "INFO" : "DEBUG"
    }
    cloudwatch_logs_kms_key_id        = var.cloudwatch_logs_kms_key_id
    cloudwatch_logs_retention_in_days = 7

    create_package         = false
    local_existing_package = "${path.module}/../dist/dynamodbStreamEvents.zip"

    event_source_mapping = {
        dynamodb = {
            event_source_arn  = var.dynamodb_stream_arn
            starting_position = "LATEST"
        }
    }

    create_current_version_allowed_triggers = false
    allowed_triggers = {
        dynamodb = {
            principal  = "dynamodb.amazonaws.com"
            source_arn = var.dynamodb_stream_arn
        }
    }

    attach_policy_json = true
    policy_json        = data.aws_iam_policy_document.this.json
}