# =========================================================
# Data
# =========================================================

data "aws_s3_object" "this" {
    count = var.deploy_s3zip == null ?  0 : 1

    bucket = var.deploy_s3zip.bucket
    key    = "${var.deploy_s3zip.prefix}dynamodbStreamEvents/${var.environment}.zip"
}

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
        resources = [ var.dynamodb_table.stream_arn ]
    }

    dynamic "statement" {
        for_each = var.dynamodb_table.kms_key_arn == null ? [] : [ var.dynamodb_table.kms_key_arn ]
        content {
            effect  = "Allow"
            actions = [
                "kms:GenerateDataKey",
                "kms:Decrypt",
                "kms:Encrypt",
            ]
            resources = [ statement.value ]

            condition {
                test     = "StringEquals"
                variable = "kms:EncryptionContext:aws:dynamodb:tableName"

                values = [ var.dynamodb_table.name ]
            }

            condition {
                test     = "StringEquals"
                variable = "kms:EncryptionContext:aws:dynamodb:subscriberId"

                values = [ local.account_id ]
            }
        }
    }

    statement {
        effect    = "Allow"
        actions   = [ "events:PutEvents" ]
        resources = [
            "arn:${local.partition}:events:${local.region_name}:${local.account_id}:event-bus/${var.event_bus_name}"
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
    version = "7.4.0"

    function_name = "${local.name_prefix}dynamodbStreamEvents"
    description   = "Send DynamoDB Stream Records to EventBridge"
    handler       = "dynamodb_stream_events.handler"
    runtime       = "python3.8"
    timeout       = 10
    function_tags = var.function_tags

    environment_variables = {
        EVENT_BUS_NAME       = var.event_bus_name
        EVENT_DETAILTYPE_FMT = var.event_detailtype_fmt
        LOGGING_LEVEL        = local.is_debug ? "DEBUG" : "INFO"
    }
    cloudwatch_logs_kms_key_id        = var.cloudwatch_logs_kms_key_id
    cloudwatch_logs_retention_in_days = local.is_debug ? 7 : 30

    create_package         = false
    local_existing_package = var.deploy_s3zip == null ? coalesce(var.deploy_localzip, "${path.module}/../dist/dynamodbStreamEvents.zip") : null
    s3_existing_package    = var.deploy_s3zip == null ? null : {
        bucket     = data.aws_s3_object.this[0].bucket
        key        = data.aws_s3_object.this[0].key
        version_id = data.aws_s3_object.this[0].version_id
    }

    event_source_mapping = {
        dynamodb = {
            event_source_arn  = var.dynamodb_table.stream_arn
            starting_position = "LATEST"
        }
    }

    create_current_version_async_event_config   = false
    create_current_version_allowed_triggers     = false
    create_unqualified_alias_allowed_triggers   = true
    create_unqualified_alias_async_event_config = true

    allowed_triggers = {
        dynamodb = {
            principal  = "dynamodb.amazonaws.com"
            source_arn = var.dynamodb_table.stream_arn
        }
    }

    attach_policy_json = true
    policy_json        = data.aws_iam_policy_document.this.json
}
