# =========================================================
# Cloud First
# =========================================================

variable "environment" {
    type        = string
    description = "Environment name (prod, test, dev, or poc)."
    default     = ""

    validation {
        condition     = var.environment == "" || contains(["prod", "production", "test", "dev", "development", "devtest", "poc"], lower(var.environment))
        error_message = "Value must be one of: prod, production, test, dev, development, devtest, or poc."
    }
}

variable "project" {
    type        = string
    description = "Project name within the service. This is used as part of resource names, so must be a simple alpha-numeric string."

    validation {
        condition     = can(regex("^[a-zA-Z][a-zA-Z0-9-]+[a-zA-Z0-9]$", var.project))
        error_message = "Must start with a letter (a-z), end with a letter or number, and contain only letters, numbers, and dashes."
    }
}

# =========================================================
# Lambda
# =========================================================

variable "deploy_localzip" {
    type        = string
    description = "Path to the zip file to deploy."
    default     = null
}

variable "deploy_s3zip" {
    type        = object({
                    bucket = string
                    prefix = string
                })
    description = "S3 bucket and prefix to the dynamodbStreamEvents/environment.zip file to deploy."
    default     = null
}

variable "dynamodb_table" {
    type        = string
    description = "Name of the DynamoDB table to monitor. Streams must be enabled."
}

variable "event_detailtype_fmt" {
    type        = string
    description = "Python style format() string that controls how the event DetailType field is generated."
    default     = ""
}

variable "event_bus_name" {
    type        = string
    description = "Name of the EventBridge Bus to put events to."
    default     = "default"
}

variable "cloudwatch_logs_kms_key_id" {
    type        = string
    description = "The ARN of the KMS Key to use when encrypting log data."
    default     = null
}
