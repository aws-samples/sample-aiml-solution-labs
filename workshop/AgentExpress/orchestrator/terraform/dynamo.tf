# Progress store the runtime writes to and the UI reads from.

resource "aws_dynamodb_table" "status" {
  count        = local.workflow_plane ? 1 : 0
  name         = "${var.agent_name}_status"
  billing_mode = "PAY_PER_REQUEST"
  point_in_time_recovery {
    enabled = true
  }
  hash_key = "session_id"

  attribute {
    name = "session_id"
    type = "S"
  }
  # Rows carry `ttl` only when RUN_TTL_DAYS is set (app/common/sink.py).
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

resource "aws_dynamodb_table" "events" {
  count        = local.workflow_plane ? 1 : 0
  name         = "${var.agent_name}_events"
  billing_mode = "PAY_PER_REQUEST"
  point_in_time_recovery {
    enabled = true
  }
  hash_key  = "session_id"
  range_key = "ts"

  attribute {
    name = "session_id"
    type = "S"
  }
  attribute {
    name = "ts"
    type = "S"
  }
  # Rows carry `ttl` only when RUN_TTL_DAYS is set (app/common/sink.py).
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}
