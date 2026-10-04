output "ui_url" {
  description = "CloudFront URL for the web UI."
  value       = "https://${aws_cloudfront_distribution.ui.domain_name}"
}

output "api_endpoint" {
  description = "API Gateway endpoint (also reachable via CloudFront /api/*)."
  value       = aws_apigatewayv2_api.bff.api_endpoint
}

output "agent_runtime_arn" {
  value = local.workflow_plane ? awscc_bedrockagentcore_runtime.orchestrator[0].agent_runtime_arn : ""
}

output "agent_runtime_id" {
  value = local.workflow_plane ? awscc_bedrockagentcore_runtime.orchestrator[0].agent_runtime_id : ""
}

output "agent_runtime_status" {
  value = local.workflow_plane ? awscc_bedrockagentcore_runtime.orchestrator[0].status : ""
}

output "memory_id" {
  value = local.workflow_plane ? awscc_bedrockagentcore_memory.orchestrator[0].memory_id : ""
}

output "ecr_repository_url" {
  value = local.workflow_plane ? aws_ecr_repository.orchestrator[0].repository_url : ""
}

output "image_uri" {
  value = local.image_uri
}

output "gateway_url" {
  description = "AgentCore Gateway MCP endpoint (empty when the Gateway is disabled)."
  value       = local.gateway_enabled ? aws_bedrockagentcore_gateway.mcp[0].gateway_url : ""
}

output "gateway_id" {
  value = local.gateway_enabled ? aws_bedrockagentcore_gateway.mcp[0].gateway_id : ""
}
output "knowledge_base_id" {
  description = <<-EOT
    Bedrock Knowledge Base id (empty when no tool declares type="kb").

    Worth watching after a corpus change: the id CHANGES whenever an immutable
    property of the vector store changes, because the KB and its index are named
    from a digest of those properties (see kb.tf). A new id means a new, empty KB
    and a fresh ingest, so re-check the ingestion job.
  EOT
  value       = local.kb_enabled ? local.kb_id : ""
}

output "console_mode" {
  description = "\"builder\": a control-plane console, with no workflow plane of its own."
  value       = var.console_mode
}
