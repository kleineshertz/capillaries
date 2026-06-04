resource "aws_budgets_budget" "monthly_cost" {
  name        = var.budget_name
  budget_type = "COST"

  limit_amount = var.budget_amount
  limit_unit   = var.budget_unit

  time_unit = "MONTHLY"

  dynamic "notification" {
    for_each = var.budget_email != "" ? [1] : []
    content {
      comparison_operator        = "GREATER_THAN"
      notification_type          = "ACTUAL"
      threshold                  = var.budget_threshold_percent
      threshold_type             = "PERCENTAGE"
      subscriber_email_addresses = [var.budget_email]
    }
  }
}
