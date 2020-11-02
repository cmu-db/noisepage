#include "optimizer/group.h"

#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

Group::~Group() {
  for (auto expr : logical_expressions_) {
    delete expr;
  }
  for (auto expr : physical_expressions_) {
    delete expr;
  }
  for (auto expr : enforced_exprs_) {
    delete expr;
  }
  for (auto it : lowest_cost_expressions_) {
    delete it.first;
  }
}

void Group::EraseLogicalExpression() {
  NOISEPAGE_ASSERT(logical_expressions_.size() == 1, "There should exist only 1 logical expression");
  NOISEPAGE_ASSERT(physical_expressions_.empty(), "No physical expressions should be present");
  delete logical_expressions_[0];
  logical_expressions_.clear();
}

void Group::AddExpression(GroupExpression *expr, bool enforced) {
  // Do duplicate detection
  expr->SetGroupID(id_);
  if (enforced) {
    enforced_exprs_.push_back(expr);
  } else if (expr->Contents()->IsPhysical()) {
    physical_expressions_.push_back(expr);
  } else {
    logical_expressions_.push_back(expr);
  }
}

bool Group::SetExpressionCost(GroupExpression *expr, double cost, PropertySet *properties) {
  OPTIMIZER_LOG_TRACE("Adding expression cost on group " + std::to_string(expr->GetGroupID().UnderlyingValue()) +
                      " with op {1}" + expr->Contents()->GetName());

  auto it = lowest_cost_expressions_.find(properties);
  if (it == lowest_cost_expressions_.end()) {
    // not exist so insert
    lowest_cost_expressions_[properties] = std::make_tuple(cost, expr);
    return true;
  }

  if (std::get<0>(it->second) > cost) {
    // this is lower cost
    lowest_cost_expressions_[properties] = std::make_tuple(cost, expr);
    delete properties;
    return true;
  }

  delete properties;
  return false;
}

GroupExpression *Group::GetBestExpression(PropertySet *properties) {
  auto it = lowest_cost_expressions_.find(properties);
  if (it != lowest_cost_expressions_.end()) {
    return std::get<1>(it->second);
  }

  return nullptr;
}

bool Group::HasExpressions(PropertySet *properties) const {
  const auto &it = lowest_cost_expressions_.find(properties);
  return (it != lowest_cost_expressions_.end());
}

}  // namespace noisepage::optimizer
