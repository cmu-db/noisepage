#include "network/postgres/statement.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "execution/executable_query.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop_util.h"
#include "type/type_id.h"

namespace terrier::network {

Statement::Statement(std::string &&query_text, std::unique_ptr<parser::ParseResult> &&parse_result)
    : Statement(std::move(query_text), std::move(parse_result), {}) {}

Statement::Statement(std::string &&query_text, std::unique_ptr<parser::ParseResult> &&parse_result,
                     std::vector<type::TypeId> &&param_types)
    : parse_result_(std::move(parse_result)), param_types_(std::move(param_types)) {
  if (Valid()) {
    TERRIER_ASSERT(parse_result_->GetStatements().size() <= 1, "We currently expect one statement per string.");
    if (!Empty()) {
      root_statement_ = parse_result_->GetStatement(0);
      type_ = trafficcop::TrafficCopUtil::QueryTypeForStatement(root_statement_);
    }
  }
}

bool Statement::Valid() const { return parse_result_ != nullptr; }

bool Statement::Empty() const {
  TERRIER_ASSERT(Valid(), "Attempting to check emptiness without a valid parsed result.");
  return parse_result_->Empty();
}

common::ManagedPointer<parser::ParseResult> Statement::ParseResult() const {
  TERRIER_ASSERT(Valid(), "Attempting to get parse results without a valid parsed result.");
  return common::ManagedPointer(parse_result_);
}

common::ManagedPointer<parser::SQLStatement> Statement::RootStatement() const {
  TERRIER_ASSERT(Valid(), "Attempting to get root statement without a valid parsed result.");
  return common::ManagedPointer(root_statement_);
}

const std::vector<type::TypeId> &Statement::ParamTypes() const { return param_types_; }

QueryType Statement::GetQueryType() const { return type_; }

const std::string &Statement::GetQueryText() const { return query_text_; }

common::ManagedPointer<planner::AbstractPlanNode> Statement::PhysicalPlan() const {
  return common::ManagedPointer(physical_plan_);
}

common::ManagedPointer<execution::ExecutableQuery> Statement::GetExecutableQuery() const {
  return common::ManagedPointer(executable_query_);
}

void Statement::SetPhysicalPlan(std::unique_ptr<planner::AbstractPlanNode> &&physical_plan) {
  physical_plan_ = std::move(physical_plan);
}

void Statement::SetExecutableQuery(std::unique_ptr<execution::ExecutableQuery> &&executable_query) {
  executable_query_ = std::move(executable_query);
}

}  // namespace terrier::network
