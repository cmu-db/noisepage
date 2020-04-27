#include "network/postgres/portal.h"

namespace terrier::network {

Portal::Portal(const common::ManagedPointer<Statement> statement) : Portal(statement, {}, {FieldFormat::text}) {}

Portal::Portal(const common::ManagedPointer<Statement> statement, std::vector<type::TransientValue> &&params,
               std::vector<FieldFormat> &&result_formats)
    : statement_(statement), params_(std::move(params)), result_formats_(std::move(result_formats)) {}

common::ManagedPointer<Statement> Portal::GetStatement() const { return statement_; }

common::ManagedPointer<planner::AbstractPlanNode> Portal::PhysicalPlan() const { return statement_->PhysicalPlan(); }

const std::vector<FieldFormat> &Portal::ResultFormats() const { return result_formats_; }

common::ManagedPointer<const std::vector<type::TransientValue>> Portal::Parameters() {
  return common::ManagedPointer(&params_);
}

}  // namespace terrier::network
