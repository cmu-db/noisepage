#pragma once

#include "common/managed_pointer.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "type/transient_value.h"

namespace terrier::network {

class Portal {
 public:
  Portal(std::unique_ptr<planner::AbstractPlanNode> &&physical_plan, const common::ManagedPointer<Statement> statement,
         std::vector<FieldFormat> &&result_formats, std::vector<type::TransientValue> &&parameters)
      : physical_plan_(std::move(physical_plan)),
        statement_(statement),
        result_formats_(std::move(result_formats)),
        parameters_(std::move(parameters)) {}

  common::ManagedPointer<planner::AbstractPlanNode> PhysicalPlan() const {
    return common::ManagedPointer(physical_plan_);
  }
  common::ManagedPointer<Statement> Statement() const { return statement_; }

  const std::vector<FieldFormat> &ResultFormats() const { return result_formats_; }
  common::ManagedPointer<const std::vector<type::TransientValue>> Parameters() {
    return common::ManagedPointer(&parameters_);
  }

 private:
  const std::unique_ptr<planner::AbstractPlanNode> physical_plan_;
  const common::ManagedPointer<network::Statement> statement_;
  const std::vector<FieldFormat> result_formats_;
  const std::vector<type::TransientValue> parameters_;
};

}  // namespace terrier::network