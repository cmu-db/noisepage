#pragma once

#include "common/managed_pointer.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "type/transient_value.h"

namespace terrier::network {

class Portal {
 public:
  Portal(const common::ManagedPointer<Statement> statement, std::unique_ptr<planner::AbstractPlanNode> &&physical_plan,
         std::vector<type::TransientValue> &&params, std::vector<FieldFormat> &&result_formats)
      : statement_(statement),
        physical_plan_(std::move(physical_plan)),
        params_(std::move(params)),
        result_formats_(std::move(result_formats)) {}

  common::ManagedPointer<Statement> Statement() const { return statement_; }

  const std::vector<FieldFormat> &ResultFormats() const { return result_formats_; }
  common::ManagedPointer<const std::vector<type::TransientValue>> Parameters() {
    return common::ManagedPointer(&params_);
  }

 private:
  const common::ManagedPointer<network::Statement> statement_;
  std::unique_ptr<planner::AbstractPlanNode> physical_plan_;
  const std::vector<type::TransientValue> params_;
  const std::vector<FieldFormat> result_formats_;
};

}  // namespace terrier::network