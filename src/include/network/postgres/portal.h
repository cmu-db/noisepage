#pragma once

#include "common/managed_pointer.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "type/transient_value.h"

namespace terrier::network {

/**
 * Portal is a postgres concept (see the Extended Query documentation:
 * https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
 * It encapsulates a reference to its originating statement, the parameters (if any), the output formats, and the
 * optimized physical plan. It represents a query ready to be executed.
 */
class Portal {
 public:
  /**
   * Constructor that doesnt have params or result_formats, i.e. Simple Query protocol
   * @param statement statement that this Portal refers to
   * @param physical_plan optimized plan ready for execution
   */
  Portal(const common::ManagedPointer<Statement> statement, std::unique_ptr<planner::AbstractPlanNode> &&physical_plan)
      : Portal(statement, std::move(physical_plan), {}, {FieldFormat::text}) {}

  /**
   * Constructor that doesnt have params or result_formats, i.e. Extended Query protocol
   * @param statement statement that this Portal refers to
   * @param physical_plan optimized plan ready for execution
   * @param params params for this query
   * @param result_formats output formats for this query
   */
  Portal(const common::ManagedPointer<Statement> statement, std::unique_ptr<planner::AbstractPlanNode> &&physical_plan,
         std::vector<type::TransientValue> &&params, std::vector<FieldFormat> &&result_formats)
      : statement_(statement),
        physical_plan_(std::move(physical_plan)),
        params_(std::move(params)),
        result_formats_(std::move(result_formats)) {}

  common::ManagedPointer<Statement> Statement() const { return statement_; }

  common::ManagedPointer<planner::AbstractPlanNode> PhysicalPlan() const {
    return common::ManagedPointer(physical_plan_);
  }

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