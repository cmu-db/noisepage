#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/statement.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::planner {
class AbstractPlanNode;
}  // namespace noisepage::planner

namespace noisepage::network {

/**
 * Portal is a postgres concept (see the Extended Query documentation:
 * https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
 * It encapsulates a reference to its originating statement, the parameters (if any), the output formats, and the
 * optimized physical plan. It represents a query ready to be executed.
 */
class Portal {
 public:
  /**
   * Constructor that doesnt have params or result_formats, i.e. Simple Query protocol. We default the output format to
   * text for Simple Query protocol per the spec.
   * @param statement statement that this Portal refers to
   */
  explicit Portal(const common::ManagedPointer<Statement> statement) : Portal(statement, {}, {FieldFormat::text}) {}

  /**
   * Constructor that doesnt have params or result_formats, i.e. Extended Query protocol
   * @param statement statement that this Portal refers to
   * @param params params for this query
   * @param result_formats output formats for this query
   */
  Portal(const common::ManagedPointer<Statement> statement, std::vector<parser::ConstantValueExpression> &&params,
         std::vector<FieldFormat> &&result_formats)
      : statement_(statement), params_(std::move(params)), result_formats_(std::move(result_formats)) {}

  /**
   * @return Statement that this Portal references
   */
  common::ManagedPointer<Statement> GetStatement() const { return statement_; }

  /**
   * @return the optimized physical plan for this query
   */
  common::ManagedPointer<planner::AbstractPlanNode> PhysicalPlan() const { return statement_->PhysicalPlan(); }

  /**
   * @return output formats for this query
   */
  const std::vector<FieldFormat> &ResultFormats() const { return result_formats_; }

  /**
   * @return params for this query
   */
  common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> Parameters() {
    return common::ManagedPointer(&params_);
  }

 private:
  const common::ManagedPointer<network::Statement> statement_;
  const std::vector<parser::ConstantValueExpression> params_;
  const std::vector<FieldFormat> result_formats_;
};

}  // namespace noisepage::network
