#pragma once

#include <memory>
#include <utility>
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {
/**
 * Base class for sql scans
 */
class AbstractScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Base constructor for scans. Derived scan plans should call this constructor
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param is_for_update scan is used for an update
   * @param parallel parallel scan flag
   */
  AbstractScanPlanNode(std::shared_ptr<OutputSchema> output_schema, parser::AbstractExpression *predicate,
                       bool is_for_update = false, bool parallel = false)
      : AbstractPlanNode(std::move(output_schema)),
        predicate_(predicate),
        is_for_update_(is_for_update),
        parallel_(parallel) {}

  /**
   * @return predicate used for performing scan
   */
  const parser::AbstractExpression *GetPredicate() const { return predicate_.get(); }

  /**
   * @return for update flag
   */
  bool IsForUpdate() const { return is_for_update_; }

  /**
   * @return parallel scan flag
   */
  bool IsParallel() const { return parallel_; }

 private:
  /**
   * Selection predicate. We remove const to make it used when deserialization
   */
  std::unique_ptr<parser::AbstractExpression> predicate_;

  /**
   * Are the tuples produced by this plan intended for update?
   */
  bool is_for_update_ = false;

  /**
   * Should this scan be performed in parallel?
   */
  bool parallel_;

 public:
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode);
};

}  // namespace terrier::plan_node
