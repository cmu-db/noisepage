#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Base class for sql scans
 */
class AbstractScanPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Base builder class for scan plan nodes
   * @tparam ConcreteType
   */
  template <class ConcreteType>
  class Builder : public AbstractPlanNode::Builder<ConcreteType> {
   public:
    /**
     * @param predicate predicate to use for scan
     * @return builder object
     */
    ConcreteType &SetPredicate(std::unique_ptr<const parser::AbstractExpression> &&predicate) {
      predicate_ = std::move(predicate);
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param flag is for update flag
     * @return builder object
     */
    ConcreteType &SetIsForUpdateFlag(bool flag) {
      is_for_update_ = flag;
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param flag is parallel scan flag
     * @return builder object
     */
    ConcreteType &SetIsParallelFlag(bool flag) {
      is_parallel_ = flag;
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    /**
     * Scan predicate
     */
    std::unique_ptr<const parser::AbstractExpression> predicate_;
    /**
     * Is scan for update
     */
    bool is_for_update_ = false;
    /**
     * Is this a parallel scan
     */
    bool is_parallel_ = false;
  };

  /**
   * Base constructor for scans. Derived scan plans should call this constructor
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param predicate predicate used for performing scan
   * @param is_for_update scan is used for an update
   * @param is_parallel parallel scan flag
   */
  AbstractScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                       std::unique_ptr<const parser::AbstractExpression> &&predicate, bool is_for_update,
                       bool is_parallel)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        predicate_(std::move(predicate)),
        is_for_update_(is_for_update),
        is_parallel_(is_parallel) {}

 public:
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
  bool IsParallel() const { return is_parallel_; }

 private:
  /**
   * Selection predicate. We remove const to make it used when deserialization
   */
  std::unique_ptr<const parser::AbstractExpression> predicate_;

  /**
   * Are the tuples produced by this plan intended for update?
   */
  bool is_for_update_ = false;

  /**
   * Should this scan be performed in parallel?
   */
  bool is_parallel_;

 public:
  /**
   * Dont allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode);
};

}  // namespace terrier::plan_node
