#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/plan_node_defs.h"

namespace terrier::plan_node {

/**
 * Plan node for order by operator
 */
class OrderByPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for order by plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param key column id for key to sort
     * @param ordering ordering (ASC or DESC) for key
     * @return builder object
     */
    Builder &AddSortKey(catalog::col_oid_t key, OrderByOrdering ordering) {
      sort_keys_.push_back(key);
      sort_key_orderings_.push_back(ordering);
      return *this;
    }

    /**
     * @param limit number of tuples to limit to
     * @return builder object
     */
    Builder &SetLimit(size_t limit) {
      limit_ = limit;
      has_limit_ = true;
      return *this;
    }

    /**
     * @param offset offset for where to limit from
     * @return builder object
     */
    Builder &SetOffset(size_t offset) {
      offset_ = offset;
      return *this;
    }

    /**
     * Build the order by plan node
     * @return plan node
     */
    std::shared_ptr<OrderByPlanNode> Build() {
      return std::shared_ptr<OrderByPlanNode>(
          new OrderByPlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_,
                              std::move(sort_keys_), std::move(sort_key_orderings_), has_limit_, limit_, offset_));
    }

   protected:
    std::vector<catalog::col_oid_t> sort_keys_;
    std::vector<OrderByOrdering> sort_key_orderings_;
    bool has_limit_ = false;
    size_t limit_ = 0;
    size_t offset_ = 0;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param sort_keys keys on which to sort on
   * @param sort_key_orderings orderings for each sort key (ASC or DESC). Same size as sort_keys
   * @param limit number of tuples to limit output to
   * @param offset offset in sort from where to limit from
   */
  OrderByPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                  std::vector<catalog::col_oid_t> sort_keys, std::vector<OrderByOrdering> sort_key_orderings,
                  bool has_limit, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        sort_keys_(std::move(sort_keys)),
        sort_key_orderings_(std::move(sort_key_orderings)),
        has_limit_(has_limit),
        limit_(limit),
        offset_(offset) {}

 public:
  /**
   * @return vector of col_oid_t of keys to sort on
   */
  const std::vector<catalog::col_oid_t> &GetSortKeys() const { return sort_keys_; }

  /**
   * @return vector of type of ordering for each sort key (ASC or DESC)
   */
  const std::vector<OrderByOrdering> &GetSortKeyOrderings() const { return sort_key_orderings_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ORDERBY; }

  /**
   * @return true if sort has a defined limit
   */
  bool HasLimit() const { return has_limit_; }

  /**
   * Should only be used if sort has limit
   * @return limit for sort
   */
  size_t GetLimit() const {
    TERRIER_ASSERT(HasLimit(), "OrderBy plan has no limit");
    return limit_;
  }

  /**
   * Should only be used if sort has limit
   * @return offset for sort
   */
  size_t GetOffset() const {
    TERRIER_ASSERT(HasLimit(), "OrderBy plan has no limit");
    return offset_;
  }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  /* Column Ids used (in order) to sort input tuples */
  const std::vector<catalog::col_oid_t> sort_keys_;

  /* Sort order flag. descend_flags_[i] */
  const std::vector<OrderByOrdering> sort_key_orderings_;

  /* Whether there is limit clause */
  bool has_limit_;

  /* How many tuples to return */
  size_t limit_;

  /* How many tuples to skip first */
  size_t offset_;

 public:
  DISALLOW_COPY_AND_MOVE(OrderByPlanNode);
};

}  // namespace terrier::plan_node
