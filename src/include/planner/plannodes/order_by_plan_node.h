#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for order by operator
 */
class OrderByPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for order by plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param key column id for key to sort
     * @param ordering ordering (ASC or DESC) for key
     * @return builder object
     */
    Builder &AddSortKey(catalog::col_oid_t key, OrderByOrderingType ordering) {
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
    std::unique_ptr<OrderByPlanNode> Build() {
      return std::unique_ptr<OrderByPlanNode>(new OrderByPlanNode(std::move(children_), std::move(output_schema_),
                                                                  std::move(sort_keys_), std::move(sort_key_orderings_),
                                                                  has_limit_, limit_, offset_));
    }

   protected:
    /**
     * col_oid_t of keys to sort on
     */
    std::vector<catalog::col_oid_t> sort_keys_;
    /**
     * type of ordering for each sort key (ASC or DESC)
     */
    std::vector<OrderByOrderingType> sort_key_orderings_;
    /**
     * true if sort has a defined limit. False by default
     */
    bool has_limit_ = false;
    /**
     * limit for sort
     */
    size_t limit_ = 0;
    /**
     * offset for sort
     */
    size_t offset_ = 0;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param sort_keys keys on which to sort on
   * @param sort_key_orderings orderings for each sort key (ASC or DESC). Same size as sort_keys
   * @param has_limit true if operator should perform a limit
   * @param limit number of tuples to limit output to
   * @param offset offset in sort from where to limit from
   */
  OrderByPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::shared_ptr<OutputSchema> output_schema, std::vector<catalog::col_oid_t> sort_keys,
                  std::vector<OrderByOrderingType> sort_key_orderings, bool has_limit, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        sort_keys_(std::move(sort_keys)),
        sort_key_orderings_(std::move(sort_key_orderings)),
        has_limit_(has_limit),
        limit_(limit),
        offset_(offset) {}

 public:
  /**
   * @return col_oid_t of keys to sort on
   */
  const std::vector<catalog::col_oid_t> &GetSortKeys() const { return sort_keys_; }

  /**
   * @return type of ordering for each sort key (ASC or DESC)
   */
  const std::vector<OrderByOrderingType> &GetSortKeyOrderings() const { return sort_key_orderings_; }

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

 private:
  /* Column Ids used (in order) to sort input tuples */
  const std::vector<catalog::col_oid_t> sort_keys_;

  /* Sort order flag. descend_flags_[i] */
  const std::vector<OrderByOrderingType> sort_key_orderings_;

  /* Whether there is limit clause */
  bool has_limit_;

  /* How many tuples to return */
  size_t limit_;

  /* How many tuples to skip first */
  size_t offset_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(OrderByPlanNode);
};

}  // namespace terrier::planner
