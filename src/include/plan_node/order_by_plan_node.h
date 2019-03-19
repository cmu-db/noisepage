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
 public:
  /**
   * Constructor for SORT without LIMIT
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param sort_keys col_oid_t for keys on which to sort on
   * @param sort_key_orderings orderings for each sort key (ASC or DESC). Same size as sort_keys
   */
  OrderByPlanNode(std::shared_ptr<OutputSchema> output_schema, std::vector<catalog::col_oid_t> sort_keys,
                  std::vector<OrderByOrdering> sort_key_orderings)
      : AbstractPlanNode(std::move(output_schema)),
        sort_keys_(std::move(sort_keys)),
        sort_key_orderings_(std::move(sort_key_orderings)),
        has_limit_(false),
        limit_(0),
        offset_(0) {}

  /**
   * Constructor for SORT with LIMIT
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param sort_keys keys on which to sort on
   * @param sort_key_orderings orderings for each sort key (ASC or DESC). Same size as sort_keys
   * @param limit number of tuples to limit output to
   * @param offset offset in sort from where to limit from
   */
  OrderByPlanNode(std::shared_ptr<OutputSchema> output_schema, std::vector<catalog::col_oid_t> sort_keys,
                  std::vector<OrderByOrdering> sort_key_orderings, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(output_schema)),
        sort_keys_(std::move(sort_keys)),
        sort_key_orderings_(std::move(sort_key_orderings)),
        has_limit_(true),
        limit_(limit),
        offset_(offset) {}

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
