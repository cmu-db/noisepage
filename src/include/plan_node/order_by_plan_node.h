#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/plan_node_defs.h"

namespace terrier::plan_node {

class OrderByPlanNode : public AbstractPlanNode {
 public:
  // Constructor for SORT without LIMIT
  OrderByPlanNode(std::shared_ptr<OutputSchema> output_schema, std::vector<catalog::col_oid_t> sort_keys,
                  std::vector<OrderByOrdering> sort_key_orderings)
      : AbstractPlanNode(std::move(output_schema)),
        sort_keys_(std::move(sort_keys)),
        sort_key_orderings_(std::move(sort_key_orderings)),
        has_limit_(false),
        limit_(0),
        offset_(0) {}

  // Constructor for SORT with LIMIT
  OrderByPlanNode(std::shared_ptr<OutputSchema> output_schema, std::vector<catalog::col_oid_t> sort_keys,
                  std::vector<OrderByOrdering> sort_key_orderings, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(output_schema)),
        sort_keys_(std::move(sort_keys)),
        sort_key_orderings_(std::move(sort_key_orderings)),
        has_limit_(true),
        limit_(limit),
        offset_(offset) {}

  const std::vector<catalog::col_oid_t> &GetSortKeys() const { return sort_keys_; }

  const std::vector<OrderByOrdering> &GetSortKeyOrderings() const { return sort_key_orderings_; }

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ORDERBY; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "OrderByPlanNode"; }

  bool HasLimit() const { return has_limit_; }

  size_t GetLimit() const {
    TERRIER_ASSERT(HasLimit(), "OrderBy plan has no limit");
    return limit_;
  }

  size_t GetOffset() const {
    TERRIER_ASSERT(HasLimit(), "OrderBy plan has no limit");
    return offset_;
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  std::unique_ptr<AbstractPlanNode> Copy() const override;

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

 private:
  DISALLOW_COPY_AND_MOVE(OrderByPlanNode);
};

}  // namespace terrier::plan_node
