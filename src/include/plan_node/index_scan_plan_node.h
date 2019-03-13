#pragma once

#include "abstract_scan_plan_node.h"
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"

// TODO(Gus,Wen): IndexScanDesc had a `p_runtime_key_list` that did not have a comment explaining its use. We should
// figure that out. IndexScanDesc also had an expression type list, i dont see why this can't just be taken from the
// predicate

// TODO(Gus,Wen): plan node contianed info on whether the scan was left or right open. This should be computed at
// exection time

namespace terrier::plan_node {

class IndexScanPlanNode : public AbstractScanPlanNode {
 public:
  IndexScanPlanNode(std::shared_ptr<OutputSchema> output_schema, catalog::index_oid_t index_oid,
                    parser::AbstractExpression *predicate)
      : AbstractScanPlanNode(output_schema, predicate), index_oid_(index_oid) {}

  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INDEXSCAN; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    IndexScanPlanNode *new_plan = new IndexScanPlanNode(GetOutputSchema(), GetIndexOid(), GetPredicate()->Copy().get());
    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // Index oid associated with index scan
  catalog::index_oid_t index_oid_;

 private:
  DISALLOW_COPY_AND_MOVE(IndexScanPlanNode);
};

}  // namespace terrier::plan_node
