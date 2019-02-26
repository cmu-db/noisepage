#pragma once

#include "../catalog/catalog_defs.h"
#include "../catalog/schema.h"
#include "abstract_scan_plan_node.h"
#include "plan_node_defs.h"

namespace terrier::plan_node {

class HybridScanPlanNode : public AbstractScanPlanNode {
 public:
  HybridScanPlanNode(catalog::Schema output_schema, catalog::index_oid_t index_oid,
                     parser::AbstractExpression *predicate, HybridScanType hybrid_scan_type)
      : AbstractScanPlanNode(output_schema, predicate), index_oid_(index_oid), hybrid_scan_type_(hybrid_scan_type) {}

  ~HybridScanPlanNode() {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HYBRIDSCAN; }

  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  HybridScanType GetHybridScanType() const { return hybrid_scan_type_; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    HybridScanPlanNode *new_plan =
        new HybridScanPlanNode(GetOutputSchema(), GetIndexOid(), GetPredicate()->Copy().get(), GetHybridScanType());
    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  catalog::index_oid_t index_oid_;

  HybridScanType hybrid_scan_type_;

 private:
  DISALLOW_COPY_AND_MOVE(HybridScanPlanNode);
};

}  // namespace terrier::plan_node
