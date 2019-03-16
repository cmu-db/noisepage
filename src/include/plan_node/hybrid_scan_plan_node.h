#pragma once

#include <memory>
#include <string>
#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "plan_node/abstract_scan_plan_node.h"
#include "plan_node/plan_node_defs.h"

namespace terrier::plan_node {

class HybridScanPlanNode : public AbstractScanPlanNode {
 public:
  HybridScanPlanNode(std::shared_ptr<OutputSchema> output_schema, catalog::index_oid_t index_oid,
                     parser::AbstractExpression *predicate, HybridScanType hybrid_scan_type)
      : AbstractScanPlanNode(std::move(output_schema), predicate),
        index_oid_(index_oid),
        hybrid_scan_type_(hybrid_scan_type) {}

  ~HybridScanPlanNode() override;

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HYBRIDSCAN; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "HybridScanPlanNode"; }

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

  DISALLOW_COPY_AND_MOVE(HybridScanPlanNode);
};

}  // namespace terrier::plan_node
