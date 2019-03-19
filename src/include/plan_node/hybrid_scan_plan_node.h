#pragma once

#include <memory>
#include <string>
#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "plan_node/abstract_scan_plan_node.h"
#include "plan_node/plan_node_defs.h"

namespace terrier::plan_node {

/**
 * Plan node for a hybryd scan
 */
class HybridScanPlanNode : public AbstractScanPlanNode {
 public:
  /**
   * @param output_schema Schema representing the structure of the output of this plan nod
   * @param index_oid OID of index to be used in hybrid scan
   * @param predicate scan predicate
   * @param hybrid_scan_type hybrid scan type to be used
   */
  HybridScanPlanNode(std::shared_ptr<OutputSchema> output_schema, catalog::index_oid_t index_oid,
                     parser::AbstractExpression *predicate, HybridScanType hybrid_scan_type)
      : AbstractScanPlanNode(std::move(output_schema), predicate),
        index_oid_(index_oid),
        hybrid_scan_type_(hybrid_scan_type) {}

  ~HybridScanPlanNode() override;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HYBRIDSCAN; }

  /**
   * @return index OID to be used for scan
   */
  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  /**
   * @return hybrid scan type to be used
   */
  HybridScanType GetHybridScanType() const { return hybrid_scan_type_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  catalog::index_oid_t index_oid_;
  HybridScanType hybrid_scan_type_;

 public:
  DISALLOW_COPY_AND_MOVE(HybridScanPlanNode);
};

}  // namespace terrier::plan_node
