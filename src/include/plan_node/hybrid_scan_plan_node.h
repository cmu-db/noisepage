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
 protected:
  /**
   * Builder for a hybrid scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param oid oid for index to use for scan
     * @return
     */
    Builder &SetIndexOID(catalog::index_oid_t oid) {
      index_oid_ = oid;
      return *this;
    }

    /**
     * @param type hybrid scan type to use
     * @return builder object
     */
    Builder &SetHybridScanType(HybridScanType type) {
      hybrid_scan_type_ = type;
      return *this;
    }

    /**
     * Build the Hybrid scan plan node
     * @return plan node
     */
    std::shared_ptr<HybridScanPlanNode> Build() {
      return std::shared_ptr<HybridScanPlanNode>(
          new HybridScanPlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_,
                                 std::move(predicate_), is_for_update_, is_parallel_, index_oid_, hybrid_scan_type_));
    }

   protected:
    catalog::index_oid_t index_oid_;
    HybridScanType hybrid_scan_type_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param predicate predicate used for performing scan
   * @param is_for_update scan is used for an update
   * @param parallel parallel scan flag
   * @param index_oid OID of index to be used in hybrid scan
   * @param hybrid_scan_type hybrid scan type to be used
   */
  HybridScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                     std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                     std::unique_ptr<const parser::AbstractExpression> &&predicate, bool is_for_update,
                     bool is_parallel, catalog::index_oid_t index_oid, HybridScanType hybrid_scan_type)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), estimated_cardinality, std::move(predicate),
                             is_for_update, is_parallel),
        index_oid_(index_oid),
        hybrid_scan_type_(hybrid_scan_type) {}

 public:
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
