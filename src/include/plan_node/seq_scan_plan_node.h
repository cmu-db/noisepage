#pragma once

#include <memory>
#include <string>
#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/abstract_scan_plan_node.h"

namespace terrier::plan_node {

/**
 * Plan node for sequanial table scan
 */
class SeqScanPlanNode : public AbstractScanPlanNode {
 protected:
  /**
   * Builder for a sequential scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param oid oid for table to scan
     * @return builder object
     */
    Builder &SetTableOID(catalog::table_oid_t oid) {
      table_oid_ = oid;
      return *this;
    }

    /**
     * Build the sequential scan plan node
     * @return plan node
     */
    std::shared_ptr<SeqScanPlanNode> Build() {
      return std::shared_ptr<SeqScanPlanNode>(new SeqScanPlanNode(std::move(children_), std::move(output_schema_),
                                                                  estimated_cardinality_, std::move(predicate_),
                                                                  is_for_update_, is_parallel_, table_oid_));
    }

   protected:
    catalog::table_oid_t table_oid_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param predicate scan predicate
   * @param table_oid OID for table to scan
   * @param is_for_update flag for if scan is for an update
   * @param parallel flag for parallel scan
   */
  SeqScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                  std::unique_ptr<const parser::AbstractExpression> &&predicate, bool is_for_update, bool is_parallel,
                  catalog::table_oid_t table_oid)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), estimated_cardinality, std::move(predicate),
                             is_for_update, is_parallel),
        table_oid_(table_oid) {}

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  /**
   * @return the OID for the table being scanned
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // OID for table being scanned
  catalog::table_oid_t table_oid_;

 public:
  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode);
};

}  // namespace terrier::plan_node
