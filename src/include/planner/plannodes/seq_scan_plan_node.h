#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/abstract_scan_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for sequanial table scan
 */
class SeqScanPlanNode : public AbstractScanPlanNode {
 public:
  /**
   * Builder for a sequential scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param oid oid for table to scan
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t oid) {
      table_oid_ = oid;
      return *this;
    }

    /**
     * Build the sequential scan plan node
     * @return plan node
     */
    std::shared_ptr<SeqScanPlanNode> Build() {
      return std::shared_ptr<SeqScanPlanNode>(
          new SeqScanPlanNode(std::move(children_), std::move(output_schema_), std::move(scan_predicate_),
                              is_for_update_, is_parallel_, database_oid_, namespace_oid_, table_oid_));
    }

   protected:
    /**
     * OID for table being scanned
     */
    catalog::table_oid_t table_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate scan predicate
   * @param is_for_update flag for if scan is for an update
   * @param is_parallel flag for parallel scan
   * @param database_oid database oid for scan
   * @param table_oid OID for table to scan
   */
  SeqScanPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                  std::shared_ptr<OutputSchema> output_schema,
                  std::shared_ptr<const parser::AbstractExpression> predicate, bool is_for_update, bool is_parallel,
                  catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                  catalog::table_oid_t table_oid)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), std::move(predicate), is_for_update,
                             is_parallel, database_oid, namespace_oid),
        table_oid_(table_oid) {}

 public:
  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode)

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

 private:
  /**
   * OID for table being scanned
   */
  catalog::table_oid_t table_oid_;
};

}  // namespace terrier::planner
