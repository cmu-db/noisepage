#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/abstract_scan_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

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
     * @param column_oids OIDs of columns to scan
     * @return builder object
     */
    Builder &SetColumnOids(std::vector<catalog::col_oid_t> &&column_oids) {
      column_oids_ = std::move(column_oids);
      return *this;
    }

    /**
     * Build the sequential scan plan node
     * @return plan node
     */
    std::unique_ptr<SeqScanPlanNode> Build();

   protected:
    /**
     * OIDs of columns to scan
     */
    std::vector<catalog::col_oid_t> column_oids_;

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
   * @param column_oids OIDs of columns to scan
   * @param is_for_update flag for if scan is for an update
   * @param database_oid database oid for scan
   * @param table_oid OID for table to scan
   * @param plan_node_id Plan node id
   */
  SeqScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::unique_ptr<OutputSchema> output_schema,
                  common::ManagedPointer<parser::AbstractExpression> predicate,
                  std::vector<catalog::col_oid_t> &&column_oids, bool is_for_update, catalog::db_oid_t database_oid,
                  catalog::table_oid_t table_oid, uint32_t scan_limit, bool scan_has_limit, uint32_t scan_offset,
                  bool scan_has_offset, plan_node_id_t plan_node_id);

 public:
  /**
   * Default constructor used for deserialization
   */
  SeqScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  /**
   * @return OIDs of columns to scan
   */
  const std::vector<catalog::col_oid_t> &GetColumnOids() const { return column_oids_; }

  /**
   * @return the OID for the table being scanned
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OIDs of columns to scan
   */
  std::vector<catalog::col_oid_t> column_oids_;

  /**
   * OID for table being scanned
   */
  catalog::table_oid_t table_oid_;
};

DEFINE_JSON_HEADER_DECLARATIONS(SeqScanPlanNode);

}  // namespace noisepage::planner
