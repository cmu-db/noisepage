#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/json.h"
#include "parser/parser_defs.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/plan_visitor.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace noisepage::planner {

/**
 * Plan node for a ctescan operator
 *
 * There are two "kinds" of CteScanPlanNodes: those that are leaders and those that are not
 * The leader node is the FIRST node to attempt to read from a CTE table; all other nodes
 * that read from this CTE table are considered not to be the leader. The leader is responsible
 * for populating the table based on whatever subqueries were used to define the CTE before reading
 * from the table, while all other nodes simply read from it.
 */
class CteScanPlanNode : public SeqScanPlanNode {
 public:
  /**
   * Builder for cte scan plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param is_leader bool indicating leader or not
     * @return builder object
     */
    Builder &SetLeader(bool is_leader) {
      is_leader_ = is_leader;
      return *this;
    }

    /**
     * @param table_schema schema for the cte table
     * @return builder object
     */
    Builder &SetTableSchema(catalog::Schema table_schema) {
      table_schema_ = std::move(table_schema);
      return *this;
    }

    /**
     * @param cte_type The cte type to set this node to
     * @return builder object
     */
    Builder &SetCTEType(parser::CTEType cte_type) {
      cte_type_ = cte_type;
      return *this;
    }

    /**
     * @param table_name The table name this node's cte refers to
     * @return builder object
     */
    Builder &SetCTETableName(std::string &&table_name) {
      cte_table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param scan_predicate Sets a scan predicate for this
     * @return builder object
     */
    Builder &SetScanPredicate(common::ManagedPointer<parser::AbstractExpression> scan_predicate) {
      scan_predicate_ = scan_predicate;
      return *this;
    }

    /**
     * @param table_oid The temp table oid for this cte table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * Build the CTE scan plan node
     * @return plan node
     */
    std::unique_ptr<CteScanPlanNode> Build() {
      std::vector<catalog::col_oid_t> col_oids;
      for (auto &col : table_schema_.GetColumns()) {
        col_oids.push_back(col.Oid());
      }
      return std::unique_ptr<CteScanPlanNode>(
          new CteScanPlanNode(std::move(cte_table_name_), std::move(children_), std::move(output_schema_), is_leader_,
                              table_oid_, std::move(table_schema_), cte_type_, std::move(col_oids), scan_predicate_));
    }

   private:
    std::string cte_table_name_;
    bool is_leader_ = false;
    parser::CTEType cte_type_ = parser::CTEType::SIMPLE;
    catalog::Schema table_schema_;
    common::ManagedPointer<parser::AbstractExpression> scan_predicate_{nullptr};
    catalog::table_oid_t table_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  CteScanPlanNode(std::string &&cte_table_name, std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::unique_ptr<OutputSchema> output_schema, bool is_leader, catalog::table_oid_t table_oid,
                  catalog::Schema table_schema, parser::CTEType cte_type, std::vector<catalog::col_oid_t> &&column_oids,
                  common::ManagedPointer<parser::AbstractExpression> scan_predicate)
      : SeqScanPlanNode(std::move(children), std::move(output_schema), scan_predicate, std::move(column_oids), false,
                        catalog::MakeTempOid<catalog::db_oid_t>(catalog::INVALID_DATABASE_OID.UnderlyingValue()),
                        table_oid, 0, false, 0, false),
        cte_table_name_(std::move(cte_table_name)),
        is_leader_(is_leader),
        cte_type_(cte_type),
        table_schema_(std::move(table_schema)) {
    if (is_leader_) {
      leader_ = this;
    }
  }

 public:
  /**
   * Constructor used for JSON serialization
   */
  CteScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CteScanPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override {
    return is_leader_ ? PlanNodeType::CTESCANLEADER : PlanNodeType::CTESCAN;
  }

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  /**
   * @return True if this node is assigned the CTE leader node and will
   * populate the temporary table
   */
  bool IsLeader() const { return is_leader_; }

  /**
   * Assigns this node as the leader CTE scan node
   */
  void MakeLeader() { is_leader_ = true; }

  /**
   * Sets the leader cte node for this/the node that fills in the cte temp table
   * @param leader The leader for this cte node
   */
  void SetLeader(common::ManagedPointer<const planner::CteScanPlanNode> leader) { leader_ = leader; }

  /**
   * Gets the leader of this node
   * @return The leader for this cte node
   */
  common::ManagedPointer<const planner::CteScanPlanNode> GetLeader() const { return leader_; }

  /**
   * @return What cte type this node represents
   */
  parser::CTEType GetCTEType() const { return cte_type_; }

  /**
   * @return Whether or not this is an iterative cte
   */
  bool GetIsIterative() const { return cte_type_ == parser::CTEType::ITERATIVE; }

  /**
   * @return Whether or not this is a recursive cte
   */
  bool GetIsRecursive() const { return cte_type_ == parser::CTEType::RECURSIVE; }

  /**
   * @return Whether or not this is an inductive (recursive or iterative) cte
   */
  bool GetIsInductive() const { return GetIsRecursive() || GetIsIterative(); }

  /**
   * Set the cte type for this node
   * @param cte_type The cte type to set this node to
   */
  void SetCTEType(parser::CTEType cte_type) { cte_type_ = cte_type; }

  /**
   * @return table output schema for the node. The output schema contains information on columns of the output of the
   * plan node operator
   */
  common::ManagedPointer<const catalog::Schema> GetTableSchema() const {
    return common::ManagedPointer(&table_schema_);
  }

  /**
   * @return The table name of this cte
   */
  const std::string &GetCTETableName() const { return cte_table_name_; }

  //===--------------------------------------------------------------------===//
  // Update schema
  //===--------------------------------------------------------------------===//

  /**
   * Output schema for the node. The output schema contains information on columns of the output of the plan
   * node operator
   * @param schema output schema for plan node
   */
  void SetTableSchema(catalog::Schema schema) {
    table_schema_ = std::move(schema);
  }

 private:
  std::string cte_table_name_;
  // Boolean to indicate whether this plan node is leader or not
  bool is_leader_;
  parser::CTEType cte_type_;
  // Output table schema for CTE scan
  catalog::Schema table_schema_;
  common::ManagedPointer<const CteScanPlanNode> leader_{nullptr};
};

}  // namespace noisepage::planner
