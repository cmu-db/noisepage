#pragma once

#include <common/json.h>
#include <parser/parser_defs.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::planner {

/**
 * Plan node for a ctescan operator
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
     * @param table_output_schema output schema for plan node
     * @return builder object
     */
    Builder &SetTableSchema(catalog::Schema table_schema) {
      table_schema_ = std::move(table_schema);
      return *this;
    }

    Builder &SetCTEType(parser::CTEType cte_type) {
      cte_type_ = cte_type;
      return *this;
    }

    Builder &SetCTETableName(std::string &&table_name) {
      cte_table_name_ = std::move(table_name);
      return *this;
    }

    Builder &SetScanPredicate(common::ManagedPointer<parser::AbstractExpression> scan_predicate) {
      scan_predicate_ = scan_predicate;
      return *this;
    }

    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * Build the limit plan node
     * @return plan node
     */
    std::unique_ptr<CteScanPlanNode> Build() {
      std::vector<catalog::col_oid_t> col_oids;
      for(auto &col : table_schema_.GetColumns()){
        col_oids.push_back(col.Oid());
      }
      return std::unique_ptr<CteScanPlanNode>(new CteScanPlanNode(std::move(cte_table_name_),
          std::move(children_), std::move(output_schema_), is_leader_, table_oid_, std::move(table_schema_), cte_type_,
                                                                  std::move(col_oids),scan_predicate_));
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
                  catalog::Schema table_schema, parser::CTEType cte_type,
                  std::vector<catalog::col_oid_t> &&column_oids,
                  common::ManagedPointer<parser::AbstractExpression> scan_predicate)
      : SeqScanPlanNode(std::move(children), std::move(output_schema), scan_predicate, std::move(column_oids),
                        false, TEMP_OID(catalog::db_oid_t, !catalog::INVALID_DATABASE_OID),
                        table_oid, 0, false, 0, false),
        cte_table_name_(std::move(cte_table_name)),
        is_leader_(is_leader),
        cte_type_(cte_type),
        table_schema_(std::move(table_schema)),
        scan_predicate_(scan_predicate) {}

 public:
  /**
   * Constructor used for JSON serialization
   */
  CteScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CteScanPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return is_leader_ ? PlanNodeType::CTESCANLEADER :
                                                                    PlanNodeType::CTESCAN; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  /**
   * @return True is this node is assigned the CTE leader node and will
   * populate the temporary table
   */
  bool IsLeader() const { return is_leader_; }

  /**
   * Assign's this node as the leader CTE scan node
   */
  void MakeLeader() { is_leader_ = true; }

  void SetLeader(common::ManagedPointer<const planner::CteScanPlanNode> leader) { leader_ = leader; }

  common::ManagedPointer<const planner::CteScanPlanNode> GetLeader() const { return leader_; }

  parser::CTEType GetCTEType() const { return cte_type_; }

  bool GetIsIterative() const { return cte_type_ == parser::CTEType::ITERATIVE; }

  bool GetIsRecursive() const { return cte_type_ == parser::CTEType::RECURSIVE; }

  bool GetIsInductive() const { return GetIsRecursive() || GetIsIterative(); }

  /**
   * Assigns a boolean for whether this node is for a recursive tabl
   */
  void SetCTEType(parser::CTEType cte_type) { cte_type_ = cte_type; }

  /**
   * @return table output schema for the node. The output schema contains information on columns of the output of the
   * plan node operator
   */
  common::ManagedPointer<const catalog::Schema> GetTableSchema() const {
    return common::ManagedPointer(&table_schema_);
  }

  const std::string &GetCTETableName() const {
    return cte_table_name_;
  }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

  //===--------------------------------------------------------------------===//
  // Update schema
  //===--------------------------------------------------------------------===//

  /**
   * Output schema for the node. The output schema contains information on columns of the output of the plan
   * node operator
   * @param schema output schema for plan node
   */
  void SetTableSchema(catalog::Schema schema) {
    // TODO(preetang): Test for memory leak
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

  /**
   * Selection predicate.
   */
  UNUSED_ATTRIBUTE common::ManagedPointer<parser::AbstractExpression> scan_predicate_;
};

}  // namespace terrier::planner
