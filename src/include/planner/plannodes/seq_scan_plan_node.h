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
                  std::shared_ptr<OutputSchema> output_schema, std::shared_ptr<parser::AbstractExpression> predicate,
                  bool is_for_update, bool is_parallel, catalog::db_oid_t database_oid,
                  catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), std::move(predicate), is_for_update,
                             is_parallel, database_oid, namespace_oid),
        table_oid_(table_oid) {}

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
   * @return the OID for the table being scanned
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

  /**
   * Collect all column oids in this expression
   * @return the vector of unique columns oids
   */
  std::vector<catalog::col_oid_t> CollectInputOids() const {
    std::vector<catalog::col_oid_t> result;
    // Scan predicate
    if (GetScanPredicate() != nullptr) CollectOids(&result, GetScanPredicate().get());
    // Output expressions
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      CollectOids(&result, col.GetExpr());
    }
    // Remove duplicates
    std::unordered_set<catalog::col_oid_t> s(result.begin(), result.end());
    result.assign(s.begin(), s.end());
    return result;
  }

 private:
  void CollectOids(std::vector<catalog::col_oid_t> *result, const parser::AbstractExpression *expr) const {
    if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto column_val = static_cast<const parser::ColumnValueExpression *>(expr);
      result->emplace_back(column_val->GetColumnOid());
    } else {
      for (const auto &child : expr->GetChildren()) {
        CollectOids(result, child.get());
      }
    }
  }

  /**
   * OID for table being scanned
   */
  catalog::table_oid_t table_oid_;
};

DEFINE_JSON_DECLARATIONS(SeqScanPlanNode);

}  // namespace terrier::planner
