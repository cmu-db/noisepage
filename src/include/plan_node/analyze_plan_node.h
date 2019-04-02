#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/analyze_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {
namespace catalog {
class Schema;
}

namespace plan_node {
/**
 * The plan node for ANALYZE
 */
class AnalyzePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an analyze plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param table_name name of the target table
     * @return builder object
     */
    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param column_names names of the columns of the target table
     * @return builder object
     */
    Builder &SetColumnNames(std::vector<std::string> &&column_names) {
      column_names_ = std::move(column_names);
      return *this;
    }

    /**
     * @param analyze_stmt the SQL ANALYZE statement
     * @return builder object
     */
    Builder &SetFromAnalyzeStatement(parser::AnalyzeStatement *analyze_stmt) {
      table_name_ = analyze_stmt->GetAnalyzeTable()->GetTableName();
      column_names_ = *analyze_stmt->GetAnalyzeColumns();
      return *this;
    }

    /**
     * Build the analyze plan node
     * @return plan node
     */
    std::shared_ptr<AnalyzePlanNode> Build() {
      return std::shared_ptr<AnalyzePlanNode>(new AnalyzePlanNode(std::move(children_), std::move(output_schema_),
                                                                  table_oid_, std::move(table_name_),
                                                                  std::move(column_names_)));
    }

   protected:
    /**
     * OID of the target table
     */
    catalog::table_oid_t table_oid_;

    /**
     * name of the target table
     */
    std::string table_name_;

    /**
     * names of the columns to be analyzed
     */
    std::vector<std::string> column_names_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param table_oid the OID of the target SQL table
   * @param table_name name of the target table
   * @param column_names names of the columns of the target table
   */
  AnalyzePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::shared_ptr<OutputSchema> output_schema, catalog::table_oid_t table_oid, std::string table_name,
                  std::vector<std::string> &&column_names)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        table_oid_(table_oid),
        table_name_(std::move(table_name)),
        column_names_(std::move(column_names)) {}

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ANALYZE; }

  /**
   * @return the OID of the target table
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the name of the target table
   */
  std::string GetTableName() const { return table_name_; }

  /**
   * @return the names of the columns to be analyzed
   */
  std::vector<std::string> GetColumnNames() const { return column_names_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * OID of the target table
   */
  catalog::table_oid_t table_oid_;

  /**
   * name of the target table
   */
  std::string table_name_;

  /**
   * names of the columns to be analyzed
   */
  std::vector<std::string> column_names_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AnalyzePlanNode);
};

}  // namespace plan_node
}  // namespace terrier
