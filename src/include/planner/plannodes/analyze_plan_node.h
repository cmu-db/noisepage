#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/analyze_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

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
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid OID of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param column_oids OIDs of the columns of the target table
     * @return builder object
     */
    Builder &SetColumnOIDs(std::vector<catalog::col_oid_t> &&column_oids) {
      column_oids_ = std::move(column_oids);
      return *this;
    }

    /**
     * Build the analyze plan node
     * @return plan node
     */
    std::unique_ptr<AnalyzePlanNode> Build();

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of the target table
     */
    catalog::table_oid_t table_oid_;

    /**
     * oids of the columns to be analyzed
     */
    std::vector<catalog::col_oid_t> column_oids_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param table_oid OID of the target SQL table
   * @param column_oids OIDs of the columns of the target table
   */
  AnalyzePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                  catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&column_oids);

 public:
  /**
   * Default constructor for deserialization
   */
  AnalyzePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AnalyzePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ANALYZE; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return the OID of the target table
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the OIDs of the columns to be analyzed
   */
  std::vector<catalog::col_oid_t> GetColumnOids() const { return column_oids_; }

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
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the target table
   */
  catalog::table_oid_t table_oid_;

  /**
   * OIDs of the columns to be analyzed
   */
  std::vector<catalog::col_oid_t> column_oids_;
};

}  // namespace noisepage::planner
