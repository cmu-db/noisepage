#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/parameter.h"
#include "parser/update_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Describes a single SET clause of an UPDATE query.
 * For UPDATE tbl SET [$1] = $2:
 * - $1 is SetClause.first which mentions the column OID to update
 * - $2 is SetClause.second which describes the expression to set
 */
using SetClause = std::pair<catalog::col_oid_t, common::ManagedPointer<parser::AbstractExpression>>;

/**
 * Plan node for update
 */
class UpdatePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an delete plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder)

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
     * @param update_primary_key whether to update primary key
     * @return builder object
     */
    Builder &SetUpdatePrimaryKey(bool update_primary_key) {
      update_primary_key_ = update_primary_key;
      return *this;
    }

    /**
     * @param indexed_update whether to update indexes
     * @return builder object
     */
    Builder &SetIndexedUpdate(bool indexed_update) {
      indexed_update_ = indexed_update;
      return *this;
    }

    /**
     * @param clause SET clause in a update statement
     * @return builder object
     */
    Builder &AddSetClause(SetClause clause) {
      sets_.push_back(clause);
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::unique_ptr<UpdatePlanNode> Build() {
      return std::unique_ptr<UpdatePlanNode>(new UpdatePlanNode(std::move(children_), std::make_unique<OutputSchema>(),
                                                                database_oid_, table_oid_, update_primary_key_,
                                                                indexed_update_, std::move(sets_)));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of the table to update
     */
    catalog::table_oid_t table_oid_;

    /**
     * Whether to update primary key
     */
    bool update_primary_key_;

    /**
     * Whether to update indexes
     */
    bool indexed_update_;

    /**
     * Set Clauses
     */
    std::vector<SetClause> sets_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the target SQL table
   * @param update_primary_key whether to update primary key
   * @param indexed_update whether to update indexes
   * @param sets SET clauses
   */
  UpdatePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, bool update_primary_key,
                 bool indexed_update, std::vector<SetClause> sets)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        table_oid_(table_oid),
        update_primary_key_(update_primary_key),
        indexed_update_(indexed_update),
        sets_(std::move(sets)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  UpdatePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(UpdatePlanNode)

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return the OID of the target table to operate on
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return whether to update primary key
   */
  bool GetUpdatePrimaryKey() const { return update_primary_key_; }

  /**
   * @return whether to update indexes
   */
  bool GetIndexedUpdate() const { return indexed_update_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UPDATE; }

  /**
   * @return SET clauses
   */
  const std::vector<SetClause> &GetSetClauses() const { return sets_; }

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
   * OID of the table to update
   */
  catalog::table_oid_t table_oid_;

  /**
   * Whether to update primary key
   */
  bool update_primary_key_;

  /**
   * Whether to update indexes
   */
  bool indexed_update_;

  /**
   * SET clauses
   */
  std::vector<SetClause> sets_;
};

DEFINE_JSON_HEADER_DECLARATIONS(UpdatePlanNode);

}  // namespace noisepage::planner
