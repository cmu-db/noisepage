#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/delete_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {
/**
 * The plan node for DELETE
 */
class DeletePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a delete plan node
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
     * @param namespace_oid OID of the namespace
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
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
     * @param delete_condition expression of delete condition
     * @return builder object
     */
    Builder &SetDeleteCondition(std::shared_ptr<parser::AbstractExpression> delete_condition) {
      delete_condition_ = std::move(delete_condition);
      return *this;
    }

    /**
     * @param delete_stmt the SQL DELETE statement
     * @return builder object
     */
    Builder &SetFromDeleteStatement(parser::DeleteStatement *delete_stmt) {
      delete_condition_ = delete_stmt->GetDeleteCondition();
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::shared_ptr<DeletePlanNode> Build() {
      return std::shared_ptr<DeletePlanNode>(new DeletePlanNode(std::move(children_), std::move(output_schema_),
                                                                database_oid_, namespace_oid_, table_oid_,
                                                                std::move(delete_condition_)));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of namespace
     */
    catalog::namespace_oid_t namespace_oid_;

    /**
     * the table to be deleted
     */
    catalog::table_oid_t table_oid_;

    /**
     * expression of delete condition
     */
    std::shared_ptr<parser::AbstractExpression> delete_condition_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid the OID of the target SQL table
   * @param delete_condition expression of delete condition
   */
  DeletePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                 std::shared_ptr<parser::AbstractExpression> delete_condition)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_oid_(table_oid),
        delete_condition_(std::move(delete_condition)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  DeletePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DeletePlanNode)

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table to be deleted
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the expression of delete condition
   */
  std::shared_ptr<parser::AbstractExpression> GetDeleteCondition() const { return delete_condition_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * Table to be deleted
   */
  catalog::table_oid_t table_oid_;

  /**
   * Expression of delete condition
   */
  std::shared_ptr<parser::AbstractExpression> delete_condition_;
};

DEFINE_JSON_DECLARATIONS(DeletePlanNode);

}  // namespace terrier::planner
