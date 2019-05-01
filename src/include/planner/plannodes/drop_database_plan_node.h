#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/drop_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {
/**
 *  The plan node for dropping databases
 */
class DropDatabasePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop database plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid the OID of the database to drop
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param if_exists true if "IF EXISTS" was used
     * @return builder object
     */
    Builder &SetIfExist(bool if_exists) {
      if_exists_ = if_exists;
      return *this;
    }

    /**
     * @param drop_stmt the SQL DROP statement
     * @return builder object
     */
    Builder &SetFromDropStatement(parser::DropStatement *drop_stmt) {
      if (drop_stmt->GetDropType() == parser::DropStatement::DropType::kDatabase) {
        if_exists_ = drop_stmt->IsIfExists();
      }
      return *this;
    }

    /**
     * Build the drop database plan node
     * @return plan node
     */
    std::shared_ptr<DropDatabasePlanNode> Build() {
      return std::shared_ptr<DropDatabasePlanNode>(
          new DropDatabasePlanNode(std::move(children_), std::move(output_schema_), database_oid_, if_exists_));
    }

   protected:
    /**
     * OID of the database to drop
     */
    catalog::db_oid_t database_oid_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database to drop
   */
  DropDatabasePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        if_exists_(if_exists) {}

 public:
  /**
   * Default constructor for deserialization
   */
  DropDatabasePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropDatabasePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_DATABASE; }

  /**
   * @return OID of the database to drop
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return true if "IF EXISTS" was used
   */
  bool IsIfExists() const { return if_exists_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the database to drop
   */
  catalog::db_oid_t database_oid_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;
};

DEFINE_JSON_DECLARATIONS(DropDatabasePlanNode);

}  // namespace terrier::planner
