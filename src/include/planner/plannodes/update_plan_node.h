#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parameter.h"
#include "parser/update_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::planner {

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
     * @param update_primary_key whether to update primary key
     * @return builder object
     */
    Builder &SetUpdatePrimaryKey(bool update_primary_key) {
      update_primary_key_ = update_primary_key;
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::shared_ptr<UpdatePlanNode> Build() {
      return std::shared_ptr<UpdatePlanNode>(new UpdatePlanNode(std::move(children_), std::move(output_schema_),
                                                                database_oid_, namespace_oid_, table_oid_,
                                                                update_primary_key_));
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
     * OID of the table to update
     */
    catalog::table_oid_t table_oid_;

    /**
     * Whether to update primary key
     */
    bool update_primary_key_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the target SQL table
   * @param update_primary_key whether to update primary key
   */
  UpdatePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                 bool update_primary_key)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_oid_(table_oid),
        update_primary_key_(update_primary_key) {}

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
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return the OID of the target table to operate on
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return whether to update primary key
   */
  bool GetUpdatePrimaryKey() const { return update_primary_key_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UPDATE; }

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
   * OID of the table to update
   */
  catalog::table_oid_t table_oid_;

  /**
   * Whether to update primary key
   */
  bool update_primary_key_;
};

DEFINE_JSON_DECLARATIONS(UpdatePlanNode);

}  // namespace terrier::planner
