#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/delete_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {
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
     * @param table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param index_oids vector of index oids to insert into
     * @return builder object
     */
    Builder &SetIndexOids(std::vector<catalog::index_oid_t> &&index_oids) {
      index_oids_ = index_oids;
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::unique_ptr<DeletePlanNode> Build();

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * the table to be deleted
     */
    catalog::table_oid_t table_oid_;

    /**
     * vector of indexes to delete from
     */
    std::vector<catalog::index_oid_t> index_oids_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid the OID of the target SQL table
   * @param index_oids indexes to delete from
   * @param plan_node_id Plan node id
   */
  DeletePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                 std::vector<catalog::index_oid_t> &&index_oids, plan_node_id_t plan_node_id);

 public:
  /**
   * Default constructor used for deserialization
   */
  DeletePlanNode() = default;

  ~DeletePlanNode() override = default;

  DISALLOW_COPY_AND_MOVE(DeletePlanNode)

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the table to be deleted
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return indexes to delete from
   */
  const std::vector<catalog::index_oid_t> &GetIndexOids() const { return index_oids_; }

  /** @return the type of this plan node */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  /** @return the hashed value of this plan node */
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
   * Table to be deleted
   */
  catalog::table_oid_t table_oid_;

  /**
   * Indexes to delete from
   */
  std::vector<catalog::index_oid_t> index_oids_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DeletePlanNode);

}  // namespace noisepage::planner
