
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {
namespace plan_node {

/**
 * Plan node for populating an index
 * IMPORTANT All tiles got from child must have the same physical schema.
 */

class PopulateIndexPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for a populate index plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param target_table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTargetTableOid(catalog::table_oid_t target_table_oid) {
      target_table_oid_ = target_table_oid;
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
    Builder &SetColumnOids(std::vector<catalog::col_oid_t> &&column_oids) {
      column_oids_ = std::move(column_oids);
      return *this;
    }

    /**
     * Build the setop plan node
     * @return plan node
     */
    std::shared_ptr<PopulateIndexPlanNode> Build() {
      return std::shared_ptr<PopulateIndexPlanNode>(
          new PopulateIndexPlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_,
                                    target_table_oid_, std::move(table_name_), std::move(column_oids_)));
    }

   protected:
    catalog::table_oid_t target_table_oid_;
    std::string table_name_;
    std::vector<catalog::col_oid_t> column_oids_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param set_op the set pperation of this node
   */
  PopulateIndexPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                        std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                        catalog::table_oid_t target_table_oid, std::string table_name,
                        std::vector<catalog::col_oid_t> &&column_oids)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        target_table_oid_(target_table_oid),
        table_name_(std::move(table_name)),
        column_oids_(std::move(column_oids)) {}

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::POPULATE_INDEX; }

  /**
   * @return the column IDs to be populated into the index
   */
  const std::vector<catalog::col_oid_t> &GetColumnOids() const { return column_oids_; }

  /**
   * @return the OID of the target table
   */
  catalog::table_oid_t GetTargetTableOid() const { return target_table_oid_; }

  /**
   * @return the target table name
   */
  const std::string &GetTableName() const { return table_name_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  // Target table OID
  catalog::table_oid_t target_table_oid_;

  // Table name
  std::string table_name_;

  // Column Ids
  std::vector<catalog::col_oid_t> column_oids_;

 public:
  DISALLOW_COPY_AND_MOVE(PopulateIndexPlanNode);
};

}  // namespace plan_node
}  // namespace terrier
