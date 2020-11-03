#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "parser/create_statement.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for creating indexes
 */
class CreateIndexPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create index plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param namespace_oid OID of the namespace
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *this;
    }

    /**
     * @param table_oid OID of the table to create index on
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param index_name the name of the index
     * @return builder object
     */
    Builder &SetIndexName(std::string index_name) {
      index_name_ = std::move(index_name);
      return *this;
    }

    /**
     * @param schema table schema, node takes ownership
     * @return builder object
     */
    Builder &SetSchema(std::unique_ptr<catalog::IndexSchema> schema) {
      schema_ = std::move(schema);
      return *this;
    }

    /**
     * Build the create index plan node
     * @return plan node
     */
    std::unique_ptr<CreateIndexPlanNode> Build();

   protected:
    /**
     * OID of namespace
     */
    catalog::namespace_oid_t namespace_oid_;

    /**
     * OID of the table to create index on
     */
    catalog::table_oid_t table_oid_;

    /**
     * Name of the Index
     */
    std::string index_name_;

    /**
     * table schema
     */
    std::unique_ptr<catalog::IndexSchema> schema_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table to create index on
   * @param table_schema schema of the table to create
   * @param index_type type of index to create
   * @param unique_index true if index should be unique
   * @param index_name name of index to be created
   */
  CreateIndexPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                      std::unique_ptr<OutputSchema> output_schema, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid, std::string index_name,
                      std::unique_ptr<catalog::IndexSchema> schema);

 public:
  /**
   * Default constructor for deserialization
   */
  CreateIndexPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateIndexPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_INDEX; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table to create index on
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

  /**
   * @return pointer to the schema
   */
  common::ManagedPointer<catalog::IndexSchema> GetSchema() const { return common::ManagedPointer(schema_); }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  catalog::namespace_oid_t namespace_oid_;
  catalog::table_oid_t table_oid_;
  std::string index_name_;
  std::unique_ptr<catalog::IndexSchema> schema_;
};

DEFINE_JSON_HEADER_DECLARATIONS(CreateIndexPlanNode);

}  // namespace noisepage::planner
