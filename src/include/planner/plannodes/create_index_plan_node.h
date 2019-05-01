#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/create_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

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
     * @param database_oid  OID of the database
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
     * @param unique_index true if index should be unique
     * @return builder object
     */
    Builder &SetUniqueIndex(bool unique_index) {
      unique_index_ = unique_index;
      return *this;
    }

    /**
     * @param index_attrs index attributes
     * @return builder object
     */
    Builder &SetIndexAttrs(std::vector<std::string> &&index_attrs) {
      index_attrs_ = std::move(index_attrs);
      return *this;
    }

    /**
     * @param key_attrs key attributes
     * @return builder object
     */
    Builder &SetKeyAttrs(std::vector<std::string> &&key_attrs) {
      key_attrs_ = key_attrs;
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      if (create_stmt->GetCreateType() == parser::CreateStatement::CreateType::kIndex) {
        index_name_ = std::string(create_stmt->GetIndexName());

        // This holds the attribute names.
        std::vector<std::string> index_attrs_holder;

        for (auto &attr : create_stmt->GetIndexAttributes()) {
          index_attrs_holder.push_back(attr);
        }

        index_attrs_ = index_attrs_holder;

        index_type_ = create_stmt->GetIndexType();

        unique_index_ = create_stmt->IsUniqueIndex();
      }
      return *this;
    }

    /**
     * Build the create index plan node
     * @return plan node
     */
    std::shared_ptr<CreateIndexPlanNode> Build() {
      return std::shared_ptr<CreateIndexPlanNode>(new CreateIndexPlanNode(
          std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, table_oid_, index_type_,
          unique_index_, std::move(index_name_), std::move(index_attrs_), std::move(key_attrs_)));
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
     * OID of the table to create index on
     */
    catalog::table_oid_t table_oid_;

    /**
     * Name of the Index
     */
    std::string index_name_;

    /**
     * Index type
     */
    parser::IndexType index_type_ = parser::IndexType::INVALID;

    /**
     * True if the index is unique
     */
    bool unique_index_ = false;

    /**
     * Index attributes
     */
    std::vector<std::string> index_attrs_;

    /**
     * Attributes that are part of the index key
     */
    std::vector<std::string> key_attrs_;
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
   * @param index_attrs index attributes
   * @param key_attrs key attributes
   */
  CreateIndexPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                      std::shared_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                      catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                      parser::IndexType index_type, bool unique_index, std::string index_name,
                      std::vector<std::string> &&index_attrs, std::vector<std::string> &&key_attrs)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_oid_(table_oid),
        index_type_(index_type),
        unique_index_(unique_index),
        index_name_(std::move(index_name)),
        index_attrs_(std::move(index_attrs)),
        key_attrs_(std::move(key_attrs)) {}

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
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

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
   * @return true if index should be unique
   */
  bool IsUniqueIndex() const { return unique_index_; }

  /**
   * @return index type
   */
  parser::IndexType GetIndexType() const { return index_type_; }

  /**
   * @return index attributes
   */
  const std::vector<std::string> &GetIndexAttributes() const { return index_attrs_; }

  /**
   * @return name of key attributes
   */
  const std::vector<std::string> &GetKeyAttrs() const { return key_attrs_; }

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
   * OID of the table to create index on
   */
  catalog::table_oid_t table_oid_;

  /**
   * Index type
   */
  parser::IndexType index_type_ = parser::IndexType::INVALID;

  /**
   * True if the index is unique
   */
  bool unique_index_ = false;

  /**
   * Name of the Index
   */
  std::string index_name_;

  /**
   * Index attributes
   */
  std::vector<std::string> index_attrs_;

  /**
   * Attributes that are part of the index key
   */
  std::vector<std::string> key_attrs_;
};

DEFINE_JSON_DECLARATIONS(CreateIndexPlanNode);

}  // namespace terrier::planner
