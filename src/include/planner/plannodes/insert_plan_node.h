#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/insert_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/abstract_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier::planner {

/**
 * Parameter Info
 */
struct ParameterInfo {
  /**
   * Constructor
   */
  ParameterInfo(uint32_t tuple_index, uint32_t tuple_column_index, uint32_t value_index)
      : tuple_index_(tuple_index), tuple_column_index_(tuple_column_index), value_index_(value_index) {}

  /**
   * Default constructor for deserialization
   */
  ParameterInfo() = default;

  /**
   * Index of tuple
   */
  uint32_t tuple_index_;

  /**
   * Column index
   */
  uint32_t tuple_column_index_;

  /**
   * Index of value
   */
  uint32_t value_index_;

  /**
   * @return the hashed value of this parameter info
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(tuple_index_);
    hash = common::HashUtil::CombineHashes(hash, tuple_column_index_);
    hash = common::HashUtil::CombineHashes(hash, value_index_);
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two parameter info are logically equal
   */
  bool operator==(const ParameterInfo &rhs) const {
    return tuple_index_ == rhs.tuple_index_ && tuple_column_index_ == rhs.tuple_column_index_ &&
           value_index_ == rhs.value_index_;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two parameter info are not logically equal
   */
  bool operator!=(const ParameterInfo &rhs) const { return !(*this == rhs); }

  /**
   * @return serialized ParameterInfo
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["tuple_index"] = tuple_index_;
    j["tuple_column_index"] = tuple_column_index_;
    j["value_index"] = value_index_;
    return j;
  }

  /**
   * Deserializes a ParameterInfo
   * @param j serialized json of ParameterInfo
   */
  void FromJson(const nlohmann::json &j) {
    tuple_index_ = j.at("tuple_index").get<uint32_t>();
    tuple_column_index_ = j.at("tuple_column_index").get<uint32_t>();
    value_index_ = j.at("value_index").get<uint32_t>();
  }
};

/**
 * Plan node for insert
 */
class InsertPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an insert plan node
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
     * @param values values to insert
     * @return builder object
     */
    Builder &SetValues(std::vector<type::TransientValue> &&values) {
      values_ = std::move(values);
      return *this;
    }

    /**
     * @param tuple_index Index of tuple
     * @param tuple_column_index column index
     * @param value_index Index of value
     * @return builder object
     */
    Builder &AddParameterInfo(uint32_t tuple_index, uint32_t tuple_column_index, uint32_t value_index) {
      parameter_info_.emplace_back(tuple_index, tuple_column_index, value_index);
      return *this;
    }

    /**
     * @param bulk_insert_count number of times to insert
     * @return builder object
     */
    Builder &SetBulkInsertCount(uint32_t bulk_insert_count) {
      bulk_insert_count_ = bulk_insert_count;
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::shared_ptr<InsertPlanNode> Build() {
      return std::shared_ptr<InsertPlanNode>(
          new InsertPlanNode(std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, table_oid_,
                             std::move(values_), std::move(parameter_info_), bulk_insert_count_));
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
     * OID of the table to insert into
     */
    catalog::table_oid_t table_oid_;

    /**
     * values to insert
     */
    std::vector<type::TransientValue> values_;

    // TODO(Gus,Wen) the storage layer is different now, need to whether reconsider this mapping approach is still valid
    /**
     * parameter information
     */
    std::vector<ParameterInfo> parameter_info_;

    /**
     * number of times to insert
     */
    uint32_t bulk_insert_count_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid the OID of the target SQL table
   * @param values values to insert
   * @param parameter_info parameters information
   * @param bulk_insert_count the number of times to insert
   */
  InsertPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                 std::vector<type::TransientValue> &&values, std::vector<ParameterInfo> &&parameter_info,
                 uint32_t bulk_insert_count)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_oid_(table_oid),
        values_(std::move(values)),
        parameter_info_(std::move(parameter_info)),
        bulk_insert_count_(bulk_insert_count) {}

 public:
  DISALLOW_COPY_AND_MOVE(InsertPlanNode)

  /**
   * Default constructor used for deserialization
   */
  InsertPlanNode() = default;
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INSERT; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return the OID of the table to insert into
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return values to be inserted
   */
  const std::vector<type::TransientValue> &GetValues() const { return values_; }

  /**
   * @return the information of insert parameters
   */
  const std::vector<ParameterInfo> &GetParameterInfo() const { return parameter_info_; }

  /**
   * @return number of times to insert
   */
  uint32_t GetBulkInsertCount() const { return bulk_insert_count_; }

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
   * OID of the table to insert into
   */
  catalog::table_oid_t table_oid_;

  /**
   * values to insert
   */
  std::vector<type::TransientValue> values_;

  /**
   * parameter information
   */
  std::vector<ParameterInfo> parameter_info_;

  /**
   * name of time to insert
   */
  uint32_t bulk_insert_count_;
};

DEFINE_JSON_DECLARATIONS(InsertPlanNode);
DEFINE_JSON_DECLARATIONS(ParameterInfo);

}  // namespace terrier::planner
