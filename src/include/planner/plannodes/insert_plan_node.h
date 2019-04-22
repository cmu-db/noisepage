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
     * @param parameter_info parameter information
     * @return builder object
     */
    Builder &SetParameterInfo(std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> &&parameter_info) {
      parameter_info_ = std::move(parameter_info);
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
     * Build the insert plan node
     * @return plan node
     */
    std::shared_ptr<InsertPlanNode> Build() {
      return std::shared_ptr<InsertPlanNode>(new InsertPlanNode(std::move(children_), std::move(output_schema_),
                                                                database_oid_, table_oid_, std::move(values_),
                                                                std::move(parameter_info_), bulk_insert_count_));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

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
     * parameter information <tuple_index,  tuple_column_index, value_index>
     */
    std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> parameter_info_;

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
   * @param table_oid the OID of the target SQL table
   * @param values values to insert
   * @param parameter_info parameters information
   * @param bulk_insert_count the number of times to insert
   */
  InsertPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                 std::vector<type::TransientValue> &&values,
                 std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> &&parameter_info, uint32_t bulk_insert_count)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        table_oid_(table_oid),
        values_(std::move(values)),
        parameter_info_(std::move(parameter_info)),
        bulk_insert_count_(bulk_insert_count) {}

 public:
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
  const std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> &GetParameterInfo() const { return parameter_info_; }

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
   * OID of the table to insert into
   */
  catalog::table_oid_t table_oid_;

  /**
   * values to insert
   */
  std::vector<type::TransientValue> values_;

  /**
   * parameter information <tuple_index,  tuple_column_index, value_index>
   */
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> parameter_info_;

  /**
   * number of times to insert
   */
  uint32_t bulk_insert_count_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(InsertPlanNode);
};

DEFINE_JSON_DECLARATIONS(InsertPlanNode);

}  // namespace terrier::planner
