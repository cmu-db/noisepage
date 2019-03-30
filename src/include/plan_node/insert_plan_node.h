#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/insert_statement.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/abstract_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier {

namespace plan_node {

class InsertPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an insert plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    /**
     * Don't allow builder to be copied or moved
     */
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
     * @param parameter_vector parameters information
     * @return builder object
     */
    Builder &SetBulkInsertCOunt(uint32_t bulk_insert_count) {
      bulk_insert_count_ = bulk_insert_count;
      return *this;
    }

    /**
     * @param delete_stmt the SQL DELETE statement
     * @return builder object
     */
    Builder &SetFromInsertStatement(parser::InsertStatement *insert_stmt) {
      table_name_ = insert_stmt->GetInsertionTable()->GetTableName();
      // TODO(Gus,Wen) fill in parameters
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::shared_ptr<InsertPlanNode> Build() {
      return std::shared_ptr<InsertPlanNode>(
          new InsertPlanNode(std::move(children_), std::move(output_schema_), target_table_oid_, std::move(table_name_),
                             std::move(values_), std::move(parameter_info_), bulk_insert_count_));
    }

   protected:
    catalog::table_oid_t target_table_oid_;
    std::string table_name_;
    std::vector<type::TransientValue> values_;
    std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> parameter_info_;
    uint32_t bulk_insert_count_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param target_table_oid the OID of the target SQL table
   * @param table_name name of the target table
   * @param values values to insert
   * @param parameter_info parameters information
   * @param bulk_insert_count the number of times to insert
   */
  InsertPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 catalog::table_oid_t target_table_oid, std::string table_name,
                 std::vector<type::TransientValue> &&values,
                 std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> &&parameter_info, uint32_t bulk_insert_count)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        target_table_oid_(target_table_oid),
        table_name_(std::move(table_name)),
        values_(std::move(values)),
        parameter_info_(std::move(parameter_info)),
        bulk_insert_count_(bulk_insert_count) {}

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INSERT; };

  /**
   * @return the OID of the table to insert into
   */
  catalog::table_oid_t GetTargetTableOid() const { return target_table_oid_; }

  /**
   * @return the name of the table to insert into
   */
  const std::string &GetTableName() const { return table_name_; }

  // TODO(Gus,Wen) use transient value peeker to peek values

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

 private:
  /**
   * OID of the target table
   */
  catalog::table_oid_t target_table_oid_;

  // Table name
  std::string table_name_;

  // Values to insert
  std::vector<type::TransientValue> values_;

  // TODO(Gus,Wen) the storage layer is different now, need to whether reconsider this mapping approach is still valid
  // Parameter Information <tuple_index,  tuple_column_index, value_index>
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> parameter_info_;

  // Number of times to insert
  uint32_t bulk_insert_count_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(InsertPlanNode);
};
}  // namespace plan_node
}  // namespace terrier
