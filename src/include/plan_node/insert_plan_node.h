#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/abstract_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier {

namespace storage {
class SqlTable;
}  // namespace storage

namespace parser {
class InsertStatement;
}

namespace plan_node {

class InsertPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate an InsertPlanNode
   * Construct when SELECT comes in with it
   */
  explicit InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, uint32_t bulk_insert_count = 1)
      : target_table_(std::move(target_table)), bulk_insert_count_(bulk_insert_count) {}

  /**
   * Instantiate an InsertPlanNode
   * Construct with an OutputSchema
   */
  explicit InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, std::shared_ptr<OutputSchema> output_schema,
                          uint32_t bulk_insert_count = 1)
      : AbstractPlanNode(std::move(output_schema)),
        target_table_(std::move(target_table)),
        bulk_insert_count_(bulk_insert_count) {}

  /**
   * Create an insert plan with specific values
   *
   * @param table table to insert into
   * @param columns columns to insert into
   * @param insert_values values to insert
   */
  explicit InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, const std::vector<std::string> &columns,
                          std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> &&insert_values);

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INSERT; };

  /**
   * @return the table to insert into
   */
  std::shared_ptr<storage::SqlTable> GetTargetTable() const { return target_table_; }

  // TODO(Gus,Wen) use transient value peeker to peek values

  /**
   * @return number of times to insert
   */
  uint32_t GetBulkInsertCount() const { return bulk_insert_count_; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "InsertPlanNode"; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    // TODO(Gus,Wen) Add copying mechanism
    std::unique_ptr<AbstractPlanNode> dummy;
    return dummy;
  }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  /**
   * Lookup a column name in the schema columns
   *
   * @param  col_name    column name, from insert statement
   * @param  tbl_columns table columns from the schema
   * @param  index       index into schema columns, only if found
   *
   * @return true if column was found, false otherwise
   */
  bool FindSchemaColIndex(const std::string &col_name, const std::vector<catalog::Schema::Column> &tbl_columns,
                          uint32_t *index);

  /**
   * Process column specification supplied in the insert statement.
   * Construct a map from insert columns to schema columns. Once
   * we know which columns will receive constant inserts, further
   * adjustment of the map will be needed.
   *
   * @param columns        Column specification
   */
  void ProcessColumnSpec(const std::vector<std::string> &columns);

  /**
   * Process a single expression to be inserted.
   *
   * @param expr       insert expression
   * @param schema_idx index into schema columns, where the expr
   *                       will be inserted.
   * @return  true if values imply a prepared statement
   *          false if all values are constants. This does not rule
   *             out the insert being a prepared statement.
   */
  bool ProcessValueExpr(parser::AbstractExpression *expr, uint32_t schema_idx);

  /**
   * Set default value into a schema column
   *
   * @param idx  schema column index
   */
  void SetDefaultValue(uint32_t idx);

 private:
  // Target table
  std::shared_ptr<storage::SqlTable> target_table_ = nullptr;

  // Values
  std::vector<type::TransientValue> values_;

  // Parameter Information <tuple_index, column oid, parameter_index>
  std::unique_ptr<std::vector<std::tuple<catalog::offset_oid_t, catalog::col_oid_t, catalog::offset_oid_t>>>
      parameter_vector_;

  // Parameter value types
  std::unique_ptr<std::vector<type::TypeId>> params_value_type_;

  // Number of times to insert
  uint32_t bulk_insert_count_;

  DISALLOW_COPY_AND_MOVE(InsertPlanNode);
};
}  // namespace plan_node
}  // namespace terrier
