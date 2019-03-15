#pragma once

#include "plan_node/abstract_plan_node.h"
#include "plan_node/abstract_scan_plan_node.h"

namespace terrier {

namespace storage {
class SqlTable;
class TupleSlot;
}

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
  InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, uint32_t bulk_insert_count = 1)
      : target_table_(target_table), bulk_insert_count_(bulk_insert_count) {
  }

  /**
   * Instantiate an InsertPlanNode
   * Construct with an OutputSchema
   */
  InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table,
             std::shared_ptr<OutputSchema> output_schema,
             uint32_t bulk_insert_count = 1)
      : AbstractPlanNode(output_schema),
        target_table_(target_table),
        bulk_insert_count_(bulk_insert_count) {
  }

  // Construct with a tuple
  // This can only be handled by the interpreted exeuctor
  InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, std::unique_ptr<storage::TupleSlot> &&tuple,
             uint32_t bulk_insert_count = 1)
      : target_table_(target_table), bulk_insert_count_(bulk_insert_count) {
    tuples_.push_back(std::move(tuple));
  }

  /**
   * Create an insert plan with specific values
   *
   * @param table table to insert into
   * @param columns columns to insert into
   * @param insert_values values to insert
   */
  InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, std::vector<std::string> columns,
             std::vector<
             std::vector<std::unique_ptr<parser::AbstractExpression>>> &&
  insert_values);

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INSERT; };

  /**
   * @return the table to insert into
   */
  storage::SqlTable *GetTable() const { return target_table_; }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  type::Value GetValue(uint32_t idx) const { return values_.at(idx); }

  oid_t GetBulkInsertCount() const { return bulk_insert_count_; }

  const storage::Tuple *GetTuple(int tuple_idx) const {
    if (tuple_idx >= (int)tuples_.size()) {
      return nullptr;
    }
    return tuples_[tuple_idx].get();
  }

  const std::string GetInfo() const override { return "InsertPlan"; }

  void PerformBinding(BindingContext &binding_context) override;

  const std::vector<const AttributeInfo *> &GetAttributeInfos() const {
    return ais_;
  }

  // WARNING - Not Implemented
  std::unique_ptr<AbstractPlan> Copy() const override {
    LOG_INFO("InsertPlan Copy() not implemented");
    // TODO: Add copying mechanism
    std::unique_ptr<AbstractPlan> dummy;
    return dummy;
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

  virtual void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  /**
   * Lookup a column name in the schema columns
   *
   * @param[in]  col_name    column name, from insert statement
   * @param[in]  tbl_columns table columns from the schema
   * @param[out] index       index into schema columns, only if found
   *
   * @return      true if column was found, false otherwise
   */
  bool FindSchemaColIndex(std::string col_name,
                          const std::vector<catalog::Column> &tbl_columns,
                          uint32_t &index);

  /**
   * Process column specification supplied in the insert statement.
   * Construct a map from insert columns to schema columns. Once
   * we know which columns will receive constant inserts, further
   * adjustment of the map will be needed.
   *
   * @param[in] columns        Column specification
   */
  void ProcessColumnSpec(const std::vector<std::string> *columns);

  /**
   * Process a single expression to be inserted.
   *
   * @param[in] expr       insert expression
   * @param[in] schema_idx index into schema columns, where the expr
   *                       will be inserted.
   * @return  true if values imply a prepared statement
   *          false if all values are constants. This does not rule
   *             out the insert being a prepared statement.
   */
  bool ProcessValueExpr(expression::AbstractExpression *expr,
                        uint32_t schema_idx);

  /**
   * Set default value into a schema column
   *
   * @param[in] idx  schema column index
   */
  void SetDefaultValue(uint32_t idx);

 private:
  // mapping from schema columns to insert columns
  struct SchemaColsToInsertCols {
    // this schema column is present in the insert columns
    bool in_insert_cols;

    // For a PS, insert saved value (from constant in insert values list), no
    // param value.
    bool set_value;

    // index of this column in insert columns values
    int val_idx;

    // schema column type
    type::TypeId type;

    // set_value refers to this saved value
    type::Value value;
  };

  // Target table
  std::shared_ptr<storage::SqlTable> target_table_ = nullptr;

  // Values
  std::vector<type::TransientValue> values_;

  // mapping from schema columns to vector of insert columns
  std::vector<SchemaColsToInsertCols> schema_to_insert_;
  // mapping from insert columns to schema columns
  std::vector<uint32_t> insert_to_schema_;

  // Tuple : To be deprecated after the interpreted execution disappears
  std::vector<std::unique_ptr<storage::Tuple>> tuples_;

  // Parameter Information <tuple_index, tuple_column_index, parameter_index>
  std::unique_ptr<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
  parameter_vector_;

  // Parameter value types
  std::unique_ptr<std::vector<type::TypeId>> params_value_type_;

  // Number of times to insert
  uint32_t bulk_insert_count_;

  DISALLOW_COPY_AND_MOVE(InsertPlanNode);
};
}  // namespace planner
}  // namespace peloton