#pragma once

#include <string>
#include <utility>
#include <vector>

#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/index_column.h"

namespace noisepage {

namespace planner {
class CreateIndexPlanNode;
}  // namespace planner

namespace selfdriving::pilot {

class ActionState;

/**
 * Represent a create index self-driving action
 */
class CreateIndexAction : public AbstractAction {
 public:
  /**
   * Construct CreateIndexAction
   * @param db_oid Database id of the index
   * @param index_name Name of the index
   * @param table_name The table to create index on
   * @param table_oid The oid of the table to create index on
   * @param columns The columns to build index on
   */
  CreateIndexAction(catalog::db_oid_t db_oid, std::string index_name, std::string table_name,
                    catalog::table_oid_t table_oid, std::vector<IndexColumn> columns)
      : AbstractAction(ActionType::CREATE_INDEX, db_oid),
        index_name_(std::move(index_name)),
        table_name_(std::move(table_name)),
        table_oid_(table_oid),
        columns_(std::move(columns)) {
    NOISEPAGE_ASSERT(!columns_.empty(), "Should not create index without any columns!");

    sql_command_ = "create index " + index_name_ + " on " + table_name_ + "(";

    for (auto &column : columns_) sql_command_ += column.GetColumnName() + ", ";

    sql_command_ = sql_command_.substr(0, sql_command_.size() - 2) + ");";
  }

  /**
   * @return Name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

  /**
   * @return Table oid of this index
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  void ModifyActionState(ActionState *action_state) override;

 private:
  std::string index_name_;
  std::string table_name_;
  catalog::table_oid_t table_oid_;
  std::vector<IndexColumn> columns_;

  // TODO(Lin): Add other create index specific options, e.g., the # threads
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
