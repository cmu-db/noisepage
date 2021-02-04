#pragma once

#include <string>
#include <utility>
#include <vector>

#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/index_column.h"

namespace noisepage {

namespace planner {
class CreateIndexPlanNode;
}  // namespace planner

namespace selfdriving::pilot {

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
   * @param columns The columns to build index on
   */
  CreateIndexAction(catalog::db_oid_t db_oid, std::string index_name, std::string table_name,
                    std::vector<IndexColumn> columns)
      : AbstractAction(ActionType::CREATE_INDEX, db_oid),
        index_name_(std::move(index_name)),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)) {
    sql_command_ = "create index " + index_name_ + " on " + table_name_ + "(";

    for (auto &column : columns_) sql_command_ += column.GetColumnName() + ", ";

    sql_command_ += ");";
  }

  /**
   * @return Name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

 private:
  std::string index_name_;
  std::string table_name_;
  std::vector<IndexColumn> columns_;

  // TODO(Lin): Add other create index specific options, e.g., the # threads
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
