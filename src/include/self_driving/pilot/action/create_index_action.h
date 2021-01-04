#pragma once

#include <utility>
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
   * @param table_name The table to create index on
   * @param columns The columns to build index on
   */
  CreateIndexAction(std::string table_name, std::vector<IndexColumn> columns)
      : AbstractAction(ActionType::CREATE_INDEX), table_name_(std::move(table_name)), columns_(std::move(columns)) {
    // Index name is a combination of table name and the column names
    index_name_ = "AutomatedIndex" + table_name_;
    for (auto &column : columns_) index_name_ += column.GetColumnName();

    sql_command_ = "CREATE INDEX " + index_name_ + " ON " + table_name_ + " (";

    for (auto &column : columns_) sql_command_ += column.GetColumnName() + ", ";

    sql_command_ += ");";
  }

  /**
   * @return Name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

 private:
  std::string table_name_;
  std::string index_name_;
  std::vector<IndexColumn> columns_;

  // TODO(Lin): Add other create index specific options, e.g., the # threads
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
