#pragma once

#include <string>
#include <utility>
#include <vector>

#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/index_column.h"

namespace noisepage::selfdriving::pilot {

/**
 * Represent a drop index self-driving action
 */
class DropIndexAction : public AbstractAction {
 public:
  /**
   * Construct DropIndexAction
   * @param db_oid Database id of the index
   * @param index_name The name of the index
   * @param table_name The table to create index on
   * @param columns The columns to build index on
   */
  DropIndexAction(catalog::db_oid_t db_oid, std::string index_name, std::string table_name,
                  std::vector<IndexColumn> columns)
      : AbstractAction(ActionType::DROP_INDEX, db_oid),
        index_name_(std::move(index_name)),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)) {
    sql_command_ = "drop index " + index_name_ + ";";
  }

  /**
   * @return Name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

 private:
  std::string index_name_;
  std::string table_name_;
  std::vector<IndexColumn> columns_;
};

}  // namespace noisepage::selfdriving::pilot
