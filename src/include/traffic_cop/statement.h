#pragma once

#include <sqlite3.h>
#include <utility>
#include <vector>
#include "network/postgres_protocol_utils.h"
#include "type/transient_value.h"

namespace terrier::traffic_cop {

/**
 * Statement contains a prepared statement by the backend.
 */
class Statement {
 public:
  Statement(sqlite3_stmt *stmt, std::vector<type::TypeId> param_types)
      : sqlite3_stmt_(stmt), param_types_(std::move(param_types)) {}

  /* The sqlite3 statement */
  sqlite3_stmt *sqlite3_stmt_;

  /* The types of the parameters*/
  std::vector<type::TypeId> param_types_;

  /**
   * Return the number of the parameters.
   * @return
   */
  size_t NumParams() { return param_types_.size(); }

  /**
   * Clean up the sqlite3_stmt.
   */
  void Finalize() { sqlite3_finalize(sqlite3_stmt_); }
};

}  // namespace terrier::traffic_cop
