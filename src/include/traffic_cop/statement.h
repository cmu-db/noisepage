#pragma once

#include <sqlite3.h>
#include <string>
#include <utility>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "type/transient_value.h"

namespace terrier::trafficcop {

/**
 * Statement contains a prepared statement by the backend.
 */
class Statement {
 public:
  /**
   * Creates an empty Statement.
   */
  Statement() { sqlite3_stmt_ = nullptr; }

  /**
   * Creates a Statement with parsed sqlite_stmt and param types.
   * @param stmt
   * @param param_types
   */
  Statement(sqlite3_stmt *stmt, std::vector<network::PostgresValueType> param_types)
      : sqlite3_stmt_(stmt), param_types_(std::move(param_types)) {}

  /**
   * Creates a Statement with parsed sqlite_stmt, param types, and query string
   * @param stmt
   * @param param_types
   * @param query_string
   */
  Statement(sqlite3_stmt *stmt, std::vector<network::PostgresValueType> param_types, std::string query_string)
      : sqlite3_stmt_(stmt), param_types_(std::move(param_types)), query_string_(std::move(query_string)) {}

  /**
   * The sqlite3 statement
   */
  sqlite3_stmt *sqlite3_stmt_;

  /**
   * The types of the parameters
   * To satisfy Describe command, we store Postgres type oid here instead of internal type ids.
   */
  std::vector<network::PostgresValueType> param_types_;

  /**
   * Corresponding query string
   */
  std::string query_string_;

  /**
   * Returns the number of the parameters.
   * @return
   */
  size_t NumParams() { return param_types_.size(); }

  /**
   * Cleans up the sqlite3_stmt.
   */
  void Finalize() { sqlite3_finalize(sqlite3_stmt_); }
};

}  // namespace terrier::trafficcop
