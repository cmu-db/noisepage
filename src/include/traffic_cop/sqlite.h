#pragma once
#include <sqlite3.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "traffic_cop/result_set.h"
#include "type/transient_value.h"

struct sqlite3;

namespace terrier::trafficcop {

/**
 * The sqlite3 execution engine
 */
class SqliteEngine {
 public:
  SqliteEngine();
  virtual ~SqliteEngine();

  /**
   * Prepare the query to a statement.
   * @param query The query string
   * @return The statement
   */
  sqlite3_stmt *PrepareStatement(std::string query);

  /**
   * Bind the parameters to the statement.
   * @param stmt
   * @param p_params
   */
  void Bind(sqlite3_stmt *stmt, const std::shared_ptr<std::vector<type::TransientValue>> &p_params);

  /**
   * Return the description of the columns.
   * @param stmt
   * @return
   */
  std::vector<std::string> DescribeColumns(sqlite3_stmt *stmt);

  /**
   * Execute a bound statement.
   * @param stmt
   * @return
   */
  ResultSet Execute(sqlite3_stmt *stmt);

 private:
  // SQLite database
  struct sqlite3 *sqlite_db_;
};

}  // namespace terrier::trafficcop
