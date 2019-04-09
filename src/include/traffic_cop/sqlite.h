#pragma once
#include <sqlite3.h>
#include <functional>
#include <memory>
#include <vector>
#include "network/postgres_protocol_utils.h"
#include "traffic_cop/result_set.h"
#include "type/transient_value.h"

struct sqlite3;

namespace terrier::traffic_cop {

/**
 * The sqlite3 execution engine
 */
class SqliteEngine {
 public:
  SqliteEngine();
  virtual ~SqliteEngine();

  /**
   * Execute a simple query.
   * @param query The query string
   * @param out The packet writer
   * @param callback The callback function to accept the results
   */
  void ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                    const network::SimpleQueryCallback &callback);

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
  char *error_msg;

  static int StoreResults(void *result_set_void, int elem_count, char **values, char **column_names);
};

}  // namespace terrier::traffic_cop
