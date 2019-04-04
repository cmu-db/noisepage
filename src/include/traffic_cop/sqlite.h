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

class SqliteEngine {
 public:
  SqliteEngine();
  virtual ~SqliteEngine();

  void ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                    const network::SimpleQueryCallback &callback);

  sqlite3_stmt *PrepareStatement(const char *query);

  void Bind(sqlite3_stmt *stmt, const std::shared_ptr<std::vector<type::TransientValue>> &p_params);
  ResultSet Execute(sqlite3_stmt *stmt);

 private:
  // SQLite database
  struct sqlite3 *sqlite_db_;
  char *error_msg;

  static int StoreResults(void *result_set_void, int elem_count, char **values, char **column_names);
};

}  // namespace terrier::traffic_cop
