#pragma once
#include <functional>
#include "network/postgres_protocol_utils.h"
#include "traffic_cop/fake_result_set.h"

struct sqlite3;

namespace terrier::traffic_cop {

class SqliteEngine {
 public:
  SqliteEngine();
  virtual ~SqliteEngine();

  void ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                    const network::SimpleQueryCallback &callback);

 private:
  // SQLite database
  struct sqlite3 *sqlite_db_;
  char *error_msg;

  static int StoreResults(void *result_set_void, int elem_count, char **values, char **column_names);
};

}  // namespace terrier::traffic_cop
