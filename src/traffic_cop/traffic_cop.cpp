#include <sqlite3.h>
#include <util/test_harness.h>

#include "traffic_cop/traffic_cop.h"

namespace terrier{

void TrafficCop::ExecuteQuery(const char *query, SqliteCallback callback) {
  sqlite_engine.ExecuteQuery(query, callback);
}

} // namespace terrier
