#include <sqlite3.h>
#include <util/test_harness.h>
#include <tcop/tcop.h>

#include "tcop/tcop.h"

namespace terrier{

void TrafficCop::ExecuteQuery(const char *query, SqliteCallback callback) {
  sqlite_engine.ExecuteQuery(query, callback);
}

} // namespace terrier
