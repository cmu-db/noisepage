#include <sqlite3.h>
#include <util/test_harness.h>

#include "traffic_cop/traffic_cop.h"

namespace terrier::traffic_cop{

void TrafficCop::ExecuteQuery(const char *query, std::function<void(FakeResultSet &)> &callback) {
  sqlite_engine.ExecuteQuery(query, callback);
}

} // namespace terrier
