#include <sqlite3.h>
#include <util/test_harness.h>

#include "network/postgres_protocol_utils.h"
#include "traffic_cop/traffic_cop.h"

namespace terrier::traffic_cop {

void TrafficCop::ExecuteQuery(const char *query, network::PostgresPacketWriter *const out,
                              const network::SimpleQueryCallback &callback) {
  sqlite_engine.ExecuteQuery(query, out, callback);
}

}  // namespace terrier::traffic_cop
