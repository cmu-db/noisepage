#include <sqlite3.h>
#include <util/test_harness.h>
#include <traffic_cop/traffic_cop.h>

#include "network/postgres_protocol_utils.h"
#include "traffic_cop/traffic_cop.h"
#include "type/transient_value_factory.h"

namespace terrier::traffic_cop {

void TrafficCop::ExecuteQuery(const char *query, network::PostgresPacketWriter *const out,
                              const network::SimpleQueryCallback &callback) {
  sqlite_engine.ExecuteQuery(query, out, callback);
}

Statement TrafficCop::Parse(const char *query, const std::vector<type::TypeId> &param_types) {

  Statement statement;
  statement.sqlite3_stmt_ = sqlite_engine.PrepareStatement(query);
  statement.param_types = param_types;
  return statement;
}

// With SQLite backend, we only produce a list of param values as the portal,
// because (I think) it is discouraged to copy sqlite3_stmt.
Portal TrafficCop::Bind(const Statement &stmt, const std::vector<type::TransientValue> &params) {
  using namespace type;

  Portal ret;
  return ret;
}

}  // namespace terrier::traffic_cop
