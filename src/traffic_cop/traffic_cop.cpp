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
Portal TrafficCop::Bind(const Statement &stmt, const std::shared_ptr<std::vector<type::TransientValue>> &params) {
  Portal ret;
  ret.sqlite_stmt_ = stmt.sqlite3_stmt_;
  ret.params = params;
  return ret;
}
ResultSet TrafficCop::Execute(Portal &portal) {
  sqlite_engine.Bind(portal.sqlite_stmt_, portal.params);
  return sqlite_engine.Execute(portal.sqlite_stmt_);

}

}  // namespace terrier::traffic_cop
