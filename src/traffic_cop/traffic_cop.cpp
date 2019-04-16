#include <sqlite3.h>
#include <util/test_harness.h>
#include <memory>
#include <vector>
#include <string>

#include "network/postgres_protocol_utils.h"
#include "traffic_cop/traffic_cop.h"
#include "type/transient_value_factory.h"

namespace terrier::traffic_cop {

void TrafficCop::ExecuteQuery(const char *query, network::PostgresPacketWriter *const out,
                              const network::SimpleQueryCallback &callback) {
  sqlite_engine.ExecuteQuery(query, out, callback);
}

Statement TrafficCop::Parse(const std::string &query, const std::vector<network::PostgresValueType> &param_types) {
  Statement statement(sqlite_engine.PrepareStatement(query), param_types);
  return statement;
}

// With SQLite backend, we only produce a list of param values as the portal,
// because we cannot copy a sqlite3 statement.
Portal TrafficCop::Bind(const Statement &stmt, const std::shared_ptr<std::vector<type::TransientValue>> &params) {
  Portal ret;
  ret.sqlite_stmt_ = stmt.sqlite3_stmt_;
  ret.params = params;
  return ret;
}

std::vector<std::string> TrafficCop::DescribeColumns(const Statement &stmt) {
  return sqlite_engine.DescribeColumns(stmt.sqlite3_stmt_);
}

std::vector<std::string> TrafficCop::DescribeColumns(const Portal &portal) {
  return sqlite_engine.DescribeColumns(portal.sqlite_stmt_);
}

ResultSet TrafficCop::Execute(Portal *portal) {
  sqlite_engine.Bind(portal->sqlite_stmt_, portal->params);
  return sqlite_engine.Execute(portal->sqlite_stmt_);
}

}  // namespace terrier::traffic_cop
