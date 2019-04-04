#pragma once
#include <memory>
#include <vector>
#include "network/postgres_protocol_utils.h"
#include "traffic_cop/portal.h"
#include "traffic_cop/sqlite.h"
#include "traffic_cop/statement.h"

namespace terrier::traffic_cop {

/**
 *
 * Traffic Cop of the database.
 * This is the reception of the backend execution system.
 *
 * *Should be a singleton*
 *
 */

class TrafficCop {
 public:
  virtual ~TrafficCop() = default;

  /**
   * Execute a simple query.
   * @param query the query string
   * @param out the packet writer
   * @param callback the callback function to write back the results
   */
  virtual void ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                            const network::SimpleQueryCallback &callback);

  virtual Statement Parse(const char *query, const std::vector<type::TypeId> &param_types);

  virtual Portal Bind(const Statement &stmt, const std::shared_ptr<std::vector<type::TransientValue>> &params);

  virtual ResultSet Execute(Portal *portal);

 private:
  SqliteEngine sqlite_engine;
};

}  // namespace terrier::traffic_cop
