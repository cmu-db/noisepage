#pragma once
#include "network/postgres_protocol_utils.h"
#include "traffic_cop/sqlite.h"

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
  /**
   * Execute a simple query.
   * @param query the query string
   * @param out the packet writer
   * @param callback the callback function to write back the results
   */
  virtual void ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                    const network::SimpleQueryCallback &callback);

  virtual ~TrafficCop() = default;

 private:
  SqliteEngine sqlite_engine;
};

}  // namespace terrier::traffic_cop
