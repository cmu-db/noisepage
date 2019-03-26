#pragma once
#include "network/postgres_protocol_utils.h"
#include "traffic_cop/sqlite.h"

namespace terrier::traffic_cop {

/*
 * Traffic Cop of the database.
 * This is the reception of the backend execution system.
 *
 * *Should be a singleton*
 * */

class TrafficCop {
 public:
  void ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                    const network::SimpleQueryCallback &callback);

 private:
  SqliteEngine sqlite_engine;
};

}  // namespace terrier::traffic_cop
