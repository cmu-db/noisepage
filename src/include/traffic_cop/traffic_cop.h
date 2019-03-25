#pragma once
#include "sqlite.h"
#include "network/postgres_protocol_utils.h"

namespace terrier::traffic_cop {

/*
 * Traffic Cop of the database.
 * This is the reception of the backend execution system.
 *
 * *Should be a singleton*
 * */

class TrafficCop {

 public:
  void ExecuteQuery(const char *query,
                      network::PostgresPacketWriter *out,
                      std::function<void(FakeResultSet & , network::PostgresPacketWriter * )> &callback);

 private:
  SqliteEngine sqlite_engine;

};

}

