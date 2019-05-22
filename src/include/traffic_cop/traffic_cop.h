#pragma once
#include <memory>
#include <string>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "traffic_cop/portal.h"
#include "traffic_cop/sqlite.h"
#include "traffic_cop/statement.h"

namespace terrier::traffic_cop {

/**
 *
 * Traffic Cop of the database. It provides access to all the backend components.
 *
 * *Should be a singleton*
 *
 */

class TrafficCop {
 public:
  virtual ~TrafficCop() = default;

  SqliteEngine *GetExecutionEngine() { return &sqlite_engine; }

 private:
  SqliteEngine sqlite_engine;
};

}  // namespace terrier::traffic_cop
