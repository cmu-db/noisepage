#pragma once
#include <memory>
#include <string>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "traffic_cop/portal.h"
#include "traffic_cop/sqlite.h"
#include "traffic_cop/statement.h"

namespace terrier::trafficcop {

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

  /**
   * Returns the execution engine.
   * @return the execution engine.
   */
  SqliteEngine *GetExecutionEngine() { return &sqlite_engine_; }

 private:
  SqliteEngine sqlite_engine_;
};

}  // namespace terrier::trafficcop
