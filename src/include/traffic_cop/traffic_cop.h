#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "storage/recovery/replication_log_provider.h"
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
  /**
   * @param replication_log_provider if given, the tcop will forward replication logs to this provider
   */
  explicit TrafficCop(common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider = DISABLED)
      : replication_log_provider_(replication_log_provider) {}

  virtual ~TrafficCop() = default;

  /**
   * Returns the execution engine.
   * @return the execution engine.
   */
  SqliteEngine *GetExecutionEngine() { return &sqlite_engine_; }

  /**
   * Hands a buffer of logs to replication
   * @param buffer buffer containing logs
   */
  void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer) {
    TERRIER_ASSERT(replication_log_provider_ != DISABLED,
                   "Should not be handing off logs if no log provider was given");
    replication_log_provider_->HandBufferToReplication(std::move(buffer));
  }

 private:
  SqliteEngine sqlite_engine_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
};

}  // namespace terrier::trafficcop
