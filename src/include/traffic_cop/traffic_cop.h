#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "parser/postgresparser.h"
#include "storage/recovery/replication_log_provider.h"
#include "traffic_cop/portal.h"
#include "traffic_cop/sqlite.h"
#include "traffic_cop/statement.h"

namespace terrier::trafficcop {

class TerrierEngine;

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
   * @param terrier_engine Terrier execution engine
   * @param replication_log_provider if given, the tcop will forward replication logs to this provider
   */
  explicit TrafficCop(catalog::db_oid_t default_database_oid, common::ManagedPointer<TerrierEngine> terrier_engine,
                      common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider = DISABLED)
      : default_database_oid_(default_database_oid),
        terrier_engine_(terrier_engine),
        replication_log_provider_(replication_log_provider) {}

  virtual ~TrafficCop() = default;

  /** @return the SQLite execution engine */
  SqliteEngine *GetSqliteEngine() { return &sqlite_engine_; }

  /** @return the Terrier execution engine */
  common::ManagedPointer<TerrierEngine> GetTerrierEngine() { return terrier_engine_; }

  /** @return the default database oid */
  catalog::db_oid_t GetDefaultDatabaseOid() { return default_database_oid_; }

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
  catalog::db_oid_t default_database_oid_;
  common::ManagedPointer<TerrierEngine> terrier_engine_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
};

}  // namespace terrier::trafficcop
