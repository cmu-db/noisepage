#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
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
   * @param txn_manager the transaction manager of the system
   * @param catalog the catalog of the system
   * @param replication_log_provider if given, the tcop will forward replication logs to this provider
   */
  TrafficCop(common::ManagedPointer<transaction::TransactionManager> txn_manager,
             common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider = DISABLED)
      : txn_manager_(txn_manager), catalog_(catalog), replication_log_provider_(replication_log_provider) {}

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
  void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);

  /**
   * Create a temporary namespace for a connection
   * @param sockfd the socket file descriptor the connection communicates on
   * @param database_name the name of the database the connection is accessing
   */
  std::pair<catalog::db_oid_t, catalog::namespace_oid_t> CreateTempNamespace(int sockfd,
                                                                             std::string database_name = catalog::DEFAULT_DATABASE);

  /**
   * Drop the temporary namespace for a connection and all enclosing database objects
   * @param ns_oid the OID of the temmporary namespace associated with the connection
   * @param db_oid the OID of the database the connection is accessing
   */
  void DropTempNamespace(catalog::namespace_oid_t ns_oid, catalog::db_oid_t db_oid = catalog::INVALID_DATABASE_OID);

 private:
  SqliteEngine sqlite_engine_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
};

}  // namespace terrier::trafficcop
