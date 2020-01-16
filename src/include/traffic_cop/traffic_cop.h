#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "network/postgres/postgres_protocol_utils.h"
#include "parser/create_statement.h"
#include "parser/drop_statement.h"
#include "parser/transaction_statement.h"
#include "storage/recovery/replication_log_provider.h"

namespace terrier::network {
class ConnectionContext;
class PostgresPacketWriter;
}  // namespace terrier::network

namespace terrier::optimizer {
class StatsStorage;
}

namespace terrier::planner {
class AbstractPlanNode;
}

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
             common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage, uint64_t optimizer_timeout)
      : txn_manager_(txn_manager),
        catalog_(catalog),
        replication_log_provider_(replication_log_provider),
        stats_storage_(stats_storage),
        optimizer_timeout_(optimizer_timeout) {}

  virtual ~TrafficCop() = default;

  void ExecuteSimpleQuery(const std::string &simple_query,
                          common::ManagedPointer<network::ConnectionContext> connection_ctx,
                          common::ManagedPointer<network::PostgresPacketWriter> out) const;

  /**
   * Hands a buffer of logs to replication
   * @param buffer buffer containing logs
   */
  void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);

  /**
   * Create a temporary namespace for a connection
   * @param sockfd the socket file descriptor the connection communicates on
   * @param database_name the name of the database the connection is accessing
   * @return a pair of OIDs for the database and the temporary namespace
   */
  std::pair<catalog::db_oid_t, catalog::namespace_oid_t> CreateTempNamespace(network::connection_id_t connection_id,
                                                                             const std::string &database_name);

  /**
   * Drop the temporary namespace for a connection and all enclosing database objects
   * @param ns_oid the OID of the temmporary namespace associated with the connection
   * @param db_oid the OID of the database the connection is accessing
   * @return true if the temporary namespace has been deleted, false otherwise
   */
  bool DropTempNamespace(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid);

  std::unique_ptr<parser::ParseResult> ParseQuery(const std::string &query,
                                                  common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                                  common::ManagedPointer<network::PostgresPacketWriter> out) const;

 private:
  void BeginTransaction(common::ManagedPointer<network::ConnectionContext> connection_ctx) const;
  void EndTransaction(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                      network::QueryType query_type) const;

  void ExecuteStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                        common::ManagedPointer<network::PostgresPacketWriter> out,
                        common::ManagedPointer<parser::ParseResult> parse_result,
                        common::ManagedPointer<parser::SQLStatement> statement,
                        parser::StatementType statement_type) const;

  void ExecuteTransactionStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                   common::ManagedPointer<network::PostgresPacketWriter> out,
                                   parser::TransactionStatement::CommandType type) const;

  void ExecuteCreateStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                              common::ManagedPointer<network::PostgresPacketWriter> out,
                              common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                              parser::CreateStatement::CreateType create_type, bool single_statement_txn) const;

  void ExecuteDropStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                            common::ManagedPointer<network::PostgresPacketWriter> out,
                            common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                            parser::DropStatement::DropType drop_type, bool single_statement_txn) const;

  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  uint64_t optimizer_timeout_;
};

}  // namespace terrier::trafficcop
