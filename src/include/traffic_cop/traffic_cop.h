#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "network/network_defs.h"
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
   * @param stats_storage for optimizer calls
   * @param optimizer_timeout for optimizer calls
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

  /**
   * Hands a buffer of logs to replication
   * @param buffer buffer containing logs
   */
  void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);

  /**
   * Create a temporary namespace for a connection
   * @param connection_id the unique connection ID to use for the namespace name
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

  /**
   * @param query SQL string to be parsed
   * @param connection_ctx used to maintain state
   * @param out used to write out results if necessary
   * @return parser's ParseResult, nullptr if failed
   */
  std::unique_ptr<parser::ParseResult> ParseQuery(const std::string &query,
                                                  common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                                  common::ManagedPointer<network::PostgresPacketWriter> out) const;

  /**
   * Given a parsed SQL statement, attempts to bind, optimize, and execute
   * @param connection_ctx used to maintain state
   * @param out used to write out results if necessary
   * @param parse_result parser's valid ParseResult
   * @param query_type type of the query, can be re-derived but should already be known
   */
  void ExecuteStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                        common::ManagedPointer<network::PostgresPacketWriter> out,
                        common::ManagedPointer<parser::ParseResult> parse_result,
                        terrier::network::QueryType query_type) const;

  /**
   * Adjust the TrafficCop's optimizer timeout value (for use by SettingsManager)
   * @param optimizer_timeout time in ms to spend on a task @see optimizer::Optimizer constructor
   */
  void SetOptimizerTimeout(const uint64_t optimizer_timeout) { optimizer_timeout_ = optimizer_timeout; }

 private:
  // Internal method to handle the logic of beginning a txn. Is not responsible for outputting results, only meant to be
  // called by ExecuteTransactionStatement
  void BeginTransaction(common::ManagedPointer<network::ConnectionContext> connection_ctx) const;

  // Internal method to handle the logic of ending a txn. Is not responsible for outputting results, only meant to be
  // called by ExecuteTransactionStatement
  void EndTransaction(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                      network::QueryType query_type) const;

  // Contains the logic to reason about BEGIN, COMMIT, ROLLBACK execution. Responsible for outputting results.
  void ExecuteTransactionStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                   common::ManagedPointer<network::PostgresPacketWriter> out,
                                   terrier::network::QueryType query_type) const;

  // Contains logic to reason about binding, and basic IF EXISTS logic. Responsible for outputting results.
  bool BindStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                     common::ManagedPointer<network::PostgresPacketWriter> out,
                     common::ManagedPointer<parser::ParseResult> parse_result,
                     terrier::network::QueryType query_type) const;

  // Contains the logic to reason about CREATE execution. Responsible for outputting results.
  void ExecuteCreateStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                              common::ManagedPointer<network::PostgresPacketWriter> out,
                              common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                              terrier::network::QueryType query_type, bool single_statement_txn) const;

  // Contains the logic to reason about DROP execution. Responsible for outputting results.
  void ExecuteDropStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                            common::ManagedPointer<network::PostgresPacketWriter> out,
                            common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                            terrier::network::QueryType query_type, bool single_statement_txn) const;

  // Contains the logic to reason about DML execution. Responsible for outputting results.
  void CodegenAndRunPhysicalPlan(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                 common::ManagedPointer<network::PostgresPacketWriter> out,
                                 common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                 terrier::network::QueryType query_type) const;

  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  uint64_t optimizer_timeout_;
};

}  // namespace terrier::trafficcop
