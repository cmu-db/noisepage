#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/managed_pointer.h"
#include "network/network_defs.h"
#include "network/postgres/postgres_protocol_utils.h"
#include "parser/create_statement.h"
#include "parser/drop_statement.h"
#include "parser/transaction_statement.h"
#include "storage/recovery/replication_log_provider.h"
#include "traffic_cop/traffic_cop_defs.h"

namespace terrier::network {
class ConnectionContext;
class PostgresPacketWriter;
class Statement;
class Portal;
}  // namespace terrier::network

namespace terrier::optimizer {
class StatsStorage;
}

namespace terrier::planner {
class AbstractPlanNode;
}

namespace terrier::trafficcop {

/**
 * The TrafficCop acts as a translation layer between protocol implementations at at the front-end and execution of
 * queries in the back-end. We strive to encapsulate protocol-agnostic behavior at this layer (i.e. nothing
 * Postgres-specific). Anything protocol specific should be done at the network's protocol interpreter or command
 * processing layers.
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
   * @return parser's ParseResult, nullptr if failed
   */
  std::unique_ptr<parser::ParseResult> ParseQuery(
      const std::string &query, common::ManagedPointer<network::ConnectionContext> connection_ctx) const;

  /**
   * @param connection_ctx context containg txn and catalog accessor to be used
   * @param query bound ParseResult
   * @return physical plan that can be executed
   */
  std::unique_ptr<planner::AbstractPlanNode> OptimizeBoundQuery(
      common::ManagedPointer<network::ConnectionContext> connection_ctx,
      common::ManagedPointer<parser::ParseResult> query) const;

  /**
   * Calls to txn manager to begin txn, and updates ConnectionContext state
   * @param connection_ctx context to own this txn
   */
  void BeginTransaction(common::ManagedPointer<network::ConnectionContext> connection_ctx) const;

  /**
   * Calls to txn manager to end txn, and updates ConnectionContext state
   * @param connection_ctx context to release its txn
   * @param query_type if the txn is being ended with COMMIT or ROLLBACK
   */
  void EndTransaction(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                      network::QueryType query_type) const;

  /**
   * Contains the logic to reason about BEGIN, COMMIT, ROLLBACK execution. Responsible for outputting results, since we
   * need to be able to do more than return a single TrafficCopResult (i.e. we may need a NOTICE and a COMPLETE)
   * @param connection_ctx context to be modified by changing txn state
   * @param out packet writer for writing results
   * @param explicit_txn_block true if in a txn from BEGIN, false otherwise
   * @param query_type BEGIN, COMMIT, or ROLLBACK
   */
  void ExecuteTransactionStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                   common::ManagedPointer<network::PostgresPacketWriter> out, bool explicit_txn_block,
                                   terrier::network::QueryType query_type) const;

  /**
   * Contains logic to reason about binding, and basic IF EXISTS logic.
   * @param connection_ctx context to be used to access the internal txn
   * @param statement parse result to be bound
   * @return result of the operation
   */
  TrafficCopResult BindQuery(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                             const common::ManagedPointer<network::Statement> statement) const;

  /**
   * Contains the logic to reason about CREATE execution.
   * @param connection_ctx
   * @param physical_plan
   * @param query_type
   * @return result of the operation
   */
  TrafficCopResult ExecuteCreateStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                          common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                          terrier::network::QueryType query_type) const;

  /**
   * Contains the logic to reason about DROP execution.
   * @param connection_ctx
   * @param physical_plan
   * @param query_type
   * @return result of the operation
   */
  TrafficCopResult ExecuteDropStatement(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                        common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                        terrier::network::QueryType query_type) const;

  /**
   * Contains the logic to reason about DML execution. Responsible for outputting results because we don't want to
   * (can't) stick it in TrafficCopResult.
   * @param connection_ctx
   * @param out
   * @param portal
   * @return result of the operation
   */
  TrafficCopResult CodegenAndRunPhysicalPlan(common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                             common::ManagedPointer<network::PostgresPacketWriter> out,
                                             common::ManagedPointer<network::Portal> portal) const;

  /**
   * Adjust the TrafficCop's optimizer timeout value (for use by SettingsManager)
   * @param optimizer_timeout time in ms to spend on a task @see optimizer::Optimizer constructor
   */
  void SetOptimizerTimeout(const uint64_t optimizer_timeout) { optimizer_timeout_ = optimizer_timeout; }

 private:
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  uint64_t optimizer_timeout_;
};

}  // namespace terrier::trafficcop
