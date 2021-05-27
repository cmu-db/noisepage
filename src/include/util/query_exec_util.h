#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "planner/plannodes/output_schema.h"
#include "type/type_id.h"
#include "util/util_defs.h"

namespace noisepage::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace noisepage::transaction

namespace noisepage::parser {
class ConstantValueExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::sql {
struct Val;
}  // namespace noisepage::execution::sql

namespace noisepage::settings {
class SettingsManager;
}  // namespace noisepage::settings

namespace noisepage::metrics {
class MetricsManager;
}  // namespace noisepage::metrics

namespace noisepage::catalog {
class Catalog;
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::optimizer {
class AbstractCostModel;
class StatsStorage;
}  // namespace noisepage::optimizer

namespace noisepage::planner {
class AbstractPlanNode;
}  // namespace noisepage::planner

namespace noisepage::network {
class Statement;
}  // namespace noisepage::network

namespace noisepage::util {

/**
 * Utility class for query execution. This class is not thread-safe.
 *
 * A QueryExecUtil only supports running 1 transaction at a time. If multiple
 * components may run multiple transactions interleaved, each component should
 * then have is own QueryExecUtil for use.
 */
class QueryExecUtil {
 public:
  /**
   * Construct a new QueryExecUtil instance by copying useful members from an old QueryExecUtil instance.
   * @param util    An existing QueryExecUtil whose useful members are to be copied.
   * @return        A new QueryExecUtil instance with the useful members of the old instance copied.
   */
  static std::unique_ptr<util::QueryExecUtil> ConstructThreadLocal(common::ManagedPointer<util::QueryExecUtil> util);

  /** Constructor. */
  QueryExecUtil(common::ManagedPointer<transaction::TransactionManager> txn_manager,
                common::ManagedPointer<catalog::Catalog> catalog,
                common::ManagedPointer<settings::SettingsManager> settings,
                common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout);

  /**
   * Start a new transaction from the viewpoint of the utility.
   * @param db_oid  The OID of the database to execute the transaction in.
   * @param policy  The policy to use for the transaction.
   */
  void BeginTransaction(catalog::db_oid_t db_oid, const transaction::TransactionPolicy &policy);

  /**
   * Instruct this QueryExecUtil instance to utilize the specified transaction.
   * This QueryExecUtil instance must not have already started a transaction.
   *
   * @note  The caller is responsible for invoking UseTransaction(nullptr)
   *        once the transaction no longer requires this utility.
   *
   * @param db_oid  The OID of the database to execute the transaction in.
   * @param txn     The existing transaction to use.
   */
  void UseTransaction(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * End the transaction.
   * @param commit True if the transaction should commit. False if the transaction should abort.
   */
  void EndTransaction(bool commit);

  /**
   * Execute a standalone DDL command.
   * @param query           The DDL command to execute.
   * @param what_if         True if this is a "what-if" API call.
   *                        (e.g., only create an index entry in the catalog without populating it)
   * @return                True if the DDL command was successfully executed.
   */
  bool ExecuteDDL(const std::string &query, bool what_if);

  /**
   * Execute a standalone DML statement.
   * @param query           The DML query to execute.
   * @param params          The query parameters.
   * @param param_types     The types for the query parameters.
   * @param tuple_fn        A function to be called per output row.
   * @param metrics         The metrics manager to use for recording.
   * @param cost            The cost model to use for query planning.
   * @param override_qid    Optional; will use the given query id if specified.
   * @param exec_settings   The execution settings for executing this DML.
   * @return                True if the DML command was succcessfully executed.
   */
  bool ExecuteDML(const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                  common::ManagedPointer<std::vector<type::TypeId>> param_types, TupleFunction tuple_fn,
                  common::ManagedPointer<metrics::MetricsManager> metrics,
                  std::unique_ptr<optimizer::AbstractCostModel> cost, std::optional<execution::query_id_t> override_qid,
                  const execution::exec::ExecutionSettings &exec_settings);

  /**
   * Compile the given statement and cache the resulting plan.
   * @param statement       The statement to compile (serves as unique identifier).
   * @param params          Placeholder parameters for the query.
   * @param param_types     The types for the query parameters.
   * @param cost            The cost model to use for compilation.
   * @param override_qid    Optional; will use the given query ID if specified.
   * @param exec_settings   The execution settings for compiling this DML.
   * @return                True if compilation succeeded. False otherwise.
   */
  bool CompileQuery(const std::string &statement,
                    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                    common::ManagedPointer<std::vector<type::TypeId>> param_types,
                    std::unique_ptr<optimizer::AbstractCostModel> cost,
                    std::optional<execution::query_id_t> override_qid,
                    const execution::exec::ExecutionSettings &exec_settings);

  /**
   * Execute the specified pre-compiled query.
   * @param statement       Previously compiled query statement (serves as identifier).
   * @param tuple_fn        Function to be invoked per output row.
   * @param params          The query parameters.
   * @param metrics         The metrics manager to use for recording.
   * @param exec_settings   The execution settings for executing this query.
   * @return                True if the query was successfully executed. False otherwise.
   */
  bool ExecuteQuery(const std::string &statement, TupleFunction tuple_fn,
                    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                    common::ManagedPointer<metrics::MetricsManager> metrics,
                    const execution::exec::ExecutionSettings &exec_settings);

  /** @return The compiled query for the specified statement. */
  common::ManagedPointer<execution::compiler::ExecutableQuery> GetExecutableQuery(const std::string &statement) {
    return common::ManagedPointer(exec_queries_[statement]);
  }

  /**
   * Plan the specified query.
   * @param query       Query to be planned.
   * @param params      Placeholder parameters for the query plan.
   * @param param_types Types of query parameters.
   * @param cost        The cost model to use in planning the query.
   * @return            The resulting Statement, which includes the optimized plan.
   */
  std::unique_ptr<network::Statement> PlanStatement(
      const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
      common::ManagedPointer<std::vector<type::TypeId>> param_types,
      std::unique_ptr<optimizer::AbstractCostModel> cost);

  /**
   * Remove the cached query plan for the specified query.
   * @param query The query to invalidate.
   */
  void ClearPlan(const std::string &query);

  /** Erase all cached plans */
  void ClearPlans();

  /** @return The most recent error message. Should only be invoked if PlanStatement or Execute failed. */
  std::string GetError() { return error_msg_; }

  /** @return The database being accessed. */
  catalog::db_oid_t GetDatabaseOid() const { return db_oid_; }

 private:
  void ResetError();
  void SetDatabase(catalog::db_oid_t db_oid);

  common::ManagedPointer<transaction::TransactionManager> txn_manager_;  ///< Transaction manager to be used.
  common::ManagedPointer<catalog::Catalog> catalog_;                     ///< Catalog to be used.
  common::ManagedPointer<settings::SettingsManager> settings_;           ///< Settings manager to be used.
  common::ManagedPointer<optimizer::StatsStorage> stats_;                ///< Statistics to be used.
  uint64_t optimizer_timeout_;                                           ///< The timeout in the optimizer.

  catalog::db_oid_t db_oid_;                        ///< The database being accessed.
  bool own_txn_ = false;                            ///< True if QueryExecUtil owns its txn_ object.
  transaction::TransactionContext *txn_ = nullptr;  ///< The transaction in which queries are executed.

  /**
   * Cached query information.
   * @warning Assumes that the query string is a unique identifier.
   */
  //@{
  std::unordered_map<std::string, std::unique_ptr<planner::OutputSchema>> schemas_;
  std::unordered_map<std::string, std::unique_ptr<execution::compiler::ExecutableQuery>> exec_queries_;
  //@}

  std::string error_msg_;  ///< The most recently encountered error.
};

}  // namespace noisepage::util
