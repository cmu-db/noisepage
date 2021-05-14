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
#include "type/type_id.h"

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
class OutputSchema;
}  // namespace noisepage::planner

namespace noisepage::network {
class Statement;
}  // namespace noisepage::network

namespace noisepage::util {

/**
 * Signature of a function that is capable of processing rows retrieved
 * from ExecuteDML or ExecuteQuery. This function is invoked once per
 * row, with the argument being a row's attributes.
 */
using TupleFunction = std::function<void(const std::vector<execution::sql::Val *> &)>;

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
   * Construct a copy of useful members state.
   * This allows creating another QueryExecUtil from an existing one.
   */
  static std::unique_ptr<util::QueryExecUtil> ConstructThreadLocal(common::ManagedPointer<util::QueryExecUtil> util);

  /**
   * Construct a QueryExecUtil
   *
   * @param txn_manager Transaction manager
   * @param catalog Catalog
   * @param settings Settings manager
   * @param stats Stats storage
   * @param optimizer_timeout Timeout for optimizer
   */
  QueryExecUtil(common::ManagedPointer<transaction::TransactionManager> txn_manager,
                common::ManagedPointer<catalog::Catalog> catalog,
                common::ManagedPointer<settings::SettingsManager> settings,
                common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout);

  /**
   * Starts a new transaction from the utility's viewpoint.
   * @param db_oid Database OID to use (INVALID_DATABASE_OID for default)
   */
  void BeginTransaction(catalog::db_oid_t db_oid);

  /**
   * Instructs the utility to utilize the specified transaction.
   * A transaction must not already be started.
   *
   * @note It is the caller's responsibility to invoke UseTransaction(nullptr)
   * once the transaction no longer requires this utility.
   *
   * @param db_oid Database OID to use (INVALID_DATABASE_OID for default)
   * @param txn Transaction to use
   */
  void UseTransaction(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Set external execution settings to adopt
   * @param exec_settings Settings to adopt
   */
  void SetExecutionSettings(execution::exec::ExecutionSettings exec_settings);

  /**
   * End the transaction
   * @param commit Commit or abort
   */
  void EndTransaction(bool commit);

  /**
   * Execute a standalone DDL
   * @param query DDL query to execute
   * @param what_if whether this is a "what-if" API call (e.g., only create the index entry in the catalog without
   * populating it)
   * @return true if success
   */
  bool ExecuteDDL(const std::string &query, bool what_if);

  /**
   * Execute a standalone DML statement
   * @param query DML query to execute
   * @param params query parameters to utilize
   * @param param_types Types of query parameters
   * @param tuple_fn A function to be called per row
   * @param metrics Metrics manager to use for recording
   * @param cost Cost model to use
   * @param override_qid Optional describing how to override query's id
   * @param exec_settings ExecutionSettings to utilize
   * @return true if success
   */
  bool ExecuteDML(const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                  common::ManagedPointer<std::vector<type::TypeId>> param_types, TupleFunction tuple_fn,
                  common::ManagedPointer<metrics::MetricsManager> metrics,
                  std::unique_ptr<optimizer::AbstractCostModel> cost, std::optional<execution::query_id_t> override_qid,
                  const execution::exec::ExecutionSettings &exec_settings);

  /**
   * Compiles a query and caches the resultant plan
   * @param statement Statement to compile (serves as unique identifier)
   * @param params placeholder parameters for query
   * @param param_types Types of the query parameters
   * @param cost cost model to use for compilation
   * @param override_qid Optional describing how to override query's id
   * @param exec_settings ExecutionSettings to use for compiling
   * @return whether compilation succeeded or not
   */
  bool CompileQuery(const std::string &statement,
                    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                    common::ManagedPointer<std::vector<type::TypeId>> param_types,
                    std::unique_ptr<optimizer::AbstractCostModel> cost,
                    std::optional<execution::query_id_t> override_qid,
                    const execution::exec::ExecutionSettings &exec_settings);

  /**
   * Executes a pre-compiled query
   * @param statement Previously compiled query statement (serves as identifier)
   * @param tuple_fn Per-row function invoked during output
   * @param params Parameters to use for execution
   * @param metrics Metrics manager to use for recording
   * @param exec_settings ExecutionSettings to use for executing
   * @return true if success
   */
  bool ExecuteQuery(const std::string &statement, TupleFunction tuple_fn,
                    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                    common::ManagedPointer<metrics::MetricsManager> metrics,
                    const execution::exec::ExecutionSettings &exec_settings);

  /**
   * Get ExecutableQuery
   * @param statement Previously compiled query statement (serves as identifier)
   * @return compiled query statement or nullptr if haven't compiled
   */
  common::ManagedPointer<execution::compiler::ExecutableQuery> GetExecutableQuery(const std::string &statement) {
    return common::ManagedPointer(exec_queries_[statement]);
  }

  /**
   * Plans a query
   * @param query Statement to plan
   * @param params Placeholder parameters for query plan
   * @param param_types Types of query parameters
   * @param cost Cost model to use
   * @return the result Statement including the optimized plan
   */
  std::unique_ptr<network::Statement> PlanStatement(
      const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
      common::ManagedPointer<std::vector<type::TypeId>> param_types,
      std::unique_ptr<optimizer::AbstractCostModel> cost);

  /**
   * Remove a cached query plan
   * @param query Query to invalidate
   */
  void ClearPlan(const std::string &query);

  /** Erases all cached plans */
  void ClearPlans();

  /**
   * Returns the most recent error message.
   * Should only be invoked if PlanStatement or Execute has failed.
   */
  std::string GetError() { return error_msg_; }

 private:
  void ResetError();
  void SetDatabase(catalog::db_oid_t db_oid);

  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<settings::SettingsManager> settings_;
  common::ManagedPointer<optimizer::StatsStorage> stats_;
  uint64_t optimizer_timeout_;

  /** Database being accessed */
  catalog::db_oid_t db_oid_{catalog::INVALID_DATABASE_OID};
  bool own_txn_ = false;
  transaction::TransactionContext *txn_ = nullptr;

  /**
   * Information about cached executable queries
   * Assumes that the query string is a unique identifier.
   */
  std::unordered_map<std::string, std::unique_ptr<planner::OutputSchema>> schemas_;
  std::unordered_map<std::string, std::unique_ptr<execution::compiler::ExecutableQuery>> exec_queries_;

  /**
   * Stores the most recently encountered error.
   */
  std::string error_msg_;
};

}  // namespace noisepage::util
