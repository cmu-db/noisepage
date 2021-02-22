#pragma once

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_settings.h"
#include "type/type_id.h"

namespace noisepage::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace noisepage::transaction

namespace noisepage::parser {
class ConstantValueExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::compiler {
class ExecutableQuery;
}  // namespace noisepage::execution::compiler

namespace noisepage::execution::sql {
class Val;
}  // namespace noisepage::execution::compiler

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

using TupleFunction = std::function<void(const std::vector<execution::sql::Val *> &)>;

/**
 * Utility class for query execution
 */
class QueryExecUtil {
 public:
  QueryExecUtil(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionManager> txn_manager,
                common::ManagedPointer<catalog::Catalog> catalog,
                common::ManagedPointer<settings::SettingsManager> settings,
                common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout);

  void UseTransaction(common::ManagedPointer<transaction::TransactionContext> txn);
  void BeginTransaction();
  void SetCostModelFunction(std::function<std::unique_ptr<optimizer::AbstractCostModel>()> func);
  void SetDatabase(catalog::db_oid_t db_oid);
  void SetExecutionSettings(execution::exec::ExecutionSettings exec_settings);
  void EndTransaction(bool commit);

  bool ExecuteDDL(const std::string &statement);
  bool ExecuteDML(const std::string &statement,
                  common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                  common::ManagedPointer<std::vector<type::TypeId>> param_types, TupleFunction tuple_fn,
                  common::ManagedPointer<metrics::MetricsManager> metrics);

  size_t CompileQuery(const std::string &statement,
                      common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                      common::ManagedPointer<std::vector<type::TypeId>> param_types);

  bool ExecuteQuery(size_t idx, TupleFunction tuple_fn,
                    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                    common::ManagedPointer<metrics::MetricsManager> metrics);

  std::pair<std::unique_ptr<network::Statement>, std::unique_ptr<planner::AbstractPlanNode>> PlanStatement(
      const std::string &statement, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
      common::ManagedPointer<std::vector<type::TypeId>> param_types);

  void ClearPlans();

 private:
  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<settings::SettingsManager> settings_;
  common::ManagedPointer<optimizer::StatsStorage> stats_;
  uint64_t optimizer_timeout_;

  bool own_txn_ = false;
  std::function<std::unique_ptr<optimizer::AbstractCostModel>()> cost_func_;
  transaction::TransactionContext *txn_ = nullptr;

  std::vector<std::unique_ptr<planner::OutputSchema>> schemas_;
  std::vector<std::unique_ptr<execution::compiler::ExecutableQuery>> exec_queries_;

  execution::exec::ExecutionSettings exec_settings_;
};

}  // namespace noisepage::util
