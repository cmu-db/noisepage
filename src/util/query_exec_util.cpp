#include "util/query_exec_util.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/ddl_executors.h"
#include "execution/vm/vm_defs.h"
#include "loggers/common_logger.h"
#include "metrics/metrics_manager.h"
#include "network/network_defs.h"
#include "network/network_util.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "parser/variable_set_statement.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace noisepage::util {

std::unique_ptr<util::QueryExecUtil> QueryExecUtil::ConstructThreadLocal(
    common::ManagedPointer<util::QueryExecUtil> util) {
  return std::make_unique<util::QueryExecUtil>(util->txn_manager_, util->catalog_, util->settings_, util->stats_,
                                               util->optimizer_timeout_);
}

QueryExecUtil::QueryExecUtil(common::ManagedPointer<transaction::TransactionManager> txn_manager,
                             common::ManagedPointer<catalog::Catalog> catalog,
                             common::ManagedPointer<settings::SettingsManager> settings,
                             common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout)
    : txn_manager_(txn_manager),
      catalog_(catalog),
      settings_(settings),
      stats_(stats),
      optimizer_timeout_(optimizer_timeout) {}

void QueryExecUtil::ClearPlans() {
  schemas_.clear();
  exec_queries_.clear();
}

void QueryExecUtil::ClearPlan(const std::string &query) {
  schemas_.erase(query);
  exec_queries_.erase(query);
}

void QueryExecUtil::SetDatabase(catalog::db_oid_t db_oid) {
  if (db_oid != catalog::INVALID_DATABASE_OID) {
    db_oid_ = db_oid;
  } else {
    auto *txn = txn_manager_->BeginTransaction();
    db_oid_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

void QueryExecUtil::UseTransaction(catalog::db_oid_t db_oid,
                                   common::ManagedPointer<transaction::TransactionContext> txn) {
  NOISEPAGE_ASSERT(!own_txn_, "QueryExecUtil already using a transaction");
  SetDatabase(db_oid);
  txn_ = txn.Get();
  own_txn_ = false;
}

void QueryExecUtil::BeginTransaction(catalog::db_oid_t db_oid) {
  NOISEPAGE_ASSERT(txn_ == nullptr, "Nesting transactions not supported");
  SetDatabase(db_oid);
  txn_ = txn_manager_->BeginTransaction();
  own_txn_ = true;
}

void QueryExecUtil::EndTransaction(bool commit) {
  NOISEPAGE_ASSERT(txn_ != nullptr, "Transaction has not started");
  NOISEPAGE_ASSERT(own_txn_, "EndTransaction can only be called on an owned transaction");
  if (commit)
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  else
    txn_manager_->Abort(txn_);
  txn_ = nullptr;
  own_txn_ = false;
}

std::pair<std::unique_ptr<network::Statement>, std::unique_ptr<planner::AbstractPlanNode>> QueryExecUtil::PlanStatement(
    const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
    common::ManagedPointer<std::vector<type::TypeId>> param_types, std::unique_ptr<optimizer::AbstractCostModel> cost) {
  NOISEPAGE_ASSERT(txn_ != nullptr, "Transaction must have been started");
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);

  std::unique_ptr<network::Statement> statement;
  try {
    std::string query_tmp = query;
    auto parse_tree = parser::PostgresParser::BuildParseTree(query_tmp);
    statement = std::make_unique<network::Statement>(std::move(query_tmp), std::move(parse_tree));
  } catch (std::exception &e) {
    // Catched a parsing error
    COMMON_LOG_ERROR("QueryExecUtil::PlanStatement caught error {} when parsing {}", e.what(), query);
    return {nullptr, nullptr};
  }

  // If QUERY_SET can be run through the optimizer and/or a executor,
  // then we don't need to do this special case here.
  if (statement->GetQueryType() == network::QueryType::QUERY_SET) {
    return std::make_pair(std::move(statement), nullptr);
  }

  try {
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
    binder.BindNameToNode(statement->ParseResult(), params, param_types);
  } catch (std::exception &e) {
    // Caught a binding exception
    COMMON_LOG_ERROR("QueryExecUtil::PlanStatement caught error {} when binding {}", e.what(), query);
    return {nullptr, nullptr};
  }

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(txn, common::ManagedPointer(accessor), statement->ParseResult(),
                                                       db_oid_, stats_, std::move(cost), optimizer_timeout_, params)
                      ->TakePlanNodeOwnership();
  return std::make_pair(std::move(statement), std::move(out_plan));
}

bool QueryExecUtil::ExecuteDDL(const std::string &query) {
  NOISEPAGE_ASSERT(txn_ != nullptr, "Requires BeginTransaction() or UseTransaction()");
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto result = PlanStatement(query, nullptr, nullptr, std::make_unique<optimizer::TrivialCostModel>());
  const std::unique_ptr<network::Statement> &statement = result.first;
  const std::unique_ptr<planner::AbstractPlanNode> &out_plan = result.second;
  NOISEPAGE_ASSERT(!network::NetworkUtil::DMLQueryType(statement->GetQueryType()), "ExecuteDDL expects DDL statement");

  // Handle SET queries
  bool status = true;
  if (statement->GetQueryType() == network::QueryType::QUERY_SET) {
    const auto &set_stmt = statement->RootStatement().CastManagedPointerTo<parser::VariableSetStatement>();
    settings_->SetParameter(set_stmt->GetParameterName(), set_stmt->GetValues());
    status = true;
  } else {
    switch (statement->GetQueryType()) {
      case network::QueryType::QUERY_CREATE_TABLE:
        status = execution::sql::DDLExecutors::CreateTableExecutor(
            common::ManagedPointer<planner::CreateTablePlanNode>(
                reinterpret_cast<planner::CreateTablePlanNode *>(out_plan.get())),
            common::ManagedPointer(accessor), db_oid_);
        break;
      case network::QueryType::QUERY_DROP_INDEX:
        // Drop index does not need execution of compiled query
        status =
            execution::sql::DDLExecutors::DropIndexExecutor(common::ManagedPointer<planner::AbstractPlanNode>(out_plan)
                                                                .CastManagedPointerTo<planner::DropIndexPlanNode>(),
                                                            common::ManagedPointer<catalog::CatalogAccessor>(accessor));
        break;
      case network::QueryType::QUERY_CREATE_INDEX:
        status = execution::sql::DDLExecutors::CreateIndexExecutor(
            common::ManagedPointer<planner::AbstractPlanNode>(out_plan)
                .CastManagedPointerTo<planner::CreateIndexPlanNode>(),
            common::ManagedPointer<catalog::CatalogAccessor>(accessor));
        if (status) {
          execution::exec::ExecutionSettings settings{};
          if (CompileQuery(query, nullptr, nullptr, std::make_unique<optimizer::TrivialCostModel>(), false,
                           execution::query_id_t(0), settings)) {
            ExecuteQuery(query, nullptr, nullptr, nullptr, settings);
          }
        }
        break;
      default:
        NOISEPAGE_ASSERT(false, "Unsupported QueryExecUtil::ExecuteStatement");
        break;
    }
  }

  return status;
}

bool QueryExecUtil::CompileQuery(const std::string &statement,
                                 common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                                 common::ManagedPointer<std::vector<type::TypeId>> param_types,
                                 std::unique_ptr<optimizer::AbstractCostModel> cost, bool override_qid,
                                 execution::query_id_t override_qid_target,
                                 const execution::exec::ExecutionSettings &exec_settings) {
  if (exec_queries_.find(statement) != exec_queries_.end()) {
    // We have already optimized and compiled this query before
    return true;
  }

  NOISEPAGE_ASSERT(txn_ != nullptr, "Requires BeginTransaction() or UseTransaction()");
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto result = PlanStatement(statement, params, param_types, std::move(cost));
  if (!result.first || !result.second) {
    return false;
  }

  const std::unique_ptr<planner::AbstractPlanNode> &out_plan = result.second;
  NOISEPAGE_ASSERT(network::NetworkUtil::DMLQueryType(result.first->GetQueryType()), "ExecuteDML expects DML");
  common::ManagedPointer<planner::OutputSchema> schema = out_plan->GetOutputSchema();

  auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                     execution::compiler::CompilationMode::OneShot,
                                                                     override_qid, override_qid_target);
  schemas_[statement] = schema->Copy();
  exec_queries_[statement] = std::move(exec_query);
  return true;
}

bool QueryExecUtil::ExecuteQuery(const std::string &statement, TupleFunction tuple_fn,
                                 common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                                 common::ManagedPointer<metrics::MetricsManager> metrics,
                                 const execution::exec::ExecutionSettings &exec_settings) {
  NOISEPAGE_ASSERT(exec_queries_.find(statement) != exec_queries_.end(), "Cached query not found");
  NOISEPAGE_ASSERT(txn_ != nullptr, "Requires BeginTransaction() or UseTransaction()");
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  planner::OutputSchema *schema = schemas_[statement].get();
  auto consumer = [&tuple_fn, schema](byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
    if (tuple_fn != nullptr) {
      for (uint32_t row = 0; row < num_tuples; row++) {
        uint32_t curr_offset = 0;
        std::vector<execution::sql::Val *> vals;
        for (const auto &col : schema->GetColumns()) {
          auto alignment = execution::sql::ValUtil::GetSqlAlignment(col.GetType());
          if (!common::MathUtil::IsAligned(curr_offset, alignment)) {
            curr_offset = static_cast<uint32_t>(common::MathUtil::AlignTo(curr_offset, alignment));
          }

          auto *val = reinterpret_cast<execution::sql::Val *>(tuples + row * tuple_size + curr_offset);
          vals.emplace_back(val);
          curr_offset += execution::sql::ValUtil::GetSqlSize(col.GetType());
        }

        tuple_fn(vals);
      }
    }
  };

  // Create ExecutionContext with no metrics to prevent recording
  execution::exec::OutputCallback callback = consumer;
  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, txn, callback, schema, common::ManagedPointer(accessor), exec_settings, metrics, nullptr);

  exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(params.Get()));

  exec_queries_[statement]->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  return true;
}

bool QueryExecUtil::ExecuteDML(const std::string &query,
                               common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                               common::ManagedPointer<std::vector<type::TypeId>> param_types, TupleFunction tuple_fn,
                               common::ManagedPointer<metrics::MetricsManager> metrics,
                               std::unique_ptr<optimizer::AbstractCostModel> cost, bool override_qid,
                               execution::query_id_t override_qid_target,
                               const execution::exec::ExecutionSettings &exec_settings) {
  if (!CompileQuery(query, params, param_types, std::move(cost), override_qid, override_qid_target, exec_settings)) {
    return false;
  }

  return ExecuteQuery(query, std::move(tuple_fn), params, metrics, exec_settings);
}

}  // namespace noisepage::util
