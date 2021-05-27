#include "task/task.h"

#include <utility>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "parser/expression/constant_value_expression.h"
#include "task/task_manager.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

Task::Task(const catalog::db_oid_t db_oid, std::string &&query_text, const transaction::TransactionPolicy policy,
           const common::ManagedPointer<common::FutureDummy> sync)
    : db_oid_(db_oid), query_text_(std::move(query_text)), policy_(policy), sync_(sync) {}

std::unique_ptr<TaskDDL> TaskDDL::Builder::Build() {
  NOISEPAGE_ASSERT(db_oid_.has_value(), "Database OID must be specified.");
  NOISEPAGE_ASSERT(*db_oid_ != catalog::INVALID_DATABASE_OID, "You're doing something hacky. Get a real database OID.");
  NOISEPAGE_ASSERT(query_text_.has_value(), "Query text must be specified.");
  return std::unique_ptr<TaskDDL>(new TaskDDL(*db_oid_, std::move(*query_text_), policy_, sync_));
}

std::unique_ptr<TaskDML> TaskDML::Builder::Build() {
  // Sanity checks.
  NOISEPAGE_ASSERT(db_oid_.has_value(), "Database OID must be specified.");
  NOISEPAGE_ASSERT(*db_oid_ != catalog::INVALID_DATABASE_OID, "You're doing something hacky. Get a real database OID.");
  NOISEPAGE_ASSERT(query_text_.has_value(), "Query text must be specified.");
  NOISEPAGE_ASSERT(!override_qid_.has_value() || should_skip_query_cache_,
                   "Cannot override query ID without skipping the query cache.");
  NOISEPAGE_ASSERT(params_.empty() || (params_.at(0).size() == param_types_.size()),
                   "Parameter types do not match parameters in length.");
  // Set defaults.
  cost_model_ = cost_model_ != nullptr ? std::move(cost_model_) : std::make_unique<optimizer::TrivialCostModel>();

  return std::unique_ptr<TaskDML>(new TaskDML(*db_oid_, std::move(*query_text_), policy_, sync_, std::move(cost_model_),
                                              std::move(params_), std::move(param_types_), settings_, override_qid_,
                                              metrics_manager_, should_force_abort_, should_skip_query_cache_,
                                              std::move(tuple_fn_)));
}

TaskDML::Builder &TaskDML::Builder::SetCostModel(std::unique_ptr<optimizer::AbstractCostModel> cost_model) {
  cost_model_ = std::move(cost_model);
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetParameters(std::vector<std::vector<parser::ConstantValueExpression>> params) {
  params_ = std::move(params);
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetParameterTypes(std::vector<type::TypeId> param_types) {
  param_types_ = std::move(param_types);
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetMetricsManager(common::ManagedPointer<metrics::MetricsManager> metrics_manager) {
  metrics_manager_ = metrics_manager;
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetExecutionSettings(execution::exec::ExecutionSettings settings) {
  settings_ = settings;
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetShouldForceAbort(bool should_force_abort) {
  should_force_abort_ = should_force_abort;
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetShouldSkipQueryCache(bool should_skip_query_cache) {
  should_skip_query_cache_ = should_skip_query_cache;
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetOverrideQueryId(execution::query_id_t query_id) {
  override_qid_ = query_id;
  return *this;
}
TaskDML::Builder &TaskDML::Builder::SetTupleFn(util::TupleFunction tuple_fn) {
  tuple_fn_ = std::move(tuple_fn);
  return *this;
}

TaskDML::TaskDML(catalog::db_oid_t db_oid, std::string &&query_text, transaction::TransactionPolicy policy,
                 common::ManagedPointer<common::FutureDummy> sync,
                 std::unique_ptr<optimizer::AbstractCostModel> cost_model,
                 std::vector<std::vector<parser::ConstantValueExpression>> &&params,
                 std::vector<type::TypeId> &&param_types, execution::exec::ExecutionSettings settings,
                 std::optional<execution::query_id_t> override_qid,
                 common::ManagedPointer<metrics::MetricsManager> metrics_manager, bool should_force_abort,
                 bool should_skip_query_cache, util::TupleFunction &&tuple_fn)
    : Task(db_oid, std::move(query_text), policy, sync),
      cost_model_(std::move(cost_model)),
      params_(std::move(params)),
      param_types_(std::move(param_types)),
      settings_(settings),
      override_qid_(override_qid),
      metrics_manager_(metrics_manager),
      should_force_abort_(should_force_abort),
      should_skip_query_cache_(should_skip_query_cache),
      tuple_fn_(std::move(tuple_fn)) {}

void TaskDDL::Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                      common::ManagedPointer<task::TaskManager> task_manager) {
  query_exec_util->BeginTransaction(db_oid_);
  bool ddl_ok = query_exec_util->ExecuteDDL(query_text_, false);
  query_exec_util->EndTransaction(ddl_ok);

  if (sync_ != DISABLED) {
    if (ddl_ok) {
      sync_->Success(common::FutureDummyResult{});
    } else {
      sync_->Fail(query_exec_util->GetError());
    }
  }
}

void TaskDML::Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                      common::ManagedPointer<task::TaskManager> task_manager) {
  bool dml_ok = true;  // Track whether the DML was executed successfully.
  query_exec_util->BeginTransaction(db_oid_);

  if (should_skip_query_cache_) {
    // Skipping the query cache is implemented by invalidating the plan
    // and then invalidating the plan entry after execution.
    query_exec_util->ClearPlan(query_text_);
  }

  if (params_.empty()) {
    dml_ok = query_exec_util->ExecuteDML(query_text_, nullptr, nullptr, tuple_fn_, metrics_manager_,
                                         std::move(cost_model_), override_qid_, settings_);
  } else {
    std::vector<parser::ConstantValueExpression> &params_0 = params_[0];
    dml_ok = query_exec_util->CompileQuery(query_text_, common::ManagedPointer(&params_0),
                                           common::ManagedPointer(&param_types_), std::move(cost_model_), override_qid_,
                                           settings_);

    // Execute with specified parameters only if compilation succeeded
    if (dml_ok) {
      for (auto &param_vec : params_) {
        if (!dml_ok) break;

        dml_ok &= query_exec_util->ExecuteQuery(query_text_, tuple_fn_, common::ManagedPointer(&param_vec),
                                                metrics_manager_, settings_);
      }
    }

    // TODO(wz2): Require disciplined plan for clearing the plans
    // query_exec_util->ClearPlans();
  }

  if (should_skip_query_cache_) {
    // Don't save this query plan
    query_exec_util->ClearPlan(query_text_);
  }

  query_exec_util->EndTransaction(dml_ok && !should_force_abort_);

  if (sync_ != DISABLED) {
    if (dml_ok) {
      sync_->Success(common::FutureDummyResult{});
    } else {
      sync_->Fail(query_exec_util->GetError());
    }
  }
}

}  // namespace noisepage::task
