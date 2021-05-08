#include "task/task.h"
#include "task/task_manager.h"

#include "optimizer/cost_model/trivial_cost_model.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

void TaskDDL::Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                      common::ManagedPointer<task::TaskManager> task_manager) {
  query_exec_util->BeginTransaction(db_oid_);
  bool status = query_exec_util->ExecuteDDL(query_text_, false);
  query_exec_util->EndTransaction(status);

  if (sync_) {
    if (status)
      sync_->Success(DummyResult{});
    else
      sync_->Fail(query_exec_util->GetError());
  }
}

void TaskDML::Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                      common::ManagedPointer<task::TaskManager> task_manager) {
  bool result = true;
  query_exec_util->BeginTransaction(db_oid_);

  if (skip_query_cache_) {
    // Skipping the query cache is implemented by invalidating the plan
    // and then invalidating the plan entry after execution.
    query_exec_util->ClearPlan(query_text_);
  }

  if (params_.empty()) {
    result = query_exec_util->ExecuteDML(query_text_, nullptr, nullptr, tuple_fn_, metrics_manager_,
                                         std::make_unique<optimizer::TrivialCostModel>(), override_qid_, settings_);
  } else {
    std::vector<parser::ConstantValueExpression> &params_0 = params_[0];
    result = query_exec_util->CompileQuery(query_text_, common::ManagedPointer(&params_0),
                                           common::ManagedPointer(&param_types_), std::move(cost_model_), override_qid_,
                                           settings_);

    // Execute with specified parameters only if compilation succeeded
    if (result) {
      for (auto &param_vec : params_) {
        if (!result) break;

        result &= query_exec_util->ExecuteQuery(query_text_, tuple_fn_, common::ManagedPointer(&param_vec),
                                                metrics_manager_, settings_);
      }
    }

    // TODO(wz2): Require disciplined plan for clearing the plans
    // query_exec_util->ClearPlans();
  }

  if (skip_query_cache_) {
    // Don't save this query plan
    query_exec_util->ClearPlan(query_text_);
  }

  query_exec_util->EndTransaction(result && !force_abort_);
  if (sync_) {
    if (result)
      sync_->Success(DummyResult{});
    else
      sync_->Fail(query_exec_util->GetError());
  }
}

}  // namespace noisepage::task
