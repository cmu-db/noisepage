#include "task/task.h"
#include "task/task_manager.h"

#include "optimizer/cost_model/trivial_cost_model.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

void TaskDDL::Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                      common::ManagedPointer<task::TaskManager> task_manager) {
  query_exec_util->BeginTransaction(db_oid_);
  bool status = query_exec_util->ExecuteDDL(query_text_);
  query_exec_util->EndTransaction(status);
}

void TaskDML::Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                      common::ManagedPointer<task::TaskManager> task_manager) {
  bool result = true, compiled_result = true;
  query_exec_util->BeginTransaction(db_oid_);

  // TODO(wz2): https://github.com/cmu-db/noisepage/issues/1352
  // This works for now. Fixing the above issue will make it work beter.
  execution::exec::ExecutionSettings settings{};
  if (params_.empty()) {
    result = query_exec_util->ExecuteDML(query_text_, nullptr, nullptr, tuple_fn_, nullptr,
                                         std::make_unique<optimizer::TrivialCostModel>(), settings);
  } else {
    std::vector<parser::ConstantValueExpression> &params_0 = params_[0];
    compiled_result =
        query_exec_util->CompileQuery(query_text_, common::ManagedPointer(&params_0),
                                      common::ManagedPointer(&param_types_), std::move(cost_model_), settings);
    if (compiled_result) {
      // Execute with specified parameters
      for (auto &param_vec : params_) {
        if (!result) break;

        result &= query_exec_util->ExecuteQuery(query_text_, tuple_fn_, common::ManagedPointer(&param_vec), nullptr,
                                                settings);
      }
      query_exec_util->ClearPlans();
    }
  }

  query_exec_util->EndTransaction(!compiled_result || result);
  if (sync_) {
    sync_->Success(compiled_result && result);
  }
}

}  // namespace noisepage::task
