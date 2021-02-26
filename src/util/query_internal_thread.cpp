#include "util/query_internal_thread.h"

#include "optimizer/cost_model/trivial_cost_model.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::util {

void QueryInternalThread::QueryThreadLoop() {
  // This appears to be unsafe. The reason this is "correct" is that
  // run_queries_ is only set by the teardown.
  while (run_queries_ || !queue_.empty()) {
    std::unique_lock lock(queue_mutex_);
    queue_cv_.wait(lock);

    if (!queue_.empty()) {
      ExecuteRequest &req = queue_.front();
      query_exec_util_->BeginTransaction();
      if (req.cost_model_ == nullptr) {
        query_exec_util_->SetCostModelFunction([] { return std::make_unique<optimizer::TrivialCostModel>(); });
      } else {
        query_exec_util_->SetCostModelFunction([&req] { return std::move(req.cost_model_); });
      }

      if (req.db_oid_ == catalog::INVALID_DATABASE_OID) {
        query_exec_util_->SetDefaultDatabase();
      }

      bool result = true;
      bool compiled_result = true;
      if (req.is_ddl_) {
        result = query_exec_util_->ExecuteDDL(req.query_text_);
      } else if (req.params_.empty()) {
        result = query_exec_util_->ExecuteDML(req.query_text_, nullptr, nullptr, nullptr, nullptr);
      } else {
        std::vector<parser::ConstantValueExpression> &params_0 = req.params_[0];
        size_t idx = query_exec_util_->CompileQuery(req.query_text_, common::ManagedPointer(&params_0),
                                                    common::ManagedPointer(&req.param_types_), &compiled_result);
        if (compiled_result) {
          for (auto &param_vec : req.params_) {
            if (!result) break;

            result &= query_exec_util_->ExecuteQuery(idx, nullptr, common::ManagedPointer(&param_vec), nullptr);
          }
        }
      }

      // If compile fails, commit the transaction
      query_exec_util_->EndTransaction(!compiled_result || result);
      queue_.pop();
    }
  }
}

}  // namespace noisepage::util
