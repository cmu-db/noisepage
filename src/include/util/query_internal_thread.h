#pragma once

#include <chrono>  //NOLINT
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>  //NOLINT

#include "catalog/catalog_defs.h"
#include "execution/compiler/executable_query.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "planner/plannodes/output_schema.h"
#include "type/type_id.h"
#include "util/query_exec_util.h"

namespace noisepage::optimizer {
class AbstractCostModel;
}

namespace noisepage::parser {
class ConstantValueExpression;
}

namespace noisepage::util {

class ExecuteRequest {
 public:
  bool is_ddl_;
  catalog::db_oid_t db_oid_;
  std::string query_text_;
  std::unique_ptr<optimizer::AbstractCostModel> cost_model_;

  std::vector<std::vector<parser::ConstantValueExpression>> params_;
  std::vector<type::TypeId> param_types_;
};

/**
 * Class for spinning off a thread that runs metrics collection at a fixed interval. This should be used in most cases
 * to enable metrics aggregation in the system unless you need fine-grained control over state or profiling.
 */
class QueryInternalThread {
 public:
  QueryInternalThread(std::unique_ptr<util::QueryExecUtil> query_exec_util)
      : query_exec_util_(std::move(query_exec_util)), query_thread_(std::thread([this] { QueryThreadLoop(); })) {}

  ~QueryInternalThread() {
    run_queries_ = false;
    queue_cv_.notify_one();
    query_thread_.join();
  }

  void AddRequest(ExecuteRequest &&request) {
    if (!run_queries_) {
      return;
    }

    {
      std::scoped_lock lock(queue_mutex_);
      queue_.emplace(std::move(request));
    }

    queue_cv_.notify_one();
  }

 private:
  volatile bool run_queries_ = true;

  std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  std::thread query_thread_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<ExecuteRequest> queue_;

  void QueryThreadLoop();
};

}  // namespace noisepage::util
