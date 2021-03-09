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

namespace noisepage::parser {
class ConstantValueExpression;
}

namespace noisepage::common {
template <class Result>
class Future;
}

namespace noisepage::util {

/**
 * Request type
 */
enum class RequestType : uint8_t {
  DDL,
  DML,
  SYNC,
};

/**
 * Describes a single request to be submitted to QueryInternalThread for execution.
 */
class ExecuteRequest {
 public:
  /** Whether statement is a DDL */
  RequestType type_;

  /** What database OID to use */
  catalog::db_oid_t db_oid_;

  /** Query text to execute */
  std::string query_text_;

  /** Cost model to utilize */
  std::unique_ptr<optimizer::AbstractCostModel> cost_model_;

  /** Vector of params for queries */
  std::vector<std::vector<parser::ConstantValueExpression>> params_;

  /** Param types */
  std::vector<type::TypeId> param_types_;

  /** A future used to enable waiting by the caller for execution */
  common::ManagedPointer<common::Future<bool>> notify_;
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
    NOISEPAGE_ASSERT(run_queries_, "QueryInternalThread should not be shutting down");

    {
      // Add the request
      std::scoped_lock lock(queue_mutex_);
      queue_.emplace(std::move(request));
    }

    queue_cv_.notify_one();
  }

 private:
  volatile bool run_queries_ = true;

  std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<ExecuteRequest> queue_;
  std::thread query_thread_;

  void QueryThreadLoop();
};

}  // namespace noisepage::util
