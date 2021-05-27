#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/future.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "transaction/transaction_defs.h"
#include "type/type_id.h"
#include "util/util_defs.h"

namespace noisepage::metrics {
class MetricsManager;
}  // namespace noisepage::metrics

namespace noisepage::optimizer {
class AbstractCostModel;
}  // namespace noisepage::optimizer

namespace noisepage::parser {
class ConstantValueExpression;
}  // namespace noisepage::parser

namespace noisepage::util {
class QueryExecUtil;
}  // namespace noisepage::util

namespace noisepage::task {

class TaskManager;

/** The types of tasks available. */
enum class TaskType : uint8_t { DDL_TASK, DML_TASK };

/** An abstract representation of a Task to be executed. */
class Task {
 public:
  /** Base builder class for tasks. */
  template <class ConcreteType>
  class Builder {
   public:
    Builder() = default;
    virtual ~Builder() = default;

    /** Set the database to execute the task in. */
    ConcreteType &SetDatabaseOid(const catalog::db_oid_t db_oid) {
      db_oid_ = db_oid;
      return *dynamic_cast<ConcreteType *>(this);
    }
    /** Set the query text to be executed. */
    ConcreteType &SetQueryText(std::string &&query_text) {
      query_text_ = std::move(query_text);
      return *dynamic_cast<ConcreteType *>(this);
    }
    /** Set the policy for the transaction that this Task will be executed in. */
    ConcreteType &SetTransactionPolicy(const transaction::TransactionPolicy policy) {
      policy_ = policy;
      return *dynamic_cast<ConcreteType *>(this);
    }
    /** (Optional) Set the pointer to the Future that the caller is potentially blocking on. */
    ConcreteType &SetFuture(const common::ManagedPointer<common::FutureDummy> sync) {
      sync_ = sync;
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    std::optional<catalog::db_oid_t> db_oid_{std::nullopt};  ///< The database to execute the task in.
    std::optional<std::string> query_text_{std::nullopt};    ///< The query text to be executed.
    std::optional<transaction::TransactionPolicy> policy_;   ///< The policy for this Task's transaction.
    common::ManagedPointer<common::FutureDummy> sync_;       ///< Future* that the caller may block on.
  };

  /** Destructor. */
  virtual ~Task() = default;

  /** @return The type of this task. */
  virtual TaskType GetTaskType() = 0;

  /**
   * Execute the specified task.
   * @param query_exec_util Query execution utility.
   * @param task_manager    TaskManager that the task was submitted to (i.e., TaskManager running the current task).
   */
  virtual void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                       common::ManagedPointer<task::TaskManager> task_manager) = 0;

 protected:
  const catalog::db_oid_t db_oid_;               ///< The database to execute the task in.
  const std::string query_text_;                 ///< The query text to be executed.
  const transaction::TransactionPolicy policy_;  ///< The policy for the transaction that this Task is executed in.
  const common::ManagedPointer<common::FutureDummy> sync_;  ///< Future* that the caller may block on.

  /** Constructor. */
  Task(catalog::db_oid_t db_oid, std::string &&query_text, transaction::TransactionPolicy policy,
       common::ManagedPointer<common::FutureDummy> sync);
};

/** Task representing a DDL command or a SET statement. */
class TaskDDL : public Task {
 public:
  /** Builder for a TaskDDL. */
  class Builder : public Task::Builder<Builder> {
   public:
    /** Constructor. */
    Builder() = default;
    /** No copying or moving allowed. */
    DISALLOW_COPY_AND_MOVE(Builder);
    /** @return The TaskDDL. */
    std::unique_ptr<TaskDDL> Build();
  };

  ~TaskDDL() override = default;
  TaskType GetTaskType() override { return TaskType::DDL_TASK; }

  void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
               common::ManagedPointer<task::TaskManager> task_manager) override;

 private:
  /** Constructor. */
  TaskDDL(catalog::db_oid_t db_oid, std::string &&query_text, transaction::TransactionPolicy policy,
          common::ManagedPointer<common::FutureDummy> sync)
      : Task(db_oid, std::move(query_text), policy, sync) {}
};

/** Task representing a DML command. */
class TaskDML : public Task {
 public:
  /** Builder for a TaskDML. */
  class Builder : public Task::Builder<Builder> {
   public:
    /** Constructor. */
    Builder() = default;
    /** No copying or moving allowed. */
    DISALLOW_COPY_AND_MOVE(Builder);
    /** @return The TaskDML. */
    std::unique_ptr<TaskDML> Build();

    /** (Optional, default: TrivialCostModel) Set the cost model to use for optimizing the query. */
    Builder &SetCostModel(std::unique_ptr<optimizer::AbstractCostModel> cost_model);
    /** (Optional, default: empty) Set the query parameters. */
    Builder &SetParameters(std::vector<std::vector<parser::ConstantValueExpression>> params);
    /** (Optional, default: empty) Set the types for the query parameters. */
    Builder &SetParameterTypes(std::vector<type::TypeId> param_types);
    /** (Optional, default: nullptr) Set the metrics manager to be used. */
    Builder &SetMetricsManager(common::ManagedPointer<metrics::MetricsManager> metrics_manager);
    /** (Optional, default: default-constructed) Set the execution settings for this query. */
    Builder &SetExecutionSettings(execution::exec::ExecutionSettings settings);
    /** (Optional, default: false) Set whether to forcefully abort this transaction. */
    Builder &SetShouldForceAbort(bool should_force_abort);
    /** (Optional, default: false) Set whether to skip the query cache. */
    Builder &SetShouldSkipQueryCache(bool should_skip_query_cache);
    /** (Optional, default: will not override query ID) Set the query ID to use. */
    Builder &SetOverrideQueryId(execution::query_id_t query_id);
    /** (Optional, default: nullptr) Set the function to invoke on each output tuple. */
    Builder &SetTupleFn(util::TupleFunction tuple_fn);

   private:
    std::unique_ptr<optimizer::AbstractCostModel> cost_model_{nullptr};   ///< The cost model to use.
    std::vector<std::vector<parser::ConstantValueExpression>> params_{};  ///< Query parameters.
    std::vector<type::TypeId> param_types_{};                             ///< Types for query parameters.
    execution::exec::ExecutionSettings settings_;                         ///< Settings to execute the query with.
    std::optional<execution::query_id_t> override_qid_{std::nullopt};     ///< Query ID to use.

    common::ManagedPointer<metrics::MetricsManager> metrics_manager_{nullptr};  ///< Metrics manager to be used.
    bool should_force_abort_{false};  ///< True if the txn should be forcefully aborted.
    bool should_skip_query_cache_;    ///< True if the txn should skip the query cache.
    util::TupleFunction tuple_fn_;    ///< Function to be called on each output tuple.
  };

  ~TaskDML() override = default;
  TaskType GetTaskType() override { return TaskType::DML_TASK; }

  void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
               common::ManagedPointer<task::TaskManager> task_manager) override;

 private:
  /** Constructor. */
  explicit TaskDML(catalog::db_oid_t db_oid, std::string &&query_text, transaction::TransactionPolicy policy,
                   common::ManagedPointer<common::FutureDummy> sync,
                   std::unique_ptr<optimizer::AbstractCostModel> cost_model,
                   std::vector<std::vector<parser::ConstantValueExpression>> &&params,
                   std::vector<type::TypeId> &&param_types, execution::exec::ExecutionSettings settings,
                   std::optional<execution::query_id_t> override_qid,
                   common::ManagedPointer<metrics::MetricsManager> metrics_manager, bool should_force_abort,
                   bool should_skip_query_cache, util::TupleFunction &&tuple_fn);

  std::unique_ptr<optimizer::AbstractCostModel> cost_model_{nullptr};      ///< The cost model to use.
  std::vector<std::vector<parser::ConstantValueExpression>> params_{};     ///< Query parameters.
  std::vector<type::TypeId> param_types_{};                                ///< Types for query parameters.
  const execution::exec::ExecutionSettings settings_;                      ///< Settings to execute the query with.
  const std::optional<execution::query_id_t> override_qid_{std::nullopt};  ///< Query ID to use.

  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_{nullptr};  ///< Metrics manager to be used.
  const bool should_force_abort_{false};  ///< True if the txn should be forcefully aborted.
  const bool should_skip_query_cache_;    ///< True if the txn should skip the query cache.
  const util::TupleFunction tuple_fn_;    ///< Function to be called on each output tuple.
};

}  // namespace noisepage::task
