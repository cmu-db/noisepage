#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/future.h"
#include "execution/exec_defs.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "parser/expression/constant_value_expression.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

class TaskManager;

/** Enum class used to describe the types of tasks available */
enum class TaskType : uint8_t { DDL_TASK, DML_TASK };

/**
 * Type meant to represent a dummy result value.
 * A Future<> cannot be specified with (void), hence why
 * this dummy result type is used.
 */
struct DummyResult {};

/**
 * Abstract class for defining a task
 */
class Task {
 public:
  /**
   * Executes the task
   * @param query_exec_util Query execution utility
   * @param task_manager TaskManager that the task was submitted to (i.e., TaskManager running the current task)
   */
  virtual void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                       common::ManagedPointer<task::TaskManager> task_manager) = 0;

  /**
   * @return task type
   */
  virtual TaskType GetTaskType() = 0;

  /**
   * Default (virtual) destructor
   */
  virtual ~Task() = default;
};

/**
 * Task executes a DDL or SET task.
 */
class TaskDDL : public Task {
 public:
  /**
   * TaskDDL constructor
   * @param db_oid Database to execute task within
   * @param query_text Query text of DDL/SET
   * @param sync Future for the caller to block on
   */
  TaskDDL(catalog::db_oid_t db_oid, std::string query_text, common::ManagedPointer<common::Future<DummyResult>> sync)
      : db_oid_(db_oid), query_text_(std::move(query_text)), sync_(sync) {}

  void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
               common::ManagedPointer<task::TaskManager> task_manager) override;
  TaskType GetTaskType() override { return TaskType::DDL_TASK; }

  ~TaskDDL() override = default;

 private:
  catalog::db_oid_t db_oid_;
  std::string query_text_;
  common::ManagedPointer<common::Future<DummyResult>> sync_;
};

/**
 * Task executes a DML
 */
class TaskDML : public Task {
 public:
  /**
   * TaskDML constructor
   * @param db_oid Database to execute task within
   * @param query_text DML query to execute
   * @param cost_model Cost model to use for optimizing query
   * @param skip_query_cache Whether to skip retrieving pre-optimized and saving optimized plans
   * @param params Relevant query parameters
   * @param param_types Types of the query parameters if any
   */
  TaskDML(catalog::db_oid_t db_oid, std::string query_text, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
          bool skip_query_cache, std::vector<std::vector<parser::ConstantValueExpression>> &&params,
          std::vector<type::TypeId> &&param_types)
      : db_oid_(db_oid),
        query_text_(std::move(query_text)),
        cost_model_(std::move(cost_model)),
        params_(params),
        param_types_(param_types),
        tuple_fn_(nullptr),
        metrics_manager_(nullptr),
        force_abort_(false),
        skip_query_cache_(skip_query_cache),
        override_qid_(std::nullopt),
        sync_(nullptr) {}

  /**
   * TaskDML constructor
   * @param db_oid Database to execute task within
   * @param query_text DML query to execute
   * @param cost_model Cost model to use for optimizing query
   * @param params Relevant query parameters
   * @param param_types Types of the query parameters if any
   * @param tuple_fn Function for processing rows
   * @param metrics_manager Metrics Manager to be used
   * @param settings ExecutionSettings of this query (note: clang-tidy complains that ExecutionSettings is
   * trivially-copyable so we can't use std::move())
   * @param force_abort Whether to forcefully abort the transaction
   * @param skip_query_cache Whether to skip retrieving pre-optimized and saving optimized plans
   * @param override_qid Describes whether to override the qid with a value
   * @param sync Future for the caller to block on
   */
  TaskDML(catalog::db_oid_t db_oid, std::string query_text, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
          std::vector<std::vector<parser::ConstantValueExpression>> &&params, std::vector<type::TypeId> &&param_types,
          util::TupleFunction tuple_fn, common::ManagedPointer<metrics::MetricsManager> metrics_manager,
          execution::exec::ExecutionSettings settings, bool force_abort, bool skip_query_cache,
          std::optional<execution::query_id_t> override_qid, common::ManagedPointer<common::Future<DummyResult>> sync)
      : db_oid_(db_oid),
        query_text_(std::move(query_text)),
        cost_model_(std::move(cost_model)),
        params_(params),
        param_types_(param_types),
        tuple_fn_(std::move(tuple_fn)),
        metrics_manager_(metrics_manager),
        settings_(settings),
        force_abort_(force_abort),
        skip_query_cache_(skip_query_cache),
        override_qid_(override_qid),
        sync_(sync) {
    NOISEPAGE_ASSERT(!override_qid.has_value() || skip_query_cache, "override_qid requires skip_query_cache");
  }

  /**
   * TaskDML constructor
   * @param db_oid Database to execute task within
   * @param query_text DML query to execute
   * @param cost_model Cost model to use for optimizing query
   * @param skip_query_cache Whether to skip retrieving pre-optimized and saving optimized plans
   * @param tuple_fn Function for processing rows
   * @param sync Future for the caller to block on
   */
  TaskDML(catalog::db_oid_t db_oid, std::string query_text, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
          bool skip_query_cache, util::TupleFunction tuple_fn, common::ManagedPointer<common::Future<DummyResult>> sync)
      : db_oid_(db_oid),
        query_text_(std::move(query_text)),
        cost_model_(std::move(cost_model)),
        params_({}),
        param_types_({}),
        tuple_fn_(std::move(tuple_fn)),
        metrics_manager_(nullptr),
        force_abort_(false),
        skip_query_cache_(skip_query_cache),
        override_qid_(std::nullopt),
        sync_(sync) {}

  void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
               common::ManagedPointer<task::TaskManager> task_manager) override;
  TaskType GetTaskType() override { return TaskType::DML_TASK; }

  ~TaskDML() override = default;

 private:
  catalog::db_oid_t db_oid_;
  std::string query_text_;
  std::unique_ptr<optimizer::AbstractCostModel> cost_model_;
  std::vector<std::vector<parser::ConstantValueExpression>> params_;
  std::vector<type::TypeId> param_types_;
  util::TupleFunction tuple_fn_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  execution::exec::ExecutionSettings settings_;
  bool force_abort_;
  bool skip_query_cache_;
  std::optional<execution::query_id_t> override_qid_;
  common::ManagedPointer<common::Future<DummyResult>> sync_;
};

}  // namespace noisepage::task
