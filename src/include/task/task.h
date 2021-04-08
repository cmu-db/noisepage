#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/future.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "parser/expression/constant_value_expression.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

class TaskManager;

/** Enum class used to describe the types of tasks available */
enum class TaskType : uint8_t { DDL_TASK, DML_TASK };

/**
 * Abstract class for defining a task
 */
class Task {
 public:
  /**
   * Executes the task
   * @param query_exec_util Query execution utility
   * @param task_manager TaskManager that task was submitted to
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
   */
  TaskDDL(catalog::db_oid_t db_oid, std::string query_text) : db_oid_(db_oid), query_text_(std::move(query_text)) {}

  void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
               common::ManagedPointer<task::TaskManager> task_manager) override;
  TaskType GetTaskType() override { return TaskType::DDL_TASK; }

  ~TaskDDL() override = default;

 private:
  catalog::db_oid_t db_oid_;
  std::string query_text_;
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
        adopt_qid_(false),
        qid_target_(execution::query_id_t(0)),
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
   * @param force_abort Whether to forcefully abort the transaction
   * @param skip_query_cache Whether to skip retrieving pre-optimized and saving optimized plans
   * @param adopt_qid Whether executing query should use qid_target
   * @param qid_target Target QID to use if adopt_qid is true
   * @param sync Future for the caller to block on
   */
  TaskDML(catalog::db_oid_t db_oid, std::string query_text, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
          std::vector<std::vector<parser::ConstantValueExpression>> &&params, std::vector<type::TypeId> &&param_types,
          util::TupleFunction tuple_fn, common::ManagedPointer<metrics::MetricsManager> metrics_manager,
          bool force_abort, bool skip_query_cache, bool adopt_qid, execution::query_id_t qid_taget,
          common::ManagedPointer<common::Future<bool>> sync)
      : db_oid_(db_oid),
        query_text_(std::move(query_text)),
        cost_model_(std::move(cost_model)),
        params_(params),
        param_types_(param_types),
        tuple_fn_(std::move(tuple_fn)),
        metrics_manager_(metrics_manager),
        force_abort_(force_abort),
        skip_query_cache_(skip_query_cache),
        adopt_qid_(adopt_qid),
        qid_target_(qid_target),
        sync_(sync) {
    NOISEPAGE_ASSERT(!adopt_qid || skip_query_cache, "adopt_qid requires skip_query_cache");
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
          bool skip_query_cache, util::TupleFunction tuple_fn, common::ManagedPointer<common::Future<bool>> sync)
      : db_oid_(db_oid),
        query_text_(std::move(query_text)),
        cost_model_(std::move(cost_model)),
        params_({}),
        param_types_({}),
        tuple_fn_(std::move(tuple_fn)),
        metrics_manager_(nullptr),
        force_abort_(false),
        skip_query_cache_(skip_query_cache),
        adopt_qid_(false),
        qid_target_(execution::query_id_t(0)),
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
  bool force_abort_;
  bool skip_query_cache_;
  bool adopt_qid_;
  execution::query_id_t qid_target_;
  common::ManagedPointer<common::Future<bool>> sync_;
};

}  // namespace noisepage::task
