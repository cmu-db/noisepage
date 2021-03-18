#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "parser/expression/constant_value_expression.h"

#include <vector>

namespace noisepage::util {
class QueryExecUtil;
}

namespace noisepage::task {

class TaskManager;

enum class TaskType : uint8_t { DDL_TASK, DML_TASK };

class Task {
 public:
  virtual void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                       common::ManagedPointer<task::TaskManager> task_manager) = 0;
  virtual TaskType GetTaskType() = 0;

  virtual ~Task() = default;
};

class TaskDDL : public Task {
 public:
  TaskDDL(catalog::db_oid_t db_oid, std::string query_text) : db_oid_(db_oid), query_text_(query_text) {}

  void Execute(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
               common::ManagedPointer<task::TaskManager> task_manager) override;
  TaskType GetTaskType() override { return TaskType::DDL_TASK; }

  ~TaskDDL() override = default;

 private:
  catalog::db_oid_t db_oid_;
  std::string query_text_;
};

class TaskDML : public Task {
 public:
  TaskDML(catalog::db_oid_t db_oid, std::string query_text, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
          std::vector<std::vector<parser::ConstantValueExpression>> &&params, std::vector<type::TypeId> &&param_types)
      : db_oid_(db_oid),
        query_text_(query_text),
        cost_model_(std::move(cost_model)),
        params_(params),
        param_types_(param_types) {}

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
};

}  // namespace noisepage::task
