#include <random>
#include <string>
#include <vector>

#include "common/macros.h"
#include "main/db_main.h"
#include "metrics/metrics_manager.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "test_util/test_harness.h"

namespace noisepage::task::test {

class TaskManagerTests : public TerrierTest {
 public:
  static constexpr size_t NUM_VALUE = 1000;

  void SetUp() final {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);
    param_map.find(settings::Param::task_pool_size)->second.value_ =
        parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(1));

    db_main_ = noisepage::DBMain::Builder()
                   .SetSettingsParameterMap(std::move(param_map))
                   .SetUseSettingsManager(true)
                   .SetUseGC(true)
                   .SetUseCatalog(true)
                   .SetUseGCThread(true)
                   .SetUseTrafficCop(true)
                   .SetUseStatsStorage(true)
                   .SetUseLogging(true)
                   .SetUseNetwork(true)
                   .SetUseExecution(true)
                   .Build();

    task_manager_ = db_main_->GetTaskManager();
    task_manager_->AddTask(std::make_unique<task::TaskDDL>(catalog::db_oid_t(0), "CREATE TABLE t (a INT)", nullptr));
    task_manager_->WaitForFlush();
  }

  void LoadInsert() {
    for (size_t i = 0; i < NUM_VALUE; i++) {
      std::vector<type::TypeId> types{type::TypeId::INTEGER};
      std::vector<parser::ConstantValueExpression> params;
      params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(i));

      std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
      params_vec.emplace_back(std::move(params));
      task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::db_oid_t(0), "INSERT INTO t VALUES ($1)",
                                                             std::make_unique<optimizer::TrivialCostModel>(), false,
                                                             std::move(params_vec), std::move(types)));
    }
  }

  void CheckTable() {
    common::Future<task::DummyResult> sync;
    std::unordered_set<int64_t> results;
    auto to_row_fn = [&results](const std::vector<execution::sql::Val *> &values) {
      results.insert(static_cast<execution::sql::Integer *>(values[0])->val_);
    };

    task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, "SELECT * FROM t",
                                                           std::make_unique<optimizer::TrivialCostModel>(), false,
                                                           to_row_fn, common::ManagedPointer(&sync)));

    EXPECT_TRUE(sync.DangerousWait().second);
    EXPECT_EQ(results.size(), NUM_VALUE);
    for (size_t i = 0; i < NUM_VALUE; i++) {
      EXPECT_TRUE(results.find(i) != results.end());
    }
  }

 protected:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<task::TaskManager> task_manager_;
};

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, SingleThreadedInsert) {
  LoadInsert();
  task_manager_->WaitForFlush();
  CheckTable();
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, MultiThreadedInsert) {
  task_manager_->SetTaskPoolSize(8);

  LoadInsert();
  task_manager_->WaitForFlush();
  CheckTable();
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, WorkerEnlargeInsert) {
  LoadInsert();
  task_manager_->SetTaskPoolSize(8);
  task_manager_->WaitForFlush();
  CheckTable();
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, WorkerShrinkInsert) {
  task_manager_->SetTaskPoolSize(8);
  LoadInsert();
  task_manager_->SetTaskPoolSize(1);
  task_manager_->WaitForFlush();
  CheckTable();
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, BulkInsert) {
  std::vector<type::TypeId> types{type::TypeId::INTEGER};
  std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
  for (size_t i = 0; i < NUM_VALUE; i++) {
    std::vector<parser::ConstantValueExpression> params;
    params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(i));
    params_vec.emplace_back(std::move(params));
  }
  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::db_oid_t(0), "INSERT INTO t VALUES ($1)",
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         std::move(params_vec), std::move(types)));
  task_manager_->WaitForFlush();
  CheckTable();
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, Index) {
  task_manager_->AddTask(std::make_unique<task::TaskDDL>(catalog::db_oid_t(0), "CREATE INDEX idx ON t (a)", nullptr));
  task_manager_->WaitForFlush();

  auto query_exec_util = db_main_->GetQueryExecUtil();
  query_exec_util->BeginTransaction(catalog::INVALID_DATABASE_OID);

  std::string query = "SELECT * FROM t WHERE a = 1";
  auto result =
      query_exec_util->PlanStatement(query, nullptr, nullptr, std::make_unique<optimizer::TrivialCostModel>());
  EXPECT_TRUE(result != nullptr);
  EXPECT_EQ(result->PhysicalPlan()->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
  query_exec_util->EndTransaction(true);
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, InvalidStatements) {
  // Parse error
  common::Future<task::DummyResult> sync;
  task_manager_->AddTask(
      std::make_unique<task::TaskDDL>(catalog::db_oid_t(0), "CREATE INDEX", common::ManagedPointer(&sync)));
  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::db_oid_t(0), "INSERT INTO t VALUES (#1)",
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         nullptr, nullptr));

  // Binding error
  task_manager_->AddTask(std::make_unique<task::TaskDDL>(catalog::db_oid_t(0), "CREATE INDEX idxx on tt (a)", nullptr));
  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::db_oid_t(0), "INSERT INTO tt VALUES (1)",
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         nullptr, nullptr));
  task_manager_->WaitForFlush();

  auto result = sync.DangerousWait();
  EXPECT_TRUE(!result.second);
  EXPECT_TRUE(!sync.FailMessage().empty());
}

// NOLINTNEXTLINE
TEST_F(TaskManagerTests, AbortingTest) {
  task_manager_->AddTask(
      std::make_unique<task::TaskDDL>(catalog::db_oid_t(0), "CREATE TABLE ttt (a INT, PRIMARY KEY (a))", nullptr));
  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::db_oid_t(0), "INSERT INTO ttt VALUES (1)",
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         nullptr, nullptr));
  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::db_oid_t(0), "INSERT INTO ttt VALUES (1)",
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         nullptr, nullptr));
  task_manager_->WaitForFlush();

  common::Future<task::DummyResult> sync;
  std::vector<int64_t> results;
  auto to_row_fn = [&results](const std::vector<execution::sql::Val *> &values) {
    results.push_back(static_cast<execution::sql::Integer *>(values[0])->val_);
  };

  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, "SELECT * FROM ttt",
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         to_row_fn, common::ManagedPointer(&sync)));

  auto result = sync.DangerousWait();
  EXPECT_TRUE(result.second);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

}  // namespace noisepage::task::test
