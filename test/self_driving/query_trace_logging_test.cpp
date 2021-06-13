#include "common/json.h"
#include "execution/sql/value_util.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "metrics/query_trace_metric.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "test_util/test_harness.h"

namespace noisepage::selfdriving::pilot::test {

class QueryTraceLogging : public TerrierTest {
  static constexpr const char *BUILD_ABS_PATH = "BUILD_ABS_PATH";

  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);

    // Adjust the startup DDL path if specified
    const char *env = ::getenv(BUILD_ABS_PATH);
    if (env != nullptr) {
      std::string file = std::string(env) + "/bin/startup.sql";
      const auto string = std::string_view(file);
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      param_map.find(settings::Param::startup_ddl_path)->second.value_ =
          parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
    }

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

    settings_manager_ = db_main_->GetSettingsManager();
    metrics_manager_ = db_main_->GetMetricsManager();
    metrics_manager_->EnableMetric(metrics::MetricsComponent::QUERY_TRACE);
    metrics_manager_->SetMetricOutput(metrics::MetricsComponent::QUERY_TRACE, metrics::MetricsOutput::CSV_AND_DB);
    db_main_->GetMetricsThread()->PauseMetrics();  // We want to aggregate them manually, so pause the thread.
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    task_manager_ = db_main_->GetTaskManager();
    db_main_->TryLoadStartupDDL();
  }

  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

 protected:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<task::TaskManager> task_manager_;
};  // namespace noisepage::selfdriving::pilot::test

// NOLINTNEXTLINE
TEST_F(QueryTraceLogging, BasicLogging) {
  auto util = db_main_->GetQueryExecUtil();
  metrics_manager_->RegisterThread();
  metrics_manager_->SetMetricOutput(metrics::MetricsComponent::QUERY_TRACE, metrics::MetricsOutput::DB);

  size_t num_sample = 2;
  metrics::QueryTraceMetricRawData::query_param_sample = num_sample;
  metrics::QueryTraceMetricRawData::query_segment_interval = 10;

  std::vector<unsigned int> qids = {0, 1, 2, 3, 4, 5, 6};
  std::vector<unsigned int> db_oids = {0, 0, 0, 0, 1, 2, 0};
  std::vector<std::string> texts = {"a", "b", "c", "d", "e", "f", "g"};
  std::vector<std::vector<parser::ConstantValueExpression>> parameters = {
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(11))},
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(22))},
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(33))},
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(44))},
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(55))},
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(66))},
      std::vector<parser::ConstantValueExpression>{
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(77))},
  };

  // Create 3 intervals of time
  // timestamps[i][j] = x states qid i has x occurrences in interval  j
  std::vector<std::vector<int>> timestamps = {
      std::vector<int>{3, 5, 7}, std::vector<int>{2, 1, 3}, std::vector<int>{3, 7, 9}, std::vector<int>{9, 8, 3},
      std::vector<int>{2, 2, 2}, std::vector<int>{2, 1, 5}, std::vector<int>{4, 5, 7}};

  for (size_t i = 0; i < qids.size(); i++) {
    common::thread_context.metrics_store_->RecordQueryText(
        catalog::db_oid_t{db_oids[i]}, execution::query_id_t{qids[i]}, texts[i],
        common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&parameters[i]), 10000);
  }

  std::vector<int> totals = {0, 0, 0};
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < qids.size(); i++) {
      for (int slide_delta = 0; slide_delta < timestamps[i][j]; slide_delta++) {
        size_t time = j * metrics::QueryTraceMetricRawData::query_segment_interval + slide_delta;
        common::thread_context.metrics_store_->RecordQueryTrace(
            catalog::db_oid_t{db_oids[i]}, execution::query_id_t{qids[i]}, time,
            common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&parameters[i]));
      }

      totals[j] += timestamps[i][j];
    }
  }

  metrics_manager_->Aggregate();

  auto raw = reinterpret_cast<metrics::QueryTraceMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(metrics::MetricsComponent::QUERY_TRACE)).get());
  raw->WriteToDB(task_manager_, false, 29, nullptr, nullptr);
  task_manager_->WaitForFlush();

  auto select_count = [util](const std::string &query, size_t target) {
    util->BeginTransaction(catalog::INVALID_DATABASE_OID);
    uint64_t row_count = 0;
    auto to_row_fn = [&row_count](const std::vector<execution::sql::Val *> &values) { row_count++; };

    execution::exec::ExecutionSettings settings{};
    bool result = util->ExecuteDML(query, nullptr, nullptr, to_row_fn, nullptr,
                                   std::make_unique<optimizer::TrivialCostModel>(), std::nullopt, settings);
    EXPECT_TRUE(result && "SELECT should have succeeded");
    EXPECT_TRUE(row_count == target && "Row count incorrect");
    util->EndTransaction(true);
  };

  // These are only updated on a forecast interval
  select_count("SELECT * FROM noisepage_forecast_texts", 0);
  select_count("SELECT * FROM noisepage_forecast_parameters", 0);

  auto task_manager = task_manager_;
  auto check_freqs = [task_manager, &qids, &totals, &timestamps](size_t num_interval) {
    size_t seen = 0;
    std::unordered_map<size_t, std::unordered_map<size_t, size_t>> qid_map;
    auto freq_check = [&seen, &qid_map](const std::vector<execution::sql::Val *> &values) {
      auto ts = reinterpret_cast<execution::sql::Integer *>(values[0])->val_;
      auto qid = reinterpret_cast<execution::sql::Integer *>(values[1])->val_;
      auto qid_seen = reinterpret_cast<execution::sql::Real *>(values[2])->val_;
      auto itv = ts / metrics::QueryTraceMetricRawData::query_segment_interval;

      // Record <qid, <interval, seen>>
      qid_map[qid][itv] += qid_seen;

      // Sum the number seen
      seen += qid_seen;
    };

    size_t combined = 0;
    for (size_t i = 0; i < num_interval; i++) {
      combined += totals[i];
    }

    std::vector<std::vector<parser::ConstantValueExpression>> params;
    std::vector<type::TypeId> param_types;

    common::Future<task::DummyResult> sync;
    execution::exec::ExecutionSettings settings{};
    task_manager->AddTask(std::make_unique<task::TaskDML>(
        catalog::INVALID_DATABASE_OID, "SELECT * FROM noisepage_forecast_frequencies",
        std::make_unique<optimizer::TrivialCostModel>(), std::move(params), std::move(param_types), freq_check, nullptr,
        settings, false, true, std::nullopt, common::ManagedPointer(&sync)));

    auto sync_result = sync.DangerousWait();
    bool result = sync_result.second;
    EXPECT_TRUE(result && "SELECT frequencies should have succeeded");
    EXPECT_TRUE(seen == combined && "Incorrect number recorded");
    EXPECT_TRUE(qid_map.size() == qids.size() && "Incorrect number qids recorded");

    for (auto &info : qid_map) {
      EXPECT_TRUE(info.first < qids.size() && "Incorrect qid recorded");
      EXPECT_TRUE(info.second.size() == num_interval && "Some interval should not be recorded");
      for (auto &data : info.second) {
        EXPECT_TRUE(data.second < 10 && "Recorded occurrence incorrect");
        EXPECT_TRUE(data.second == static_cast<size_t>(timestamps[info.first][data.first]) &&
                    "Incorrect recorded for interval");
      }
    }
  };
  check_freqs(2);

  // Flush to the database. The [30] should also flush all data.
  raw->WriteToDB(task_manager_, true, 30, nullptr, nullptr);
  task_manager_->WaitForFlush();

  {
    util->BeginTransaction(catalog::INVALID_DATABASE_OID);
    std::unordered_set<int64_t> val;
    auto func = [&val, qids, texts, db_oids, &parameters](const std::vector<execution::sql::Val *> &values) {
      int64_t qid = reinterpret_cast<execution::sql::Integer *>(values[1])->val_;
      EXPECT_TRUE(static_cast<size_t>(qid) < qids.size() && "Invalid qid");
      EXPECT_TRUE(reinterpret_cast<execution::sql::Integer *>(values[0])->val_ == db_oids[qid] && "Invalid db_oid");

      auto *text_val = reinterpret_cast<execution::sql::StringVal *>(values[2]);
      auto text = std::string(text_val->StringView().data() + 1, text_val->StringView().size() - 2);
      EXPECT_TRUE(text == texts[qid] && "Incorrect text recorded");
      EXPECT_TRUE(val.find(qid) == val.end() && "Duplicate qid found");
      val.insert(qid);

      auto type_json = std::string(reinterpret_cast<execution::sql::StringVal *>(values[3])->StringView());

      std::vector<std::string> type_strs;
      for (const auto &param_val : parameters[qid]) {
        auto tstr = type::TypeUtil::TypeIdToString(param_val.GetReturnValueType());
        type_strs.push_back(tstr);
      }

      nlohmann::json j = type_strs;
      std::string correct = j.dump();
      EXPECT_TRUE(type_json == correct && "Invalid parameters recorded");
    };

    execution::exec::ExecutionSettings settings{};
    bool result = util->ExecuteDML("SELECT * FROM noisepage_forecast_texts", nullptr, nullptr, func, nullptr,
                                   std::make_unique<optimizer::TrivialCostModel>(), std::nullopt, settings);
    EXPECT_TRUE(result && "select should have succeeded");
    EXPECT_TRUE(val.size() == qids.size() && "Incorrect number recorded");
    util->EndTransaction(true);
  }

  {
    util->BeginTransaction(catalog::INVALID_DATABASE_OID);
    size_t row_count = 0;
    std::unordered_set<int64_t> seen;
    auto func = [&row_count, qids, &seen](const std::vector<execution::sql::Val *> &values) {
      EXPECT_TRUE(reinterpret_cast<execution::sql::Integer *>(values[0])->val_ == 30 && "Flush timestamp is invalid");
      EXPECT_TRUE(reinterpret_cast<execution::sql::Integer *>(values[1])->val_ < static_cast<int64_t>(qids.size()) &&
                  "Invalid query identifier");
      seen.insert(reinterpret_cast<execution::sql::Integer *>(values[1])->val_);
      row_count++;
    };

    execution::exec::ExecutionSettings settings{};
    bool result = util->ExecuteDML("SELECT * FROM noisepage_forecast_parameters", nullptr, nullptr, func, nullptr,
                                   std::make_unique<optimizer::TrivialCostModel>(), std::nullopt, settings);
    EXPECT_TRUE(result && "Select should have succeeded");
    EXPECT_TRUE(seen.size() == qids.size() && "Must sample at least 1 per qid");
    EXPECT_TRUE(row_count <= qids.size() * num_sample && "Sampling limit exceeded");
    util->EndTransaction(true);
  }
  check_freqs(3);
  metrics_manager_->UnregisterThread();
}

}  // namespace noisepage::selfdriving::pilot::test
