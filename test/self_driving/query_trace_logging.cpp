#include "common/json.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "metrics/query_trace_metric.h"
#include "test_util/test_harness.h"

namespace noisepage::selfdriving::pilot::test {

class QueryTraceLogging : public TerrierTest {
  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);
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
    metrics_manager_->SetMetricOutput(metrics::MetricsComponent::QUERY_TRACE, metrics::MetricsOutput::CSV_DB);
    db_main_->GetMetricsThread()->PauseMetrics();  // We want to aggregate them manually, so pause the thread.
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    db_main_->TryLoadStartupDDL();
  }

  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

 protected:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
};

// NOLINTNEXTLINE
TEST_F(QueryTraceLogging, BasicLogging) {
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
  metrics_manager_->ToOutput();

  {
    auto thread = db_main_->GetQueryInternalThread();
    util::ExecuteRequest req;
    common::Future<bool> future;
    req.type_ = util::RequestType::SYNC;
    req.notify_ = common::ManagedPointer(&future);
    thread->AddRequest(std::move(req));
    future.Wait();
  }

  auto util = db_main_->GetQueryExecUtil();
  auto select_count = [util](const std::string &query, size_t target) {
    util->BeginTransaction();
    uint64_t row_count = 0;
    auto to_row_fn = [&row_count](const std::vector<execution::sql::Val *> &values) { row_count++; };

    bool result = util->ExecuteDML(query, nullptr, nullptr, to_row_fn, nullptr);
    NOISEPAGE_ASSERT(result, "SELECT should have succeeded");
    NOISEPAGE_ASSERT(row_count == target, "Row count incorrect");
    util->EndTransaction(true);
  };

  // These are only updated on a forecast interval
  select_count("SELECT * FROM noisepage_forecast_texts", 0);
  select_count("SELECT * FROM noisepage_forecast_parameters", 0);

  auto check_freqs = [util, &qids, &totals, &timestamps](size_t num_interval) {
    util->BeginTransaction();
    size_t seen = 0;
    std::unordered_map<size_t, std::unordered_map<size_t, size_t>> qid_map;
    auto freq_check = [&seen, &qid_map](const std::vector<execution::sql::Val *> &values) {
      NOISEPAGE_ASSERT(reinterpret_cast<execution::sql::Integer *>(values[0])->val_ == 1,
                       "Planning iteration should be 1");

      // Record <qid, <interval, seen>>
      qid_map[reinterpret_cast<execution::sql::Integer *>(values[1])->val_]
             [reinterpret_cast<execution::sql::Integer *>(values[2])->val_] +=
          reinterpret_cast<execution::sql::Real *>(values[3])->val_;

      // Sum the number seen
      seen += reinterpret_cast<execution::sql::Real *>(values[3])->val_;
    };

    size_t combined = 0;
    for (size_t i = 0; i < num_interval; i++) {
      combined += totals[i];
    }

    bool result =
        util->ExecuteDML("SELECT * FROM noisepage_forecast_frequencies", nullptr, nullptr, freq_check, nullptr);
    NOISEPAGE_ASSERT(result, "SELECT frequencies should have succeeded");
    NOISEPAGE_ASSERT(seen == combined, "Incorrect number recorded");
    NOISEPAGE_ASSERT(qid_map.size() == qids.size(), "Incorrect number qids recorded");

    for (auto &info : qid_map) {
      NOISEPAGE_ASSERT(info.first < qids.size(), "Incorrect qid recorded");
      NOISEPAGE_ASSERT(info.second.size() == num_interval, "3rd interval should not be recorded");
      for (auto &data : info.second) {
        NOISEPAGE_ASSERT(data.first < 10, "Recorded occurrence incorrect");
        NOISEPAGE_ASSERT(data.second == static_cast<size_t>(timestamps[info.first][data.first]),
                         "Incorrect recorded for interval");
      }
    }

    util->EndTransaction(true);
  };
  check_freqs(2);

  // Flush to the database
  auto raw = reinterpret_cast<metrics::QueryTraceMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(metrics::MetricsComponent::QUERY_TRACE)).get());
  raw->WriteToDB(common::ManagedPointer(db_main_->GetQueryExecUtil()),
                 common::ManagedPointer(db_main_->GetQueryInternalThread()), true, true, nullptr, nullptr);
  std::this_thread::sleep_for(std::chrono::seconds(3));

  {
    util->BeginTransaction();
    std::unordered_set<int64_t> val;
    auto func = [&val, qids, texts, db_oids, &parameters](const std::vector<execution::sql::Val *> &values) {
      int64_t qid = reinterpret_cast<execution::sql::Integer *>(values[1])->val_;
      NOISEPAGE_ASSERT(static_cast<size_t>(qid) < qids.size(), "Invalid qid");
      NOISEPAGE_ASSERT(reinterpret_cast<execution::sql::Integer *>(values[0])->val_ == db_oids[qid], "Invalid db_oid");

      auto *text_val = reinterpret_cast<execution::sql::StringVal *>(values[2]);
      auto text = std::string(text_val->StringView().data() + 1, text_val->StringView().size() - 2);
      NOISEPAGE_ASSERT(text == texts[qid], "Incorrect text recorded");
      NOISEPAGE_ASSERT(val.find(qid) == val.end(), "Duplicate qid found");
      val.insert(qid);

      auto type_json = std::string(reinterpret_cast<execution::sql::StringVal *>(values[3])->StringView());

      std::vector<std::string> type_strs;
      for (const auto &val : parameters[qid]) {
        auto tstr = type::TypeUtil::TypeIdToString(val.GetReturnValueType());
        type_strs.push_back(tstr);
      }

      nlohmann::json j = type_strs;
      std::string correct = j.dump();
      NOISEPAGE_ASSERT(type_json == correct, "Invalid parameters recorded");
    };

    bool result = util->ExecuteDML("SELECT * FROM noisepage_forecast_texts", nullptr, nullptr, func, nullptr);
    NOISEPAGE_ASSERT(result, "select should have succeeded");
    NOISEPAGE_ASSERT(val.size() == qids.size(), "Incorrect number recorded");
    util->EndTransaction(true);
  }

  {
    util->BeginTransaction();
    size_t row_count = 0;
    std::unordered_set<int64_t> seen;
    auto func = [&row_count, qids, &seen](const std::vector<execution::sql::Val *> &values) {
      NOISEPAGE_ASSERT(reinterpret_cast<execution::sql::Integer *>(values[0])->val_ == 1, "Iteration invalid");
      NOISEPAGE_ASSERT(reinterpret_cast<execution::sql::Integer *>(values[1])->val_ < static_cast<int64_t>(qids.size()),
                       "Invalid query identifier");
      seen.insert(reinterpret_cast<execution::sql::Integer *>(values[1])->val_);
      row_count++;
    };

    bool result = util->ExecuteDML("SELECT * FROM noisepage_forecast_parameters", nullptr, nullptr, func, nullptr);
    NOISEPAGE_ASSERT(result, "Select should have succeeded");
    NOISEPAGE_ASSERT(seen.size() == qids.size(), "Must sample at least 1 per qid");
    NOISEPAGE_ASSERT(row_count <= qids.size() * num_sample, "Sampling limit exceeded");
    util->EndTransaction(true);
  }
  check_freqs(3);
  metrics_manager_->UnregisterThread();
}

}  // namespace noisepage::selfdriving::pilot::test
