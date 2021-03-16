#include <memory>
#include <pqxx/pqxx>  // NOLINT
#include <random>
#include <string>
#include <thread>  //NOLINT
#include <unordered_map>
#include <utility>

#include "main/db_main.h"
#include "metrics/metrics_manager.h"
#include "metrics/metrics_store.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "storage/sql_table.h"
#include "test_util/catalog_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace noisepage::metrics {

/**
 * @brief Test the correctness of database metric
 */
class MetricsTests : public TerrierTest {
 public:
  std::unique_ptr<DBMain> db_main_;
  storage::SqlTable *sql_table_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<MetricsManager> metrics_manager_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  uint16_t port_;
  common::ManagedPointer<catalog::Catalog> catalog_;

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
    db_main_->GetMetricsThread()->PauseMetrics();  // We want to aggregate them manually, so pause the thread.
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    sql_table_ = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), table_schema_);

    port_ = static_cast<uint16_t>(db_main_->GetSettingsManager()->GetInt(settings::Param::port));
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  }
  void TearDown() override {
    db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table_; });
  }

  std::default_random_engine generator_;

  const catalog::Schema table_schema_{
      {{"attribute", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER)}}};

  void Insert() {
    static storage::ProjectedRowInitializer tuple_initializer =
        sql_table_->InitializerForProjectedRow({catalog::col_oid_t(0)});
    auto *const insert_txn = txn_manager_->BeginTransaction();
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
    sql_table_->Insert(common::ManagedPointer(insert_txn), insert_redo);
    txn_manager_->Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}
};

/**
 *  Testing logging metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, LoggingCSVTest) {
  for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::logging_metrics_enable, true, common::ManagedPointer(action_context),
                             setter_callback);

  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  const auto aggregated_data = reinterpret_cast<LoggingMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(MetricsComponent::LOGGING)).get());
  EXPECT_NE(aggregated_data, nullptr);
  EXPECT_GE(aggregated_data->serializer_data_.size(), 0);  // 1 data point recorded
  if (!(aggregated_data->serializer_data_.empty())) {
    EXPECT_GE(aggregated_data->serializer_data_.begin()->num_records_, 0);  // 2 records: insert, commit
  }
  EXPECT_GE(aggregated_data->consumer_data_.size(), 0);  // 1 data point recorded
  if (!(aggregated_data->consumer_data_.empty())) {
    EXPECT_GE(aggregated_data->consumer_data_.begin()->num_buffers_, 0);  // 1 buffer flushed
  }
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 0);
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 0);

  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  EXPECT_GE(aggregated_data->serializer_data_.size(), 0);  // 1 data point recorded
  if (!(aggregated_data->serializer_data_.empty())) {
    EXPECT_GE(aggregated_data->serializer_data_.begin()->num_records_, 0);  // 4 records: 2 insert, 2 commit
  }
  EXPECT_GE(aggregated_data->consumer_data_.size(), 0);  // 1 data point recorded
  if (!(aggregated_data->consumer_data_.empty())) {
    EXPECT_GE(aggregated_data->consumer_data_.begin()->num_buffers_, 0);  // 2 buffers flushed
  }
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 0);
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 0);

  Insert();
  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  EXPECT_GE(aggregated_data->serializer_data_.size(), 0);  // 1 data point recorded
  if (!(aggregated_data->serializer_data_.empty())) {
    EXPECT_GE(aggregated_data->serializer_data_.begin()->num_records_, 0);  // 6 records: 3 insert, 3 commit
  }
  EXPECT_GE(aggregated_data->consumer_data_.size(), 0);  // 1 data point recorded
  if (!(aggregated_data->consumer_data_.empty())) {
    EXPECT_GE(aggregated_data->consumer_data_.begin()->num_buffers_, 0);  // 3 buffers flushed
  }
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 0);
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 0);

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  settings_manager_->SetBool(settings::Param::logging_metrics_enable, false, common::ManagedPointer(action_context),
                             setter_callback);
}

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, TransactionCSVTest) {
  for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::transaction_metrics_enable, true, common::ManagedPointer(action_context),
                             setter_callback);

  metrics_manager_->RegisterThread();

  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  const auto aggregated_data = reinterpret_cast<TransactionMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(MetricsComponent::TRANSACTION)).get());
  EXPECT_NE(aggregated_data, nullptr);
  EXPECT_GE(aggregated_data->begin_data_.size(), 0);   // 1 txn recorded
  EXPECT_GE(aggregated_data->commit_data_.size(), 0);  // 1 txn recorded
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 0);
  EXPECT_EQ(aggregated_data->commit_data_.size(), 0);

  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  EXPECT_GE(aggregated_data->begin_data_.size(), 0);   // 2 txns recorded
  EXPECT_GE(aggregated_data->commit_data_.size(), 0);  // 2 txns recorded
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 0);
  EXPECT_EQ(aggregated_data->commit_data_.size(), 0);

  Insert();
  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  EXPECT_GE(aggregated_data->begin_data_.size(), 0);   // 3 txns recorded
  EXPECT_GE(aggregated_data->commit_data_.size(), 0);  // 3 txns recorded
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 0);
  EXPECT_EQ(aggregated_data->commit_data_.size(), 0);

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  settings_manager_->SetBool(settings::Param::transaction_metrics_enable, false, common::ManagedPointer(action_context),
                             setter_callback);

  metrics_manager_->UnregisterThread();
}

/**
 *  Testing pipeline metrics, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, PipelineCSVTest) {
  // Unlink all files
  for (const auto &file : metrics::QueryTraceMetricRawData::FILES) unlink(std::string(file).c_str());

  // Function to connect via [port] and execute [num_inserts].
  // If [create_table], then the table is also created.
  auto insert_txn = [](uint16_t port, bool create_table, int num_inserts) {
    try {
      pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                              port, catalog::DEFAULT_DATABASE));

      pqxx::work txn1(connection);
      if (create_table) {
        txn1.exec("CREATE TABLE TableA (id INT, data TEXT);");
      }

      for (int i = 0; i < num_inserts; i++) {
        txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
      }
      txn1.commit();
    } catch (const std::exception &e) {
      EXPECT_TRUE(false);
    }
  };

  auto verify_scenario = [insert_txn, this](bool enable_metric, bool update_interval, int interval, int inserts,
                                            int expected_points) {
    const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
    if (enable_metric) {
      // Enable metric if necessary
      auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
      settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                                 setter_callback);
    }

    if (update_interval) {
      // Set the sampling interval correctly
      auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
      settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, interval,
                                common::ManagedPointer(action_context), setter_callback);
    }

    // Perform specified number of inserts
    insert_txn(port_, false, inserts);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    metrics_manager_->Aggregate();

    // If metrics is disabled, we expect this to be null
    const auto aggregated_data = reinterpret_cast<PipelineMetricRawData *>(
        metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(MetricsComponent::EXECUTION_PIPELINE)).get());
    EXPECT_EQ(aggregated_data != nullptr, enable_metric);
    if (aggregated_data != nullptr) {
      EXPECT_EQ(aggregated_data->pipeline_data_.size(), expected_points);
      metrics_manager_->ToCSV();

      // After ToCSV(), we should expect no more data points
      EXPECT_EQ(aggregated_data->pipeline_data_.size(), 0);
    }

    if (enable_metric) {
      // Disable metrics
      auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
      settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, false,
                                 common::ManagedPointer(action_context), setter_callback);
    }
  };

  db_main_->GetNetworkLayer()->GetServer()->RunServer();

  // Create table
  insert_txn(port_, true, 0);

  // Enable, rate = 100, 5 inserts means 5 recorded data points
  verify_scenario(true, true, 100, 5, 5);

  // Disable, rate = 50, 5 inserts means 0 recorded data points
  verify_scenario(false, true, 50, 5, 0);

  // Enable, keep rate, 100 inserts means 50 recorded data points
  verify_scenario(true, false, 100, 100, 50);

  // Enable, rate = 25, 100 inserts means 25 recorded data points
  verify_scenario(true, true, 25, 100, 25);
}

/**
 *  Testing logging query trace metrics, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, QueryCSVTest) {
  for (const auto &file : metrics::QueryTraceMetricRawData::FILES) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::query_trace_metrics_enable, true, common::ManagedPointer(action_context),
                             setter_callback);

  db_main_->GetNetworkLayer()->GetServer()->RunServer();

  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            port_, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");
    txn1.exec("SELECT * FROM TableA");
    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));

  metrics_manager_->Aggregate();
  const auto aggregated_data = reinterpret_cast<QueryTraceMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(MetricsComponent::QUERY_TRACE)).get());
  EXPECT_NE(aggregated_data, nullptr);
  EXPECT_EQ(aggregated_data->query_trace_.size(), 2);  // 2 data point recorded
  EXPECT_EQ(aggregated_data->query_text_.size(), 2);   // 2 data point recorded
  if (!(aggregated_data->query_text_.empty())) {
    EXPECT_EQ(aggregated_data->query_text_.begin()->query_text_, "\"INSERT INTO TableA VALUES (1, 'abc');\"");
    if (!(aggregated_data->query_trace_.empty())) {
      EXPECT_EQ(aggregated_data->query_trace_.begin()->query_id_,
                aggregated_data->query_text_.begin()->query_id_);  // 2 records: insert, select
    }
  }
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->query_trace_.size(), 0);
  EXPECT_EQ(aggregated_data->query_text_.size(), 0);

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  settings_manager_->SetBool(settings::Param::query_trace_metrics_enable, false, common::ManagedPointer(action_context),
                             setter_callback);
}

/**
 *  Testing that we can enable and disable per-component metrics
 *
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, ToggleSettings) {
  // logging_metrics_enable
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::LOGGING));
  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  const auto callback = +[](common::ManagedPointer<common::ActionContext> action_context) -> void {
    action_context->SetState(common::ActionState::SUCCESS);
  };
  settings_manager_->SetBool(settings::Param::logging_metrics_enable, true, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::LOGGING));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  settings_manager_->SetBool(settings::Param::logging_metrics_enable, false, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::LOGGING));

  // transaction_metrics_enable
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::TRANSACTION));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager_->SetBool(settings::Param::transaction_metrics_enable, true, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::TRANSACTION));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  settings_manager_->SetBool(settings::Param::transaction_metrics_enable, false, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::TRANSACTION));

  // gc_metrics_enable
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::GARBAGECOLLECTION));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  settings_manager_->SetBool(settings::Param::gc_metrics_enable, true, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::GARBAGECOLLECTION));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager_->SetBool(settings::Param::gc_metrics_enable, false, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::GARBAGECOLLECTION));

  // execution_metrics_enable
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::EXECUTION));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(7));
  settings_manager_->SetBool(settings::Param::execution_metrics_enable, true, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::EXECUTION));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(8));
  settings_manager_->SetBool(settings::Param::execution_metrics_enable, false, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::EXECUTION));

  // pipeline_metrics_enable
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::EXECUTION_PIPELINE));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(9));
  settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::EXECUTION_PIPELINE));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(10));
  settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::EXECUTION_PIPELINE));

  // query_trace_metrics_enable
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::QUERY_TRACE));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(11));
  settings_manager_->SetBool(settings::Param::query_trace_metrics_enable, true, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::QUERY_TRACE));
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(12));
  settings_manager_->SetBool(settings::Param::query_trace_metrics_enable, false, common::ManagedPointer(action_context),
                             callback);
  EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
  EXPECT_FALSE(metrics_manager_->ComponentEnabled(metrics::MetricsComponent::QUERY_TRACE));
}
}  // namespace noisepage::metrics
