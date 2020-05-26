#include <memory>
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

namespace terrier::metrics {

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

  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);
    db_main_ = terrier::DBMain::Builder()
                   .SetUseSettingsManager(true)
                   .SetSettingsParameterMap(std::move(param_map))
                   .SetUseGC(true)
                   .Build();
    settings_manager_ = db_main_->GetSettingsManager();
    metrics_manager_ = db_main_->GetMetricsManager();
    db_main_->GetMetricsThread()->PauseMetrics();  // We want to aggregate them manually, so pause the thread.
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    sql_table_ = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), table_schema_);
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
  settings_manager_->SetBool(settings::Param::metrics_logging, true, common::ManagedPointer(action_context),
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
  settings_manager_->SetBool(settings::Param::metrics_logging, false, common::ManagedPointer(action_context),
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
  settings_manager_->SetBool(settings::Param::metrics_transaction, true, common::ManagedPointer(action_context),
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
  settings_manager_->SetBool(settings::Param::metrics_transaction, false, common::ManagedPointer(action_context),
                             setter_callback);

  metrics_manager_->UnregisterThread();
}
}  // namespace terrier::metrics
