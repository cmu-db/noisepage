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

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier::metrics {

/**
 * @brief Test the correctness of database metric
 */
class MetricsTests : public TerrierTest {
 public:
  DBMain *db_main_;
  settings::SettingsManager *settings_manager_;
  MetricsManager *metrics_manager_;
  transaction::TransactionManager *txn_manager_;

  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = new DBMain(std::move(param_map));
    settings_manager_ = db_main_->settings_manager_;
    metrics_manager_ = db_main_->metrics_manager_;
    txn_manager_ = db_main_->txn_manager_;
  }

  void TearDown() override {
    delete db_main_;
    delete sql_table_;
  }

  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 1;
  const uint8_t num_txns_ = 100;

  storage::BlockStore block_store_{100, 100};

  const catalog::Schema table_schema_{
      {{"attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER))}}};
  storage::SqlTable *const sql_table_{new storage::SqlTable(&block_store_, table_schema_)};
  const storage::ProjectedRowInitializer tuple_initializer_{
      sql_table_->InitializerForProjectedRow({catalog::col_oid_t(0)})};

  void Insert() {
    auto *const insert_txn = txn_manager_->BeginTransaction();
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer_);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
    sql_table_->Insert(insert_txn, insert_redo);
    txn_manager_->Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  static void EmptySetterCallback(const std::shared_ptr<common::ActionContext> &action_context UNUSED_ATTRIBUTE) {}
};

/**
 *  Testing logging metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, LoggingCSVTest) {
  for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context =
      std::make_shared<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::metrics_logging, true, action_context, setter_callback);

  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  metrics_manager_->Aggregate();
  const auto aggregated_data = reinterpret_cast<LoggingMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(MetricsComponent::LOGGING)).get());
  EXPECT_NE(aggregated_data, nullptr);
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 1);                 // 1 data point recorded
  EXPECT_EQ(aggregated_data->serializer_data_.begin()->num_records_, 2);  // 2 records: insert, commit
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 1);                   // 1 data point recorded
  EXPECT_EQ(aggregated_data->consumer_data_.begin()->num_buffers_, 1);    // 1 buffer flushed
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 0);
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 0);

  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  metrics_manager_->Aggregate();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 1);                 // 1 data point recorded
  EXPECT_EQ(aggregated_data->serializer_data_.begin()->num_records_, 4);  // 4 records: 2 insert, 2 commit
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 1);                   // 1 data point recorded
  EXPECT_EQ(aggregated_data->consumer_data_.begin()->num_buffers_, 2);    // 2 buffers flushed
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 0);
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 0);

  Insert();
  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  metrics_manager_->Aggregate();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 1);                 // 1 data point recorded
  EXPECT_EQ(aggregated_data->serializer_data_.begin()->num_records_, 6);  // 6 records: 3 insert, 3 commit
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 1);                   // 1 data point recorded
  EXPECT_EQ(aggregated_data->consumer_data_.begin()->num_buffers_, 3);    // 3 buffers flushed
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->serializer_data_.size(), 0);
  EXPECT_EQ(aggregated_data->consumer_data_.size(), 0);
}

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, TransactionCSVTest) {
  for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context =
      std::make_shared<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::metrics_transaction, true, action_context, setter_callback);

  metrics_manager_->RegisterThread();

  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  metrics_manager_->Aggregate();
  const auto aggregated_data = reinterpret_cast<TransactionMetricRawData *>(
      metrics_manager_->AggregatedMetrics().at(static_cast<uint8_t>(MetricsComponent::TRANSACTION)).get());
  EXPECT_NE(aggregated_data, nullptr);
  EXPECT_EQ(aggregated_data->begin_data_.size(), 1);   // 1 txn recorded
  EXPECT_EQ(aggregated_data->commit_data_.size(), 1);  // 1 txn recorded
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 0);
  EXPECT_EQ(aggregated_data->commit_data_.size(), 0);

  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  metrics_manager_->Aggregate();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 2);   // 2 txns recorded
  EXPECT_EQ(aggregated_data->commit_data_.size(), 2);  // 2 txns recorded
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 0);
  EXPECT_EQ(aggregated_data->commit_data_.size(), 0);

  Insert();
  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  metrics_manager_->Aggregate();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 3);   // 3 txns recorded
  EXPECT_EQ(aggregated_data->commit_data_.size(), 3);  // 3 txns recorded
  metrics_manager_->ToCSV();
  EXPECT_EQ(aggregated_data->begin_data_.size(), 0);
  EXPECT_EQ(aggregated_data->commit_data_.size(), 0);

  metrics_manager_->UnregisterThread();
}
}  // namespace terrier::metrics
