#include <memory>
#include <random>
#include <thread>  //NOLINT
#include <unordered_map>
#include <utility>
#include "main/db_main.h"
#include "metrics/metrics_manager.h"
#include "metrics/metrics_store.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "storage/sql_table.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/test_harness.h"

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
      sql_table_->InitializerForProjectedRow({catalog::col_oid_t(0)}).first};


  void Insert() {
    auto *const insert_txn = txn_manager_->BeginTransaction();
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
    sql_table_->Insert(insert_txn, insert_redo);
    txn_manager_->Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
};

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, LoggingCSVTest) {
  for (const auto &file : metrics::LoggingMetricRawData::files_) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context =
      std::make_shared<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::metrics_logging, true, action_context, setter_callback);

  Insert();

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  metrics_manager_->Aggregate();
  metrics_manager_->ToCSV();

  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  metrics_manager_->Aggregate();
  metrics_manager_->ToCSV();

  Insert();
  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  metrics_manager_->Aggregate();
  metrics_manager_->ToCSV();
}

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, TransactionCSVTest) {
  for (const auto &file : metrics::TransactionMetricRawData::files_) unlink(std::string(file).c_str());
  const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context =
      std::make_shared<common::ActionContext>(common::action_id_t(1));
  settings_manager_->SetBool(settings::Param::metrics_transaction, true, action_context, setter_callback);

  metrics_manager_->RegisterThread();

  Insert();

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  metrics_manager_->Aggregate();
  metrics_manager_->ToCSV();

  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  metrics_manager_->Aggregate();
  metrics_manager_->ToCSV();

  Insert();
  Insert();
  Insert();

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  metrics_manager_->Aggregate();
  metrics_manager_->ToCSV();

  metrics_manager_->UnregisterThread();
}
}  // namespace terrier::metrics
