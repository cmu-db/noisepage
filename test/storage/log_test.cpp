#include "execution/sql_table.h"
#include "storage/data_table.h"
#include "storage/log_manager.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
#include "gtest/gtest.h"

namespace terrier {
class LargeLoggingTests : public TerrierTest {
 public:
  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_.Process();
    log_manager_.Flush();
  }

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{1000, 100};
  storage::BlockStore block_store_{100, 100};
  storage::LogManager log_manager_{"test.txt", &pool_};
  transaction::TransactionManager txn_manager_{&pool_, false, &log_manager_};
  std::thread log_thread_;
  bool logging_;
 private:
  void LogThreadLoop(uint32_t log_period_milli) {
    while (logging_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
      log_manager_.Process();
    }
  }
};

// NOLINTNEXTLINE
TEST_F(LargeLoggingTests, LargeLogTest) {
//  LargeTransactionTestObject
//      tested(2, 10, 1, {1.0, 0.0}, &block_store_, &pool_, &generator_, false, false, &log_manager_);
//  StartLogging(1);
//  tested.SimulateOltp(100, 4);
//  EndLogging();
  storage::BlockLayout layout(4, {8, 8, 8, 4});
  storage::DataTable table(&block_store_, layout, layout_version_t(0));
  auto *txn = txn_manager_.BeginTransaction();
  storage::ProjectedRowInitializer init(layout, {col_id_t(1), col_id_t(2), col_id_t(3)});
  auto *record = txn->StageWrite(nullptr, tuple_id_t(0), init);
  *reinterpret_cast<uint64_t *>(record->Delta()->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<uint64_t *>(record->Delta()->AccessForceNotNull(1)) = 721;
  record->Delta()->SetNull(2);
  table.Insert(txn, *record->Delta());
  txn_manager_.Commit(txn, [] {});
  log_manager_.Process();
  log_manager_.Flush();
  delete txn;
}
}  // namespace terrier
