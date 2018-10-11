#include <unordered_map>

#include "gtest/gtest.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

#define LOG_FILE_NAME "test.log"

namespace terrier {
class WriteAheadLoggingTests : public TerrierTest {
 public:
  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_.Shutdown();
  }

  void StartGC(transaction::TransactionManager *txn_manager, uint32_t gc_period_milli) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([gc_period_milli, this] { GCThreadLoop(gc_period_milli); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  storage::LogRecord *ReadNextRecord(storage::BufferedLogReader *in) {
    auto size = in->ReadValue<uint32_t>();
    byte *buf = common::AllocationUtil::AllocateAligned(size);
    auto record_type = in->ReadValue<storage::LogRecordType>();
    auto txn_begin = in->ReadValue<timestamp_t>();
    if (record_type == storage::LogRecordType::COMMIT) {
      auto txn_commit = in->ReadValue<timestamp_t>();
      // Okay to fill in null since nobody will invoke the callback
      return storage::CommitRecord::Initialize(buf, txn_begin, txn_commit, nullptr, nullptr);
    }
    // TODO(Tianyu): Without a lookup mechanism this oid is not exactly meaningful. Implement lookup when possible
    auto table_oid UNUSED_ATTRIBUTE = in->ReadValue<table_oid_t>();
    auto tuple_slot = in->ReadValue<storage::TupleSlot>();
    auto result = storage::RedoRecord::PartialInitialize(buf, size, txn_begin,
                                                         // TODO(Tianyu): Hacky as hell
                                                         nullptr, tuple_slot);
    // TODO(Tianyu): For now, without inlined attributes, the delta portion is a straight memory copy. This
    // will obviously change in the future. Also, this is hacky as hell
    auto delta_size = in->ReadValue<uint32_t>();
    byte *dest =
        reinterpret_cast<byte *>(result->GetUnderlyingRecordBodyAs<storage::RedoRecord>()->Delta()) + sizeof(uint32_t);
    in->Read(dest, delta_size - static_cast<uint32_t>(sizeof(uint32_t)));
    return result;
  }

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{1000, 100};
  storage::BlockStore block_store_{100, 100};
  storage::LogManager log_manager_{LOG_FILE_NAME, &pool_};
  std::thread log_thread_;
  bool logging_;
  volatile bool run_gc_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_;

 private:
  void LogThreadLoop(uint32_t log_period_milli) {
    while (logging_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
      log_manager_.Process();
    }
  }

  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      gc_->PerformGarbageCollection();
    }
  }
};

// This test uses the LargeTransactionTestObject to simulate some number of transactions with logging turned on, and
// then reads the logged out content to make sure they are correct
// NOLINTNEXTLINE
TEST_F(WriteAheadLoggingTests, LargeLogTest) {
  LargeTransactionTestObject tested(5, 1, 5, {0.0, 1.0}, &block_store_, &pool_, &generator_, true, true, &log_manager_);
  StartLogging(10);
  StartGC(tested.GetTxnManager(), 10);
  auto result = tested.SimulateOltp(100, 4);
  EndGC();
  EndLogging();

  std::unordered_map<timestamp_t, RandomWorkloadTransaction *> txns_map;
  for (auto *txn : result.first) txns_map[txn->BeginTimestamp()] = txn;
  // At this point all the log records should have been written out, we can start reading stuff back in.
  storage::BufferedLogReader in(LOG_FILE_NAME);
  while (in.HasMore()) {
    storage::LogRecord *log_record = ReadNextRecord(&in);
    if (log_record->TxnBegin() == timestamp_t(0)) {
      // TODO(Tianyu): This is hacky, but it will be a pain to extract the intiail transaction. The LargeTranasctionTest
      // harness probably needs some refactor (later after wal is in)
      // This the initial setup transaction
      delete[] reinterpret_cast<byte *>(log_record);
      continue;
    }

    auto it = txns_map.find(log_record->TxnBegin());
    if (it == txns_map.end()) {
      // Okay to write out aborted transaction's redos, just cannot be a commit
      EXPECT_NE(log_record->RecordType(), storage::LogRecordType::COMMIT);
      delete[] reinterpret_cast<byte *>(log_record);
      continue;
    }
    if (log_record->RecordType() == storage::LogRecordType::COMMIT) {
      EXPECT_EQ(log_record->GetUnderlyingRecordBodyAs<storage::CommitRecord>()->CommitTime(),
                it->second->CommitTimestamp());
      EXPECT_TRUE(it->second->Updates()->empty());  // All previous updates have been logged out previously
      txns_map.erase(it);
    } else {
      // This is leveraging the fact that we don't update the same tuple twice in a transaction with
      // bookkeeping turned on
      auto *redo = log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
      // TODO(Tianyu): The DataTable field cannot be recreated from oid_t yet (we also don't really have oids),
      // so we are not checking it
      auto update_it = it->second->Updates()->find(redo->GetTupleSlot());
      EXPECT_NE(it->second->Updates()->end(), update_it);
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), update_it->second, redo->Delta()));
      it->second->Updates()->erase(update_it);
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }
  // TODO(Tianyu): You will need to account for read-only transactions once we stop logging them out.
  EXPECT_TRUE(txns_map.empty());  // all transactions should have been logged out
  unlink(LOG_FILE_NAME);
  for (auto *txn : result.first) delete txn;
  for (auto *txn : result.second) delete txn;
}
}  // namespace terrier
