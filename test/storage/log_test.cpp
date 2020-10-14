#include <future>  // NOLINT
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/dedicated_thread_registry.h"
#include "common/managed_pointer.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "test_util/catalog_test_util.h"
#include "test_util/data_table_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

#define LOG_TEST_LOG_FILE_NAME "./test_log_test.log"

namespace terrier::storage {
class WriteAheadLoggingTests : public TerrierTest {
 protected:
  std::default_random_engine generator_;
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<storage::LogManager> log_manager_;
  common::ManagedPointer<storage::BlockStore> store_;

  void SetUp() override {
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_TEST_LOG_FILE_NAME);

    db_main_ =
        terrier::DBMain::Builder().SetWalFilePath(LOG_TEST_LOG_FILE_NAME).SetUseLogging(true).SetUseGC(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    log_manager_ = db_main_->GetLogManager();
    store_ = db_main_->GetStorageLayer()->GetBlockStore();
  }

  void TearDown() override {
    log_manager_->Start();  // all of the tests stop the LogManager, but DBMain's teardown logic expects it to still be
                            // running, so we'll just restart it
    // Delete log file
    unlink(LOG_TEST_LOG_FILE_NAME);
  }

  static void TestCommitCallback(void *const callback_arg) {
    auto *const promise = reinterpret_cast<std::promise<bool> *const>(callback_arg);
    promise->set_value(true);
  }

  /**
   * @warning If the serialization format of logs ever changes, this function will need to be updated.
   */
  storage::LogRecord *ReadNextRecord(storage::BufferedLogReader *in) {
    auto size = in->ReadValue<uint32_t>();
    byte *buf = common::AllocationUtil::AllocateAligned(size);
    auto record_type = in->ReadValue<storage::LogRecordType>();
    auto txn_begin = in->ReadValue<transaction::timestamp_t>();
    if (record_type == storage::LogRecordType::COMMIT) {
      auto txn_commit = in->ReadValue<transaction::timestamp_t>();
      auto oldest_active_txn = in->ReadValue<transaction::timestamp_t>();

      // Okay to fill in null since nobody will invoke the callback.
      // is_read_only argument is set to false, because we do not write out a commit record for a transaction if it is
      // not read-only.
      return storage::CommitRecord::Initialize(buf, txn_begin, txn_commit, nullptr, nullptr, oldest_active_txn, false,
                                               nullptr, nullptr);
    }

    if (record_type == storage::LogRecordType::ABORT)
      return storage::AbortRecord::Initialize(buf, txn_begin, nullptr, nullptr);

    auto database_oid = in->ReadValue<catalog::db_oid_t>();
    auto table_oid = in->ReadValue<catalog::table_oid_t>();
    auto tuple_slot = in->ReadValue<storage::TupleSlot>();

    if (record_type == storage::LogRecordType::DELETE) {
      return storage::DeleteRecord::Initialize(buf, txn_begin, database_oid, table_oid, tuple_slot);
    }

    // If code path reaches here, we have a REDO record.
    TERRIER_ASSERT(record_type == storage::LogRecordType::REDO, "Unknown record type during test deserialization");

    // Read in col_ids
    // IDs read individually since we can't guarantee memory layout of vector
    auto num_cols = in->ReadValue<uint16_t>();
    std::vector<storage::col_id_t> col_ids(num_cols);
    for (uint16_t i = 0; i < num_cols; i++) {
      const auto col_id = in->ReadValue<storage::col_id_t>();
      col_ids[i] = col_id;
    }

    // Read in attribute size boundaries
    std::vector<uint16_t> attr_size_boundaries;
    attr_size_boundaries.reserve(NUM_ATTR_BOUNDARIES);
    for (uint16_t i = 0; i < NUM_ATTR_BOUNDARIES; i++) {
      attr_size_boundaries.push_back(in->ReadValue<uint16_t>());
    }

    // Compute attr sizes
    std::vector<uint16_t> attr_sizes;
    attr_sizes.reserve(num_cols);
    for (uint16_t attr_idx = 0; attr_idx < num_cols; attr_idx++) {
      attr_sizes.push_back(storage::StorageUtil::AttrSizeFromBoundaries(attr_size_boundaries, attr_idx));
    }

    // Initialize the redo record.
    auto initializer = storage::ProjectedRowInitializer::Create(attr_sizes, col_ids);
    auto *result = storage::RedoRecord::Initialize(buf, txn_begin, database_oid, table_oid, initializer);
    auto *record_body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
    record_body->SetTupleSlot(tuple_slot);
    auto *delta = record_body->Delta();

    // Get an in memory copy of the record's null bitmap. Note: this is used to guide how the rest of the log file is
    // read in. It doesn't populate the delta's bitmap yet. This will happen naturally as we proceed column-by-column.
    auto bitmap_num_bytes = common::RawBitmap::SizeInBytes(num_cols);
    auto *bitmap_buffer = new uint8_t[bitmap_num_bytes];
    in->Read(bitmap_buffer, bitmap_num_bytes);
    auto *bitmap = reinterpret_cast<common::RawBitmap *>(bitmap_buffer);

    for (uint16_t i = 0; i < num_cols; i++) {
      if (!bitmap->Test(i)) {
        // Recall that 0 means null in our definition of a ProjectedRow's null bitmap.
        delta->SetNull(i);
        continue;
      }

      // The column is not null, so set the bitmap accordingly and get access to the column value.
      auto *column_value_address = delta->AccessForceNotNull(i);
      if (attr_sizes[i] == AttrSizeBytes(VARLEN_COLUMN)) {
        // Read how many bytes this varlen actually is.
        const auto varlen_attribute_size = in->ReadValue<uint32_t>();
        // Allocate a varlen buffer of this many bytes.
        auto *varlen_attribute_content = common::AllocationUtil::AllocateAligned(varlen_attribute_size);
        // Fill the entry with the next bytes from the log file.
        in->Read(varlen_attribute_content, varlen_attribute_size);
        // Create the varlen entry depending on whether it can be inlined or not
        storage::VarlenEntry varlen_entry;
        if (varlen_attribute_size <= storage::VarlenEntry::InlineThreshold()) {
          varlen_entry = storage::VarlenEntry::CreateInline(varlen_attribute_content, varlen_attribute_size);
          delete[] varlen_attribute_content;
        } else {
          varlen_entry = storage::VarlenEntry::Create(varlen_attribute_content, varlen_attribute_size, true);
        }
        // The attribute value in the ProjectedRow will be a pointer to this varlen entry.
        auto *dest = reinterpret_cast<storage::VarlenEntry *>(column_value_address);
        // Set the value to be the address of the varlen_entry.
        *dest = varlen_entry;
      } else {
        // For inlined attributes, just directly read into the ProjectedRow.
        in->Read(column_value_address, attr_sizes[i]);
      }
    }

    // Free the memory allocated for the bitmap.
    delete[] bitmap_buffer;

    return result;
  }

  storage::RedoBuffer &GetRedoBuffer(transaction::TransactionContext *txn) { return txn->redo_buffer_; }
};

// This test uses the LargeDataTableTestObject to simulate some number of transactions with logging turned on, and
// then reads the logged out content to make sure they are correct
// NOLINTNEXTLINE
TEST_F(WriteAheadLoggingTests, LargeLogTest) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumTxns(100)
                    .SetNumConcurrentTxns(4)
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(5)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(5)
                    .SetVarlenAllowed(true)
                    .Build();
  auto *const tested =
      new LargeDataTableTestObject(config, store_.Get(), txn_manager_.Get(), &generator_, log_manager_.Get());
  // Each transaction does 5 operations. The update-select ratio of operations is 50%-50%.
  auto result = tested->SimulateOltp(100, 4);
  log_manager_->PersistAndStop();

  std::unordered_map<transaction::timestamp_t, RandomDataTableTransaction *> txns_map;
  for (auto *txn : result.first) txns_map[txn->BeginTimestamp()] = txn;
  // At this point all the log records should have been written out, we can start reading stuff back in.
  storage::BufferedLogReader in(LOG_TEST_LOG_FILE_NAME);
  while (in.HasMore()) {
    storage::LogRecord *log_record = ReadNextRecord(&in);
    if (log_record->TxnBegin() == transaction::INITIAL_TXN_TIMESTAMP) {
      // TODO(Tianyu): This is hacky, but it will be a pain to extract the initial transaction. The
      // LargeTransactionTest
      //  harness probably needs some refactor (later after wal is in).
      // This the initial setup transaction.
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
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualDeep(tested->Layout(), update_it->second, redo->Delta()));
      delete[] reinterpret_cast<byte *>(update_it->second);
      it->second->Updates()->erase(update_it);
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }

  // Ensure that the only committed transactions which remain in txns_map are read-only, because any other committing
  // transaction will generate a commit record and will be erased from txns_map in the checks above, if log records
  // are properly being written out. If at this point, there is exists any transaction in txns_map which made updates,
  // then something went wrong with logging. Read-only transactions do not generate commit records, so they will
  // remain in txns_map.
  for (const auto &kv_pair : txns_map) {
    EXPECT_TRUE(kv_pair.second->Updates()->empty());
  }

  // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
  // DeferredAction
  db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });

  for (auto *txn : result.first) delete txn;
  for (auto *txn : result.second) delete txn;
}

// This test simulates a series of read-only transactions, and then reads the generated log file back in to ensure
// that read-only transactions do not generate any log records, as they are not necessary for recovery.
// NOLINTNEXTLINE
TEST_F(WriteAheadLoggingTests, ReadOnlyTransactionsGenerateNoLogTest) {
  // Each transaction is read-only (update-select ratio of 0-100). Also, no need for bookkeeping.
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumTxns(100)
                    .SetNumConcurrentTxns(4)
                    .SetUpdateSelectRatio({0.0, 1.0})
                    .SetTxnLength(5)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(5)
                    .SetVarlenAllowed(true)
                    .Build();
  auto *const tested =
      new LargeDataTableTestObject(config, store_.Get(), txn_manager_.Get(), &generator_, log_manager_.Get());
  auto result = tested->SimulateOltp(1000, 4);
  log_manager_->PersistAndStop();

  // Read-only workload has completed. Read the log file back in to check that no records were produced for these
  // transactions.
  int log_records_count = 0;
  storage::BufferedLogReader in(LOG_TEST_LOG_FILE_NAME);
  while (in.HasMore()) {
    storage::LogRecord *log_record = ReadNextRecord(&in);
    if (log_record->TxnBegin() == transaction::INITIAL_TXN_TIMESTAMP) {
      // (TODO) Currently following pattern from LargeLogTest of skipping the initial transaction. When the
      // transaction testing framework changes, fix this.
      delete[] reinterpret_cast<byte *>(log_record);
      continue;
    }

    log_records_count += 1;
    delete[] reinterpret_cast<byte *>(log_record);
  }

  EXPECT_EQ(log_records_count, 0);

  // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
  // DeferredAction
  db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });

  for (auto *txn : result.first) delete txn;
  for (auto *txn : result.second) delete txn;
}

// This test simulates a transaction that has previously flushed its buffer, and then aborts. We then check that it
// correctly flushed out an abort record
// NOLINTNEXTLINE
TEST_F(WriteAheadLoggingTests, AbortRecordTest) {
  // Create SQLTable
  auto col = catalog::Schema::Column("attribute", type::TypeId::INTEGER, false,
                                     parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(0));
  auto table_schema = catalog::Schema(std::vector<catalog::Schema::Column>({col}));
  auto *const sql_table = new storage::SqlTable(store_, table_schema);
  auto tuple_initializer = sql_table->InitializerForProjectedRow({catalog::col_oid_t(0)});

  // Initialize first transaction, this txn will write a single tuple
  auto *first_txn = txn_manager_->BeginTransaction();
  auto *insert_redo =
      first_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 1;
  auto first_tuple_slot = sql_table->Insert(common::ManagedPointer(first_txn), insert_redo);
  EXPECT_TRUE(!first_txn->Aborted());

  // Initialize the second txn, this one will write until it has flushed a buffer to the log manager
  auto second_txn = txn_manager_->BeginTransaction();
  int32_t insert_value = 2;
  while (!GetRedoBuffer(second_txn).HasFlushed()) {
    insert_redo =
        second_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
    insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = insert_value++;
    sql_table->Insert(common::ManagedPointer(second_txn), insert_redo);
  }
  EXPECT_TRUE(GetRedoBuffer(second_txn).HasFlushed());
  EXPECT_TRUE(!second_txn->Aborted());

  // Now the second txn will try to update the tuple the first txn wrote, and thus will abort. We expect this txn to
  // write an abort record
  auto update_redo =
      second_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
  auto update_tuple = update_redo->Delta();
  *reinterpret_cast<int32_t *>(update_tuple->AccessForceNotNull(0)) = 0;
  update_redo->SetTupleSlot(first_tuple_slot);
  EXPECT_FALSE(sql_table->Update(common::ManagedPointer(second_txn), update_redo));

  // Commit first txn and abort the second
  txn_manager_->Commit(first_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  txn_manager_->Abort(second_txn);
  EXPECT_TRUE(second_txn->Aborted());

  // Shut down log manager
  log_manager_->PersistAndStop();

  // Read records, look for the abort record
  bool found_abort_record = false;
  storage::BufferedLogReader in(LOG_TEST_LOG_FILE_NAME);
  while (in.HasMore()) {
    storage::LogRecord *log_record = ReadNextRecord(&in);
    if (log_record->RecordType() == LogRecordType::ABORT) {
      found_abort_record = true;
      auto *abort_record = log_record->GetUnderlyingRecordBodyAs<storage::AbortRecord>();
      EXPECT_EQ(LogRecordType::ABORT, abort_record->RecordType());
      EXPECT_EQ(second_txn->StartTime(), log_record->TxnBegin());
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }
  EXPECT_TRUE(found_abort_record);

  // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
  // DeferredAction
  db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table; });
}

// This test verifies that we don't write an abort record for an aborted transaction that never flushed its redo
// buffer
// NOLINTNEXTLINE
TEST_F(WriteAheadLoggingTests, NoAbortRecordTest) {
  // Create SQLTable
  auto col = catalog::Schema::Column("attribute", type::TypeId::INTEGER, false,
                                     parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(0));
  auto table_schema = catalog::Schema(std::vector<catalog::Schema::Column>({col}));
  auto *const sql_table = new storage::SqlTable(store_, table_schema);
  auto tuple_initializer = sql_table->InitializerForProjectedRow({catalog::col_oid_t(0)});

  // Initialize first transaction, this txn will write a single tuple
  auto *first_txn = txn_manager_->BeginTransaction();
  auto *insert_redo =
      first_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 1;
  auto first_tuple_slot = sql_table->Insert(common::ManagedPointer(first_txn), insert_redo);
  EXPECT_TRUE(!first_txn->Aborted());

  // Initialize the second txn, this txn will try to update the tuple the first txn wrote, and thus will abort. We
  // expect this txn to not write an abort record
  auto second_txn = txn_manager_->BeginTransaction();
  auto update_redo =
      second_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
  auto update_tuple = update_redo->Delta();
  *reinterpret_cast<int32_t *>(update_tuple->AccessForceNotNull(0)) = 0;
  update_redo->SetTupleSlot(first_tuple_slot);
  EXPECT_FALSE(sql_table->Update(common::ManagedPointer(second_txn), update_redo));
  EXPECT_FALSE(GetRedoBuffer(second_txn).HasFlushed());

  // Commit first txn and abort the second
  txn_manager_->Commit(first_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  txn_manager_->Abort(second_txn);
  EXPECT_TRUE(second_txn->Aborted());

  // Shut down log manager
  log_manager_->PersistAndStop();

  // Read records, make sure we don't see an abort record
  bool found_abort_record = false;
  storage::BufferedLogReader in(LOG_TEST_LOG_FILE_NAME);
  while (in.HasMore()) {
    storage::LogRecord *log_record = ReadNextRecord(&in);
    if (log_record->RecordType() == LogRecordType::ABORT) {
      found_abort_record = true;
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }
  EXPECT_FALSE(found_abort_record);

  // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
  // DeferredAction
  db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table; });
}

// Verify that we invoke the callback even for read-only txns. This test checks a bug that was found when sending
// BEGIN; COMMIT; across PSQL and noticing that COMMIT blocked forever with a real callback.
TEST_F(WriteAheadLoggingTests, ReadOnlyCallbackTest) {
  // Create SQLTable
  auto col = catalog::Schema::Column("attribute", type::TypeId::INTEGER, false,
                                     parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(0));
  auto table_schema = catalog::Schema(std::vector<catalog::Schema::Column>({col}));
  auto *const sql_table = new storage::SqlTable(store_, table_schema);

  // Initialize first transaction, this txn will write a single tuple
  auto *const txn = txn_manager_->BeginTransaction();

  // Call commit with a real callback that modifies a future
  std::promise<bool> promise;
  auto future = promise.get_future();
  EXPECT_TRUE(future.valid());
  txn_manager_->Commit(txn, TestCommitCallback, &promise);

  // Shut down log manager
  log_manager_->PersistAndStop();

  // We probably won't end up waiting at all since the PersistAndStop likely flushed it already, but we just want to
  // verify that it was invoked
  EXPECT_TRUE(future.get());

  // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
  // DeferredAction
  db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table; });
}
}  // namespace terrier::storage
