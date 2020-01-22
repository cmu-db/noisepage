#include "storage/block_compactor.h"

#include <unordered_map>
#include <vector>

#include "common/hash_util.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"

#define EXPORT_TABLE_NAME "test_table.arrow"
#define CSV_TABLE_NAME "test_table.csv"
#define PYSCRIPT_NAME "transform_table.py"
#define PYSCRIPT "import pyarrow as pa\n"                                                        \
                 "pa_table = pa.ipc.open_stream('" EXPORT_TABLE_NAME "').read_next_batch()\n"    \
                 "pandas_table = pa_table.to_pandas()\n"                                         \
                 "pandas_table.to_csv('" CSV_TABLE_NAME "', index=False, header=False)\n"

namespace terrier {

struct ExportTableTest : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  double percent_empty_ = 0.5;
};

// NOLINTNEXTLINE
TEST_F(ExportTableTest, ExportDictionaryCompressedTableTest) {
  unlink(EXPORT_TABLE_NAME);
  unlink(CSV_TABLE_NAME);
  unlink(PYSCRIPT_NAME);
  std::ofstream outfile(PYSCRIPT_NAME, std::ios_base::out);
  outfile << PYSCRIPT;
  outfile.close();
  generator_.seed(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count());
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
  storage::TupleAccessStrategy accessor(layout);
  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(&block_store_, layout, storage::layout_version_t(0));
  storage::RawBlock *block = table.begin()->GetBlock();
  accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

  // Enable GC to cleanup transactions started by the block compactor
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
  transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                              common::ManagedPointer(&deferred_action_manager),
                                              common::ManagedPointer(&buffer_pool_), true, DISABLED};
  storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                               common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                               DISABLED};
  auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);
  auto num_tuples = tuples.size();

  // Manually populate the block header's arrow metadata for test initialization
  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);

  std::vector<type::TypeId> column_types;
  column_types.reserve(layout.NumColumns());

  for (storage::col_id_t col_id : layout.AllColumns()) {
    if (layout.IsVarlen(col_id)) {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::DICTIONARY_COMPRESSED;
      column_types[!col_id] = type::TypeId::VARCHAR;
    } else {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
      column_types[!col_id] = type::TypeId::INTEGER;
    }
  }

  storage::BlockCompactor compactor;
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // compaction pass

  // Need to prune the version chain in order to make sure that the second pass succeeds
  gc.PerformGarbageCollection();
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // gathering pass

  table.ExportTable(EXPORT_TABLE_NAME, &column_types);
  system((std::string("python ") + PYSCRIPT_NAME).c_str());

  std::ifstream csv_file(CSV_TABLE_NAME, std::ios_base::in);
  auto initializer =
      storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *read_row = initializer.InitializeRow(buffer);
  // This transaction is guaranteed to start after the compacting one commits
  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
  std::string line;
  std::stringstream  lineStream(line);
  std::string item;

  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
    storage::TupleSlot slot(block, i);
    if (i < num_tuples) {
      EXPECT_TRUE(table.Select(common::ManagedPointer(txn), slot, read_row));  // Should be filled after compaction
      for (uint32_t j = 0; j < layout.NumColumns(); j++) {
        auto col_id = read_row->ColumnIds()[j];
        std::getline(lineStream, item, ',');
        auto data = read_row->AccessWithNullCheck(j);
        if (data == nullptr) {
          EXPECT_EQ(item, "");
        } else {
          if (layout.IsVarlen(col_id)) {
            auto *varlen = reinterpret_cast<storage::VarlenEntry *>(data);
            EXPECT_EQ(item, std::string(reinterpret_cast<const char *>(varlen->Content())));
          } else {
            auto integer = *reinterpret_cast<int64_t *>(data);
            EXPECT_EQ(std::stol(item), integer);
          }
        }
      }
    }
  }
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  csv_file.close();
  delete[] buffer;

  unlink(EXPORT_TABLE_NAME);
  for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();  // Second call to deallocate.
}

// NOLINTNEXTLINE
TEST_F(ExportTableTest, ExportVarlenTableTest) {
  unlink(EXPORT_TABLE_NAME);
  unlink(CSV_TABLE_NAME);
  unlink(PYSCRIPT_NAME);
  std::ofstream outfile(PYSCRIPT_NAME, std::ios_base::out);
  outfile << PYSCRIPT;
  outfile.close();
  generator_.seed(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count());
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
  storage::TupleAccessStrategy accessor(layout);
  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(&block_store_, layout, storage::layout_version_t(0));
  storage::RawBlock *block = table.begin()->GetBlock();
  accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

  // Enable GC to cleanup transactions started by the block compactor
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
  transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                              common::ManagedPointer(&deferred_action_manager),
                                              common::ManagedPointer(&buffer_pool_), true, DISABLED};
  storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                               common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                               DISABLED};
  auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);

  // Manually populate the block header's arrow metadata for test initialization
  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);

  std::vector<type::TypeId> column_types;
  column_types.reserve(layout.NumColumns());

  for (storage::col_id_t col_id : layout.AllColumns()) {
    if (layout.IsVarlen(col_id)) {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::GATHERED_VARLEN;
      column_types[!col_id] = type::TypeId::VARCHAR;
    } else {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
      column_types[!col_id] = type::TypeId::INTEGER;
    }
  }

  storage::BlockCompactor compactor;
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // compaction pass

  // Need to prune the version chain in order to make sure that the second pass succeeds
  gc.PerformGarbageCollection();
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // gathering pass

  table.ExportTable(EXPORT_TABLE_NAME, &column_types);
  system((std::string("python ") + PYSCRIPT_NAME).c_str());

  unlink(EXPORT_TABLE_NAME);
  for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();  // Second call to deallocate.
}

}  // namespace terrier
