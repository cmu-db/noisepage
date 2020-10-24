#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/block_compactor.h"
#include "storage/garbage_collector.h"
#include "test_util/storage_test_util.h"
#include "transaction/deferred_action_manager.h"

namespace noisepage {
class BlockCompactorBenchmark : public benchmark::Fixture {
 protected:
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  storage::BlockLayout layout_{{8, 8, storage::VARLEN_COLUMN}};
  storage::TupleAccessStrategy accessor_{layout_};

  storage::DataTable table_{common::ManagedPointer(&block_store_), layout_, storage::layout_version_t(0)};
  transaction::TimestampManager timestamp_manager_;
  transaction::DeferredActionManager deferred_action_manager_{common::ManagedPointer(&timestamp_manager_)};
  transaction::TransactionManager txn_manager_{common::ManagedPointer(&timestamp_manager_),
                                               common::ManagedPointer(&deferred_action_manager_),
                                               common::ManagedPointer(&buffer_pool_), true, DISABLED};
  storage::GarbageCollector gc_{common::ManagedPointer(&timestamp_manager_),
                                common::ManagedPointer(&deferred_action_manager_),
                                common::ManagedPointer(&txn_manager_), nullptr};
  storage::BlockCompactor compactor_;

  uint32_t num_blocks_ = 500;

  // NOLINTNEXTLINE
  void RunFull(benchmark::State &state, double percent_empty, storage::ArrowColumnType type) {
    uint32_t num_tuples = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
      num_tuples = 0;
      std::vector<storage::RawBlock *> blocks;
      for (uint32_t i = 0; i < num_blocks_; i++) {
        storage::RawBlock *block = block_store_.Get();
        block->data_table_ = &table_;
        num_tuples += StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(&table_, block, percent_empty, &generator_);
        auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
        for (storage::col_id_t col_id : layout_.AllColumns()) {
          if (layout_.IsVarlen(col_id))
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = type;
          else
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
        }
        blocks.push_back(block);
      }
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t compaction_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&compaction_ms);
        compactor_.ProcessCompactionQueue(&deferred_action_manager_, &txn_manager_);
      }
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t gather_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&gather_ms);
        compactor_.ProcessCompactionQueue(&deferred_action_manager_, &txn_manager_);
      }
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(gather_ms + compaction_ms) / 1000.0);
    }
    state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));  // NOLINT
  }

  // NOLINTNEXTLINE
  void RunCompaction(benchmark::State &state, double percent_empty, storage::ArrowColumnType type) {
    // NOLINTNEXTLINE
    for (auto _ : state) {
      std::vector<storage::RawBlock *> blocks;
      for (uint32_t i = 0; i < num_blocks_; i++) {
        storage::RawBlock *block = block_store_.Get();
        block->data_table_ = &table_;
        StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(&table_, block, percent_empty, &generator_);
        auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
        for (storage::col_id_t col_id : layout_.AllColumns()) {
          if (layout_.IsVarlen(col_id))
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = type;
          else
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
        }
        blocks.push_back(block);
      }
      // generate our table and instantiate GC
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        compactor_.ProcessCompactionQueue(&deferred_action_manager_, &txn_manager_);
      }
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }
    state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));  // NOLINT
  }

  // NOLINTNEXTLINE
  void RunGather(benchmark::State &state, double percent_empty, storage::ArrowColumnType type) {
    // NOLINTNEXTLINE
    for (auto _ : state) {
      std::vector<storage::RawBlock *> blocks;
      for (uint32_t i = 0; i < num_blocks_; i++) {
        storage::RawBlock *block = block_store_.Get();
        block->data_table_ = &table_;
        StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(&table_, block, percent_empty, &generator_);
        auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
        for (storage::col_id_t col_id : layout_.AllColumns()) {
          if (layout_.IsVarlen(col_id))
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = type;
          else
            arrow_metadata.GetColumnInfo(layout_, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
        }
        blocks.push_back(block);
      }
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      compactor_.ProcessCompactionQueue(&deferred_action_manager_, &txn_manager_);
      gc_.PerformGarbageCollection();
      gc_.PerformGarbageCollection();
      for (storage::RawBlock *block : blocks) compactor_.PutInQueue(block);
      uint64_t time;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&time);
        compactor_.ProcessCompactionQueue(&deferred_action_manager_, &txn_manager_);
      }
      for (storage::RawBlock *block : blocks) block_store_.Release(block);
      state.SetIterationTime(static_cast<double>(time) / 1000.0);
    }
    state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));  // NOLINT
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Compaction)(benchmark::State &state) {
  // Run compaction phase only
  RunCompaction(state, 0.1, storage::ArrowColumnType::GATHERED_VARLEN);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Gather)(benchmark::State &state) {
  // Run varlen gather phase only
  RunGather(state, 0.1, storage::ArrowColumnType::GATHERED_VARLEN);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, DictionaryCompression)(benchmark::State &state) {
  // Run dictionary compression only
  RunGather(state, 0.1, storage::ArrowColumnType::DICTIONARY_COMPRESSED);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, EndToEndGather)(benchmark::State &state) {
  // Run end to end with varlen gather backend.
  RunFull(state, 0.1, storage::ArrowColumnType::GATHERED_VARLEN);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, EndToEndDict)(benchmark::State &state) {
  // Run end to end with dictionary compression backend.
  RunFull(state, 0.1, storage::ArrowColumnType::DICTIONARY_COMPRESSED);
}

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Compaction)->Unit(benchmark::kMillisecond)->UseManualTime();

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Gather)->Unit(benchmark::kMillisecond)->UseManualTime();

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, DictionaryCompression)->Unit(benchmark::kMillisecond)->UseManualTime();

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, EndToEndGather)->Unit(benchmark::kMillisecond)->UseManualTime();
}  // namespace noisepage
