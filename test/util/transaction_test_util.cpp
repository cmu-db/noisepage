#include "util/transaction_test_util.h"
#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>
#include "common/allocator.h"
#include "transaction/transaction_util.h"
#include "util/catalog_test_util.h"

namespace terrier {
RandomWorkloadTransaction::RandomWorkloadTransaction(LargeTransactionTestObject *test_object)
    : test_object_(test_object),
      txn_(test_object->txn_manager_->BeginTransaction()),
      aborted_(false),
      start_time_(txn_->StartTime()),
      commit_time_(UINT64_MAX) {}

RandomWorkloadTransaction::~RandomWorkloadTransaction() {
  if (!test_object_->gc_on_) delete txn_;
  for (auto &entry : updates_) delete[] reinterpret_cast<byte *>(entry.second);
  for (auto &entry : selects_) delete[] reinterpret_cast<byte *>(entry.second);
}

template<class Random>
void RandomWorkloadTransaction::RandomUpdate(Random *generator) {
  if (aborted_) return;
  storage::TupleSlot updated =
      RandomTestUtil::UniformRandomElement(test_object_->last_checked_version_, generator)->first;
  auto it = updates_.find(updated);
  // don't double update if checking for correctness, as it is complicated to keep track of on snapshots,
  // and not very helpful in finding bugs anyways
  if (it != updates_.end()) return;

  std::vector<storage::col_id_t> update_col_ids =
      StorageTestUtil::ProjectionListRandomColumns(test_object_->layout_, generator);
  storage::ProjectedRowInitializer initializer =
      storage::ProjectedRowInitializer::Create(test_object_->layout_, update_col_ids);
  auto *update_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  storage::ProjectedRow *update = initializer.InitializeRow(update_buffer);

  StorageTestUtil::PopulateRandomRow(update, test_object_->layout_, 0.0, generator);

  updates_[updated] = update;

  // TODO(Tianyu): Hardly efficient, but will do for testing.
  auto *record = txn_->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, initializer);
  record->SetTupleSlot(updated);
  std::memcpy(reinterpret_cast<void *>(record->Delta()), update, update->Size());
  auto result = test_object_->table_.Update(txn_, updated, *update);
  aborted_ = !result;
}

template<class Random>
void RandomWorkloadTransaction::RandomSelect(Random *generator) {
  if (aborted_) return;
  storage::TupleSlot selected =
      RandomTestUtil::UniformRandomElement(test_object_->last_checked_version_, generator)->first;
  auto *select_buffer = common::AllocationUtil::AllocateAligned(test_object_->row_initializer_.ProjectedRowSize());
  storage::ProjectedRow *select = test_object_->row_initializer_.InitializeRow(select_buffer);
  test_object_->table_.Select(txn_, selected, select);
  auto updated = updates_.find(selected);
  // Only track reads whose value depend on the snapshot
  if (updated == updates_.end())
    selects_.emplace_back(selected, select);
  else
    delete[] select_buffer;
}

void RandomWorkloadTransaction::Finish() {
  if (aborted_)
    test_object_->txn_manager_->Abort(txn_);
  else
    commit_time_ = test_object_->txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

LargeTransactionTestObject::LargeTransactionTestObject(LargeTransactionTestConfiguration config,
                                                       storage::BlockStore *block_store,
                                                       transaction::TransactionManager *txn_manager,
                                                       std::default_random_engine *generator,
                                                       storage::LogManager *log_manager)
    : txn_length_(config.txn_length_),
      update_select_ratio_(config.update_select_ratio_),
      generator_(generator),
      layout_(config.varlen_allowed_ ? StorageTestUtil::RandomLayoutWithVarlens(config.max_columns_, generator_)
                                     : StorageTestUtil::RandomLayoutNoVarlen(config.max_columns_, generator_)),
      table_(block_store, layout_, storage::layout_version_t(0)),
      txn_manager_(txn_manager),
      gc_on_(txn_manager->GCEnabled()),
      wal_on_(log_manager != LOGGING_DISABLED) {
  // Bootstrap the table to have the specified number of tuples
  PopulateInitialTable(config.initial_table_size_, generator_);
}

LargeTransactionTestObject::~LargeTransactionTestObject() {
  if (!gc_on_) delete initial_txn_;
  for (auto &tuple : last_checked_version_) delete[] reinterpret_cast<byte *>(tuple.second);
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
SimulationResult LargeTransactionTestObject::SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns) {
  std::vector<RandomWorkloadTransaction *> txns;
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;
  txns.resize(num_transactions);
  // Either for correctness checking, or to cleanup memory afterwards, we need to retain these
  // test objects
  workload = [&](uint32_t) {
    for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
      txns[txn_id] = new RandomWorkloadTransaction(this);
      SimulateOneTransaction(txns[txn_id], txn_id);
    }
  };
  common::WorkerPool thread_pool(num_concurrent_txns, {});
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_concurrent_txns, workload);

  // filter out aborted transactions
  std::vector<RandomWorkloadTransaction *> committed, aborted;
  for (RandomWorkloadTransaction *txn : txns) (txn->aborted_ ? aborted : committed).push_back(txn);

  // Sort according to commit timestamp (Although we probably already are? Never hurts to be sure)
  std::sort(committed.begin(), committed.end(), [](RandomWorkloadTransaction *a, RandomWorkloadTransaction *b) {
    return transaction::TransactionUtil::NewerThan(b->commit_time_, a->commit_time_);
  });
  return {committed, aborted};
}

void LargeTransactionTestObject::CheckReadsCorrect(std::vector<RandomWorkloadTransaction *> *commits) {
  // If nothing commits, then all our reads are vacuously correct
  if (commits->empty()) return;
  VersionedSnapshots snapshots = ReconstructVersionedTable(commits);
  // make sure table_version is updated
  transaction::timestamp_t latest_version = commits->at(commits->size() - 1)->commit_time_;
  // Only need to check that reads make sense?
  for (RandomWorkloadTransaction *txn : *commits) CheckTransactionReadCorrect(txn, snapshots);

  // clean up memory, update the kept version to be the latest.
  for (auto &snapshot : snapshots) {
    if (snapshot.first == latest_version) {
      UpdateLastCheckedVersion(snapshot.second);
    } else {
      for (auto &entry : snapshot.second) delete[] reinterpret_cast<byte *>(entry.second);
    }
  }
}

void LargeTransactionTestObject::SimulateOneTransaction(terrier::RandomWorkloadTransaction *txn, uint32_t txn_id) {
  std::default_random_engine thread_generator(txn_id);

  auto update = [&] { txn->RandomUpdate(&thread_generator); };
  auto select = [&] { txn->RandomSelect(&thread_generator); };
  RandomTestUtil::InvokeWorkloadWithDistribution({update, select}, update_select_ratio_, &thread_generator,
                                                 txn_length_);
  txn->Finish();
}

template<class Random>
void LargeTransactionTestObject::PopulateInitialTable(uint32_t num_tuples, Random *generator) {
  initial_txn_ = txn_manager_->BeginTransaction();
  byte *redo_buffer = nullptr;
  for (uint32_t i = 0; i < num_tuples; i++) {
    // get a new redo buffer each insert so we log the values inserted.
    redo_buffer = common::AllocationUtil::AllocateAligned(row_initializer_.ProjectedRowSize());
    // reinitialize every time
    storage::ProjectedRow *redo = row_initializer_.InitializeRow(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, 0.0, generator);
    storage::TupleSlot inserted = table_.Insert(initial_txn_, *redo);
    // TODO(Tianyu): Hardly efficient, but will do for testing.
    auto *record =
        initial_txn_->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, row_initializer_);
    record->SetTupleSlot(inserted);
    std::memcpy(reinterpret_cast<void *>(record->Delta()), redo, redo->Size());
    last_checked_version_.emplace_back(inserted, redo);
  }
  txn_manager_->Commit(initial_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

storage::ProjectedRow *LargeTransactionTestObject::CopyTuple(storage::ProjectedRow *other) {
  auto *copy = common::AllocationUtil::AllocateAligned(other->Size());
  std::memcpy(copy, other, other->Size());
  return reinterpret_cast<storage::ProjectedRow *>(copy);
}

void LargeTransactionTestObject::UpdateSnapshot(RandomWorkloadTransaction *txn, TableSnapshot *curr,
                                                const TableSnapshot &before) {
  for (auto &entry : before) curr->emplace(entry.first, CopyTuple(entry.second));
  for (auto &update : txn->updates_) {
    // TODO(Tianyu): Can be smarter about copies
    storage::ProjectedRow *new_version = (*curr)[update.first];
    storage::StorageUtil::ApplyDelta(layout_, *update.second, new_version);
  }
}

VersionedSnapshots LargeTransactionTestObject::ReconstructVersionedTable(
    std::vector<RandomWorkloadTransaction *> *txns) {
  VersionedSnapshots result;
  // empty starting version
  TableSnapshot *prev = &(result.emplace(transaction::timestamp_t(0), TableSnapshot()).first->second);
  // populate with initial image of the table
  for (auto &entry : last_checked_version_) (*prev)[entry.first] = CopyTuple(entry.second);

  for (RandomWorkloadTransaction *txn : *txns) {
    auto ret = result.emplace(txn->commit_time_, TableSnapshot());
    UpdateSnapshot(txn, &(ret.first->second), *prev);
    prev = &(ret.first->second);
  }
  return result;
}

void LargeTransactionTestObject::CheckTransactionReadCorrect(RandomWorkloadTransaction *txn,
                                                             const VersionedSnapshots &snapshots) {
  transaction::timestamp_t start_time = txn->start_time_;
  // this version is the most recent future update
  auto ret = snapshots.upper_bound(start_time);
  // Go to the version visible to this txn
  --ret;
  transaction::timestamp_t version_timestamp = ret->first;
  const TableSnapshot &before_snapshot = ret->second;
  EXPECT_TRUE(transaction::TransactionUtil::NewerThan(start_time, version_timestamp));
  for (auto &entry : txn->selects_) {
    auto it = before_snapshot.find(entry.first);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(layout_, entry.second, it->second));
  }
}

void LargeTransactionTestObject::UpdateLastCheckedVersion(const TableSnapshot &snapshot) {
  for (auto &entry : last_checked_version_) {
    delete[] reinterpret_cast<byte *>(entry.second);
    entry.second = snapshot.find(entry.first)->second;
  }
}


}  // namespace terrier
