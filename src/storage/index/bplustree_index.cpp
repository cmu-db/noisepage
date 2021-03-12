#include "storage/index/bplustree_index.h"

#include "storage/index/bplustree.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"

namespace noisepage::storage::index {

template <typename KeyType>
BPlusTreeIndex<KeyType>::BPlusTreeIndex(IndexMetadata &&metadata)
    : Index(std::move(metadata)), bplustree_{new BPlusTree<KeyType, TupleSlot>} {}

template <typename KeyType>
size_t BPlusTreeIndex<KeyType>::EstimateHeapUsage() const {
  return bplustree_->EstimateHeapUsage();
}

template <typename KeyType>
bool BPlusTreeIndex<KeyType>::Insert(common::ManagedPointer<transaction::TransactionContext> txn,
                                     const ProjectedRow &tuple, TupleSlot location) {
  NOISEPAGE_ASSERT(!(metadata_.GetSchema().Unique()),
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  const bool result = bplustree_->Insert(bplustree_->GetElement(index_key, location), predicate);

  NOISEPAGE_ASSERT(
      result,
      "non-unique index shouldn't fail to insert. If it did, something went wrong deep inside the BPlusTree itself.");
  // Register an abort action with the txn context in case of rollback
  txn->RegisterAbortAction([=]() {
    const bool UNUSED_ATTRIBUTE result = bplustree_->DeleteElement(bplustree_->GetElement(index_key, location));

    NOISEPAGE_ASSERT(result, "Delete on the index failed.");
  });
  return result;
}

template <typename KeyType>
bool BPlusTreeIndex<KeyType>::InsertUnique(common::ManagedPointer<transaction::TransactionContext> txn,
                                           const ProjectedRow &tuple, TupleSlot location) {
  NOISEPAGE_ASSERT(metadata_.GetSchema().Unique(), "This Insert is designed for indexes with uniqueness constraints.");
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

  // The predicate checks if any matching keys have write-write conflicts or are still visible to the calling txn.
  auto predicate = [txn](const TupleSlot slot) -> bool {
    const auto *const data_table = slot.GetBlock()->data_table_;
    const auto has_conflict = data_table->HasConflict(*txn, slot);
    const auto is_visible = data_table->IsVisible(*txn, slot);
    return has_conflict || is_visible;
  };

  // Insert a key-value pair
  const bool result = bplustree_->Insert(bplustree_->GetElement(index_key, location), predicate);

  if (result) {
    // Register an abort action with the txn context in case of rollback
    txn->RegisterAbortAction([=]() {
      const bool UNUSED_ATTRIBUTE result = bplustree_->DeleteElement(bplustree_->GetElement(index_key, location));
      NOISEPAGE_ASSERT(result, "Delete on the index failed.");
    });
  } else {
    // Presumably you've already made modifications to a DataTable (the source of the TupleSlot argument to this
    // function) however, the index found a constraint violation and cannot allow that operation to succeed. For MVCC
    // correctness, this txn must now abort for the GC to clean up the version chain in the DataTable correctly.
    txn->SetMustAbort();
  }

  return result;
}

template <typename KeyType>
void BPlusTreeIndex<KeyType>::Delete(common::ManagedPointer<transaction::TransactionContext> txn,
                                     const ProjectedRow &tuple, TupleSlot location) {
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

  NOISEPAGE_ASSERT(!(location.GetBlock()->data_table_->HasConflict(*txn, location)) &&
                       !(location.GetBlock()->data_table_->IsVisible(*txn, location)),
                   "Called index delete on a TupleSlot that has a conflict with this txn or is still visible.");

  // Register a deferred action for the GC with txn manager. See base function comment.
  txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() {
      const bool UNUSED_ATTRIBUTE result = bplustree_->DeleteElement(bplustree_->GetElement(index_key, location));

      NOISEPAGE_ASSERT(result, "Deferred delete on the index failed.");
    });
  });
}

template <typename KeyType>
void BPlusTreeIndex<KeyType>::ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
                                      std::vector<TupleSlot> *value_list) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");

  std::vector<TupleSlot> results;

  // Build search key
  KeyType index_key;
  index_key.SetFromProjectedRow(key, metadata_, metadata_.GetSchema().GetColumns().size());

  // Perform lookup in BPlusTree
  bplustree_->FindValueOfKey(index_key, &results);

  // Avoid resizing our value_list, even if it means over-provisioning
  value_list->reserve(results.size());

  // Perform visibility check on result
  for (const auto &result : results) {
    if (IsVisible(txn, result)) value_list->emplace_back(result);
  }

  NOISEPAGE_ASSERT(!(metadata_.GetSchema().Unique()) || (metadata_.GetSchema().Unique() && value_list->size() <= 1),
                   "Invalid number of results for unique index.");
}

template <typename KeyType>
void BPlusTreeIndex<KeyType>::ScanAscending(const transaction::TransactionContext &txn, ScanType scan_type,
                                            uint32_t num_attrs, ProjectedRow *low_key, ProjectedRow *high_key,
                                            uint32_t limit, std::vector<TupleSlot> *value_list) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");
  NOISEPAGE_ASSERT(scan_type == ScanType::Closed || scan_type == ScanType::OpenLow || scan_type == ScanType::OpenHigh ||
                       scan_type == ScanType::OpenBoth,
                   "Invalid scan_type passed into BPlusTreeIndex::Scan");

  bool low_key_exists = (scan_type == ScanType::Closed || scan_type == ScanType::OpenHigh);
  bool high_key_exists = (scan_type == ScanType::Closed || scan_type == ScanType::OpenLow);

  // The predicate checks if any matching keys are still visible to the calling txn.
  auto predicate = [&txn](const TupleSlot slot) -> bool { return IsVisible(txn, slot); };

  // Build search keys
  KeyType index_low_key, index_high_key;
  if (low_key_exists) index_low_key.SetFromProjectedRow(*low_key, metadata_, num_attrs);
  if (high_key_exists) index_high_key.SetFromProjectedRow(*high_key, metadata_, num_attrs);

  bool scan_completed = false;

  while (!scan_completed) {
    value_list->clear();
    scan_completed = bplustree_->ScanAscending(index_low_key, index_high_key, low_key_exists, num_attrs,
                                               high_key_exists, limit, value_list, &metadata_, predicate);
  }
}

template <typename KeyType>
void BPlusTreeIndex<KeyType>::ScanDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                                             const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");

  // Build search keys
  KeyType index_low_key, index_high_key;
  index_low_key.SetFromProjectedRow(low_key, metadata_, metadata_.GetSchema().GetColumns().size());
  index_high_key.SetFromProjectedRow(high_key, metadata_, metadata_.GetSchema().GetColumns().size());

  bool scan_completed = false;
  std::vector<TupleSlot> results;

  while (!scan_completed) {
    results.clear();
    scan_completed = bplustree_->ScanDescending(index_low_key, index_high_key, &results);
  }

  for (const auto &result : results) {
    if (IsVisible(txn, result)) value_list->emplace_back(result);
  }
}

template <typename KeyType>
void BPlusTreeIndex<KeyType>::ScanLimitDescending(const transaction::TransactionContext &txn,
                                                  const ProjectedRow &low_key, const ProjectedRow &high_key,
                                                  std::vector<TupleSlot> *value_list, uint32_t limit) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");
  NOISEPAGE_ASSERT(limit > 0, "Limit must be greater than 0.");

  // The predicate checks if any matching keys are still visible to the calling txn.
  auto predicate = [&txn](const TupleSlot slot) -> bool { return IsVisible(txn, slot); };

  // Build search keys
  KeyType index_low_key, index_high_key;
  index_low_key.SetFromProjectedRow(low_key, metadata_, metadata_.GetSchema().GetColumns().size());
  index_high_key.SetFromProjectedRow(high_key, metadata_, metadata_.GetSchema().GetColumns().size());

  bool scan_completed = false;
  while (!scan_completed) {
    value_list->clear();
    scan_completed = bplustree_->ScanLimitDescending(index_low_key, index_high_key, value_list, limit, predicate);
  }
}

template <typename KeyType>
uint64_t BPlusTreeIndex<KeyType>::GetSize() const {
  return bplustree_->GetSize();
}

template class BPlusTreeIndex<CompactIntsKey<8>>;
template class BPlusTreeIndex<CompactIntsKey<16>>;
template class BPlusTreeIndex<CompactIntsKey<24>>;
template class BPlusTreeIndex<CompactIntsKey<32>>;

template class BPlusTreeIndex<GenericKey<64>>;
template class BPlusTreeIndex<GenericKey<128>>;
template class BPlusTreeIndex<GenericKey<256>>;
template class BPlusTreeIndex<GenericKey<512>>;

}  // namespace noisepage::storage::index
