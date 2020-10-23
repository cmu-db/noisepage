#include "storage/index/bwtree_index.h"

#include "bwtree/bwtree.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"

namespace noisepage::storage::index {

template <typename KeyType>
BwTreeIndex<KeyType>::BwTreeIndex(IndexMetadata metadata)
    : Index(std::move(metadata)), bwtree_(std::make_unique<third_party::bwtree::BwTree<KeyType, TupleSlot>>(false)) {}

template <typename KeyType>
void BwTreeIndex<KeyType>::PerformGarbageCollection() {
  bwtree_->PerformGarbageCollection();
}

template <typename KeyType>
size_t BwTreeIndex<KeyType>::EstimateHeapUsage() const {
  // This is a back-of-the-envelope calculation that could be innacurate: it does not account for deltas within the
  // BwTree. Also the mapping table is anonymously mmap'd so getting its exact size is tricky.
  constexpr auto max_elements_per_node = std::max(INNER_NODE_SIZE_UPPER_THRESHOLD,
                                                  LEAF_NODE_SIZE_UPPER_THRESHOLD);  // constant from bwtree.h

  // Calculate the size of each node in the BwTree
  constexpr auto node_size = sizeof(KeyType) + sizeof(uint64_t) +                            // low_key
                             sizeof(KeyType) + sizeof(uint64_t) +                            // high key
                             +sizeof(uintptr_t) +                                            // end pointer
                             max_elements_per_node * sizeof(std::pair<KeyType, uint64_t>) +  // elements in the node
                             third_party::bwtree::BwTree<KeyType, TupleSlot>::AllocationMeta::CHUNK_SIZE();

  // Read the size of the recycled node ID data structure
  bwtree_->node_id_list_lock.lock();
  const auto node_id_list_size = bwtree_->node_id_list.size();
  bwtree_->node_id_list_lock.unlock();

  // Compute the total number of live nodes
  const auto number_of_nodes = bwtree_->next_unused_node_id.load() - node_id_list_size;

  // Compute the size of the primary data structures in the BwTree
  const auto mapping_table_size = number_of_nodes * sizeof(uintptr_t);
  const auto tree_size = number_of_nodes * node_size;

  return mapping_table_size + tree_size;
}

template <typename KeyType>
bool BwTreeIndex<KeyType>::Insert(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const ProjectedRow &tuple, const TupleSlot location) {
  NOISEPAGE_ASSERT(!(metadata_.GetSchema().Unique()),
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());
  const bool result = bwtree_->Insert(index_key, location, false);

  NOISEPAGE_ASSERT(
      result,
      "non-unique index shouldn't fail to insert. If it did, something went wrong deep inside the BwTree itself.");
  // TODO(wuwenw): transaction context is not thread safe for now, and a latch is used here to protect it, may need
  // a better way
  common::SpinLatch::ScopedSpinLatch guard(&transaction_context_latch_);
  // Register an abort action with the txn context in case of rollback
  txn->RegisterAbortAction([=]() {
    const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
    NOISEPAGE_ASSERT(result, "Delete on the index failed.");
  });
  return result;
}

template <typename KeyType>
bool BwTreeIndex<KeyType>::InsertUnique(const common::ManagedPointer<transaction::TransactionContext> txn,
                                        const ProjectedRow &tuple, const TupleSlot location) {
  NOISEPAGE_ASSERT(metadata_.GetSchema().Unique(), "This Insert is designed for indexes with uniqueness constraints.");
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());
  bool predicate_satisfied = false;

  // The predicate checks if any matching keys have write-write conflicts or are still visible to the calling txn.
  auto predicate = [txn](const TupleSlot slot) -> bool {
    const auto *const data_table = slot.GetBlock()->data_table_;
    const auto has_conflict = data_table->HasConflict(*txn, slot);
    const auto is_visible = data_table->IsVisible(*txn, slot);
    return has_conflict || is_visible;
  };

  const bool result = bwtree_->ConditionalInsert(index_key, location, predicate, &predicate_satisfied);

  NOISEPAGE_ASSERT(predicate_satisfied != result, "If predicate is not satisfied then insertion should succeed.");

  if (result) {
    // TODO(wuwenw): transaction context is not thread safe for now, and a latch is used here to protect it, may need
    // a better way
    common::SpinLatch::ScopedSpinLatch guard(&transaction_context_latch_);
    // Register an abort action with the txn context in case of rollback
    txn->RegisterAbortAction([=]() {
      const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
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
void BwTreeIndex<KeyType>::Delete(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const ProjectedRow &tuple, const TupleSlot location) {
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

  NOISEPAGE_ASSERT(!(location.GetBlock()->data_table_->HasConflict(*txn, location)) &&
                       !(location.GetBlock()->data_table_->IsVisible(*txn, location)),
                   "Called index delete on a TupleSlot that has a conflict with this txn or is still visible.");

  // Register a deferred action for the GC with txn manager. See base function comment.
  txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() {
      const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
      NOISEPAGE_ASSERT(result, "Deferred delete on the index failed.");
    });
  });
}

template <typename KeyType>
void BwTreeIndex<KeyType>::ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
                                   std::vector<TupleSlot> *value_list) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");

  std::vector<TupleSlot> results;

  // Build search key
  KeyType index_key;
  index_key.SetFromProjectedRow(key, metadata_, metadata_.GetSchema().GetColumns().size());

  // Perform lookup in BwTree
  bwtree_->GetValue(index_key, results);

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
void BwTreeIndex<KeyType>::ScanAscending(const transaction::TransactionContext &txn, ScanType scan_type,
                                         uint32_t num_attrs, ProjectedRow *low_key, ProjectedRow *high_key,
                                         uint32_t limit, std::vector<TupleSlot> *value_list) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");
  NOISEPAGE_ASSERT(scan_type == ScanType::Closed || scan_type == ScanType::OpenLow || scan_type == ScanType::OpenHigh ||
                       scan_type == ScanType::OpenBoth,
                   "Invalid scan_type passed into BwTreeIndex::Scan");

  bool low_key_exists = (scan_type == ScanType::Closed || scan_type == ScanType::OpenHigh);
  bool high_key_exists = (scan_type == ScanType::Closed || scan_type == ScanType::OpenLow);

  // Build search keys
  KeyType index_low_key, index_high_key;
  if (low_key_exists) index_low_key.SetFromProjectedRow(*low_key, metadata_, num_attrs);
  if (high_key_exists) index_high_key.SetFromProjectedRow(*high_key, metadata_, num_attrs);

  // Perform lookup in BwTree
  auto scan_itr = low_key_exists ? bwtree_->Begin(index_low_key) : bwtree_->Begin();

  // Limit of 0 indicates "no limit"
  while ((limit == 0 || value_list->size() < limit) && !scan_itr.IsEnd() &&
         (!high_key_exists || scan_itr->first.PartialLessThan(index_high_key, &metadata_, num_attrs))) {
    // Perform visibility check on result
    if (IsVisible(txn, scan_itr->second)) value_list->emplace_back(scan_itr->second);
    scan_itr++;
  }
}

template <typename KeyType>
void BwTreeIndex<KeyType>::BwTreeIndex::ScanDescending(const transaction::TransactionContext &txn,
                                                       const ProjectedRow &low_key, const ProjectedRow &high_key,
                                                       std::vector<TupleSlot> *value_list) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");

  // Build search keys
  KeyType index_low_key, index_high_key;
  index_low_key.SetFromProjectedRow(low_key, metadata_, metadata_.GetSchema().GetColumns().size());
  index_high_key.SetFromProjectedRow(high_key, metadata_, metadata_.GetSchema().GetColumns().size());

  // Perform lookup in BwTree
  auto scan_itr = bwtree_->Begin(index_high_key);
  // Back up one element if we didn't match the high key
  // This currently uses the BwTree's decrement operator on the iterator, which is not guaranteed to be
  // constant time. In some cases it may be faster to do an ascending scan and then reverse the result vector. It
  // depends on the visibility selectivity and final result set size. We can change the implementation in the future
  // if it proves to be a problem.
  if (scan_itr.IsEnd() || bwtree_->KeyCmpGreater(scan_itr->first, index_high_key)) scan_itr--;

  while (!scan_itr.IsREnd() && (bwtree_->KeyCmpGreaterEqual(scan_itr->first, index_low_key))) {
    // Perform visibility check on result
    if (IsVisible(txn, scan_itr->second)) value_list->emplace_back(scan_itr->second);
    scan_itr--;
  }
}

template <typename KeyType>
void BwTreeIndex<KeyType>::ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                                               const ProjectedRow &high_key, std::vector<TupleSlot> *value_list,
                                               const uint32_t limit) {
  NOISEPAGE_ASSERT(value_list->empty(), "Result set should begin empty.");
  NOISEPAGE_ASSERT(limit > 0, "Limit must be greater than 0.");

  // Build search keys
  KeyType index_low_key, index_high_key;
  index_low_key.SetFromProjectedRow(low_key, metadata_, metadata_.GetSchema().GetColumns().size());
  index_high_key.SetFromProjectedRow(high_key, metadata_, metadata_.GetSchema().GetColumns().size());

  // Perform lookup in BwTree
  auto scan_itr = bwtree_->Begin(index_high_key);
  // Back up one element if we didn't match the high key, see comment on line 152.
  if (scan_itr.IsEnd() || bwtree_->KeyCmpGreater(scan_itr->first, index_high_key)) scan_itr--;

  while (value_list->size() < limit && !scan_itr.IsREnd() &&
         (bwtree_->KeyCmpGreaterEqual(scan_itr->first, index_low_key))) {
    // Perform visibility check on result
    if (IsVisible(txn, scan_itr->second)) value_list->emplace_back(scan_itr->second);
    scan_itr--;
  }
}

template <typename KeyType>
uint64_t BwTreeIndex<KeyType>::GetSize() const {
  return bwtree_->GetSize();
}

template class BwTreeIndex<CompactIntsKey<8>>;
template class BwTreeIndex<CompactIntsKey<16>>;
template class BwTreeIndex<CompactIntsKey<24>>;
template class BwTreeIndex<CompactIntsKey<32>>;

template class BwTreeIndex<GenericKey<64>>;
template class BwTreeIndex<GenericKey<128>>;
template class BwTreeIndex<GenericKey<256>>;
template class BwTreeIndex<GenericKey<512>>;

}  // namespace noisepage::storage::index
