#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "bwtree/bwtree.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage::index {
template <uint8_t KeySize>
class CompactIntsKey;
template <uint16_t KeySize>
class GenericKey;

constexpr uint32_t GC_THRESHOLD = 1000;

/**
 * Wrapper around Ziqi's OpenBwTree.
 * @tparam KeyType the type of keys stored in the BwTree
 */
template <typename KeyType>
class BwTreeIndex final : public Index {
  friend class IndexBuilder;

 private:
  explicit BwTreeIndex(IndexMetadata metadata)
      : Index(std::move(metadata)), bwtree_{new third_party::bwtree::BwTree<KeyType, TupleSlot>{false}} {}

  void IncNumModification(const common::ManagedPointer<transaction::TransactionContext> txn) {
    if (num_mod_ < GC_THRESHOLD) {
      num_mod_++;
    } else {
      num_mod_.store(0);
      txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
        deferred_action_manager->RegisterDeferredAction(
            [=]() {
              bwtree_->PerformGarbageCollection();
            },
            transaction::DafId::MEMORY_DEALLOCATION);
      });
      txn->RegisterAbortAction([=](transaction::DeferredActionManager *deferred_action_manager) {
        deferred_action_manager->RegisterDeferredAction(
            [=]() {
              bwtree_->PerformGarbageCollection();
            },
            transaction::DafId::MEMORY_DEALLOCATION);
      });
    }
  }

  const std::unique_ptr<third_party::bwtree::BwTree<KeyType, TupleSlot>> bwtree_;
  std::atomic<uint32_t> num_mod_ = 0;

 public:
  IndexType Type() const final { return IndexType::BWTREE; }

  void PerformGarbageCollection() final { bwtree_->PerformGarbageCollection(); };

  bool Insert(const common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
              const TupleSlot location) final {
    TERRIER_ASSERT(!(metadata_.GetSchema().Unique()),
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());
    const bool result = bwtree_->Insert(index_key, location, false);
    TERRIER_ASSERT(
        result,
        "non-unique index shouldn't fail to insert. If it did, something went wrong deep inside the BwTree itself.");
    IncNumModification(txn);
    // Register an abort action with the txn context in case of rollback
    txn->RegisterAbortAction([=]() {
      const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
      TERRIER_ASSERT(result, "Delete on the index failed.");
    });
    return result;
  }

  bool InsertUnique(const common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
                    const TupleSlot location) final {
    TERRIER_ASSERT(metadata_.GetSchema().Unique(), "This Insert is designed for indexes with uniqueness constraints.");
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

    TERRIER_ASSERT(predicate_satisfied != result, "If predicate is not satisfied then insertion should succeed.");

    if (result) {
      IncNumModification(txn);
      // Register an abort action with the txn context in case of rollback
      txn->RegisterAbortAction([=]() {
        const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
        TERRIER_ASSERT(result, "Delete on the index failed.");
      });
    } else {
      // Presumably you've already made modifications to a DataTable (the source of the TupleSlot argument to this
      // function) however, the index found a constraint violation and cannot allow that operation to succeed. For MVCC
      // correctness, this txn must now abort for the GC to clean up the version chain in the DataTable correctly.
      txn->SetMustAbort();
    }

    return result;
  }

  void Delete(const common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
              const TupleSlot location) final {
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

    TERRIER_ASSERT(!(location.GetBlock()->data_table_->HasConflict(*txn, location)) &&
                       !(location.GetBlock()->data_table_->IsVisible(*txn, location)),
                   "Called index delete on a TupleSlot that has a conflict with this txn or is still visible.");
    IncNumModification(txn);

    // Register a deferred action for the GC with txn manager. See base function comment.
    txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
      deferred_action_manager->RegisterDeferredAction(
          [=]() {
            const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
            TERRIER_ASSERT(result, "Deferred delete on the index failed.");
          },
          transaction::DafId::INDEX_REMOVE_KEY);
    });
  }

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

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

    TERRIER_ASSERT(!(metadata_.GetSchema().Unique()) || (metadata_.GetSchema().Unique() && value_list->size() <= 1),
                   "Invalid number of results for unique index.");
  }

  void ScanAscending(const transaction::TransactionContext &txn, ScanType scan_type, uint32_t num_attrs,
                     ProjectedRow *low_key, ProjectedRow *high_key, uint32_t limit,
                     std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");
    TERRIER_ASSERT(scan_type == ScanType::Closed || scan_type == ScanType::OpenLow || scan_type == ScanType::OpenHigh ||
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

  void ScanDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                      const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

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

  void ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                           const ProjectedRow &high_key, std::vector<TupleSlot> *value_list,
                           const uint32_t limit) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");
    TERRIER_ASSERT(limit > 0, "Limit must be greater than 0.");

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
};

extern template class BwTreeIndex<CompactIntsKey<8>>;
extern template class BwTreeIndex<CompactIntsKey<16>>;
extern template class BwTreeIndex<CompactIntsKey<24>>;
extern template class BwTreeIndex<CompactIntsKey<32>>;

extern template class BwTreeIndex<GenericKey<64>>;
extern template class BwTreeIndex<GenericKey<128>>;
extern template class BwTreeIndex<GenericKey<256>>;

}  // namespace terrier::storage::index
