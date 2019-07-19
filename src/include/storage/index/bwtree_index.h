#pragma once

#include <functional>
#include <utility>
#include <vector>
#include "bwtree/bwtree.h"
#include "storage/index/index.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage::index {

/**
 * Wrapper around Ziqi's OpenBwTree.
 * @tparam KeyType the type of keys stored in the BwTree
 */
template <typename KeyType>
class BwTreeIndex final : public Index {
  friend class IndexBuilder;

 private:
  BwTreeIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type, IndexMetadata metadata)
      : Index(constraint_type, std::move(metadata)),
        bwtree_{new third_party::bwtree::BwTree<KeyType, TupleSlot>{false}} {}

  third_party::bwtree::BwTree<KeyType, TupleSlot> *const bwtree_;

 public:
  ~BwTreeIndex() final { delete bwtree_; }

  void PerformGarbageCollection() final { bwtree_->PerformGarbageCollection(); };

  bool Insert(transaction::TransactionContext *const txn, const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT,
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);
    const bool result = bwtree_->Insert(index_key, location, false);

    TERRIER_ASSERT(
        result,
        "non-unique index shouldn't fail to insert. If it did, something went wrong deep inside the BwTree itself.");

    // Register an abort action with the txn context in case of rollback
    txn->RegisterAbortAction([=]() {
      const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
      TERRIER_ASSERT(result, "Delete on the index failed.");
    });

    return result;
  }

  bool InsertUnique(transaction::TransactionContext *const txn, const ProjectedRow &tuple,
                    const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::UNIQUE,
                   "This Insert is designed for indexes with uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);
    bool predicate_satisfied = false;

    // The predicate checks if any matching keys have write-write conflicts or are still visible to the calling txn.
    auto predicate = [&](const TupleSlot slot) -> bool {
      const auto *const data_table = slot.GetBlock()->data_table_;
      return data_table->HasConflict(*txn, slot) || data_table->IsVisible(*txn, slot);
    };

    const bool result = bwtree_->ConditionalInsert(index_key, location, predicate, &predicate_satisfied);

    TERRIER_ASSERT(predicate_satisfied != result, "If predicate is not satisfied then insertion should succeed.");

    if (result) {
      // Register an abort action with the txn context in case of rollback
      txn->RegisterAbortAction([=]() {
        const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
        TERRIER_ASSERT(result, "Delete on the index failed.");
      });
    } else {
      // Presumably you've already made modifications to a DataTable (the source of the TupleSlot argument to this
      // function) however, the index found a constraint violation and cannot allow that operation to succeed. For MVCC
      // correctness, this txn must now abort for the GC to clean up the version chain in the DataTable correctly.
      txn->MustAbort();
    }

    return result;
  }

  void Delete(transaction::TransactionContext *const txn, const ProjectedRow &tuple, const TupleSlot location) final {
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);

    TERRIER_ASSERT(!(location.GetBlock()->data_table_->HasConflict(*txn, location)) &&
                       !(location.GetBlock()->data_table_->IsVisible(*txn, location)),
                   "Called index delete on a TupleSlot that has a conflict with this txn or is still visible.");

    // Register a deferred action for the GC with txn manager. See base function comment.
    auto *const txn_manager = txn->GetTransactionManager();
    txn->RegisterCommitAction([=]() {
      txn_manager->DeferAction([=]() {
        const bool UNUSED_ATTRIBUTE result = bwtree_->Delete(index_key, location);
        TERRIER_ASSERT(result, "Deferred delete on the index failed.");
      });
    });
  }

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

    std::vector<TupleSlot> results;

    // Build search key
    KeyType index_key;
    index_key.SetFromProjectedRow(key, metadata_);

    // Perform lookup in BwTree
    bwtree_->GetValue(index_key, results);

    // Avoid resizing our value_list, even if it means over-provisioning
    value_list->reserve(results.size());

    // Perform visibility check on result
    for (const auto &result : results) {
      if (IsVisible(txn, result)) value_list->emplace_back(result);
    }

    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT ||
                       (GetConstraintType() == ConstraintType::UNIQUE && value_list->size() <= 1),
                   "Invalid number of results for unique index.");
  }

  void ScanAscending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                     const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

    // Build search keys
    KeyType index_low_key, index_high_key;
    index_low_key.SetFromProjectedRow(low_key, metadata_);
    index_high_key.SetFromProjectedRow(high_key, metadata_);

    // Perform lookup in BwTree
    auto scan_itr = bwtree_->Begin(index_low_key);
    while (!scan_itr.IsEnd() && (bwtree_->KeyCmpLessEqual(scan_itr->first, index_high_key))) {
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
    index_low_key.SetFromProjectedRow(low_key, metadata_);
    index_high_key.SetFromProjectedRow(high_key, metadata_);

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

  void ScanLimitAscending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                          const ProjectedRow &high_key, std::vector<TupleSlot> *value_list,
                          const uint32_t limit) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");
    TERRIER_ASSERT(limit > 0, "Limit must be greater than 0.");

    // Build search keys
    KeyType index_low_key, index_high_key;
    index_low_key.SetFromProjectedRow(low_key, metadata_);
    index_high_key.SetFromProjectedRow(high_key, metadata_);

    // Perform lookup in BwTree
    auto scan_itr = bwtree_->Begin(index_low_key);
    while (value_list->size() < limit && !scan_itr.IsEnd() &&
           (bwtree_->KeyCmpLessEqual(scan_itr->first, index_high_key))) {
      // Perform visibility check on result
      if (IsVisible(txn, scan_itr->second)) value_list->emplace_back(scan_itr->second);
      scan_itr++;
    }
  }

  void ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                           const ProjectedRow &high_key, std::vector<TupleSlot> *value_list,
                           const uint32_t limit) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");
    TERRIER_ASSERT(limit > 0, "Limit must be greater than 0.");

    // Build search keys
    KeyType index_low_key, index_high_key;
    index_low_key.SetFromProjectedRow(low_key, metadata_);
    index_high_key.SetFromProjectedRow(high_key, metadata_);

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

}  // namespace terrier::storage::index
