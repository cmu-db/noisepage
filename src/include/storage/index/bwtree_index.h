#pragma once

#include <functional>
#include <utility>
#include <vector>
#include "bwtree/bwtree.h"
#include "storage/index/index.h"

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
      : Index(oid, constraint_type, std::move(metadata)),
        bwtree_{new third_party::bwtree::BwTree<KeyType, TupleSlot>{false}} {}

  third_party::bwtree::BwTree<KeyType, TupleSlot> *const bwtree_;

 public:
  ~BwTreeIndex() final { delete bwtree_; }

  bool Insert(const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT,
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);
    return bwtree_->Insert(index_key, location, false);
  }

  bool Delete(const ProjectedRow &tuple, const TupleSlot location) final {
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);
    return bwtree_->Delete(index_key, location);
  }

  bool InsertUnique(const transaction::TransactionContext &txn, const ProjectedRow &tuple,
                    const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::UNIQUE,
                   "This Insert is designed for indexes with uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);
    bool predicate_satisfied = false;

    auto predicate = [&](const TupleSlot slot) -> bool {
      const auto *const data_table = slot.GetBlock()->data_table_;
      return data_table->HasConflict(txn, slot) || data_table->IsVisible(txn, slot);
    };

    const bool ret = bwtree_->ConditionalInsert(index_key, location, predicate, &predicate_satisfied);

    // if predicate is not satisfied then we know insertion succeeds
    if (!predicate_satisfied) {
      TERRIER_ASSERT(ret, "Insertion should always succeed. (Ziqi)");
    } else {
      TERRIER_ASSERT(!ret, "Insertion should always fail. (Ziqi)");
    }

    return ret;
  }

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(
        value_list->empty(),
        "Result set should begin empty. This can be changed in the future if index scan behavior requires it.");

    std::vector<TupleSlot> results;

    // Build search key
    KeyType index_key;
    index_key.SetFromProjectedRow(key, metadata_);

    // Perform lookup in BwTree
    bwtree_->GetValue(index_key, results);

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
    TERRIER_ASSERT(
        value_list->empty(),
        "Result set should begin empty. This can be changed in the future if index scan behavior requires it.");

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
    TERRIER_ASSERT(
        value_list->empty(),
        "Result set should begin empty. This can be changed in the future if index scan behavior requires it.");

    // Build search keys
    KeyType index_low_key, index_high_key;
    index_low_key.SetFromProjectedRow(low_key, metadata_);
    index_high_key.SetFromProjectedRow(high_key, metadata_);

    // Perform lookup in BwTree
    auto scan_itr = bwtree_->Begin(index_high_key);
    // Back up one element if we didn't match the high key
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
    TERRIER_ASSERT(
        value_list->empty(),
        "Result set should begin empty. This can be changed in the future if index scan behavior requires it.");
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
    TERRIER_ASSERT(
        value_list->empty(),
        "Result set should begin empty. This can be changed in the future if index scan behavior requires it.");
    TERRIER_ASSERT(limit > 0, "Limit must be greater than 0.");

    // Build search keys
    KeyType index_low_key, index_high_key;
    index_low_key.SetFromProjectedRow(low_key, metadata_);
    index_high_key.SetFromProjectedRow(high_key, metadata_);

    // Perform lookup in BwTree
    auto scan_itr = bwtree_->Begin(index_high_key);
    // Back up one element if we didn't match the high key
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
