#pragma once

#include <functional>
#include <utility>
#include <vector>
#include "libcuckoo/cuckoohash_map.hh"
#include "storage/index/index.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage::index {

/**
 * Wrapper around libcuckoo's hash map.
 * @tparam KeyType the type of keys stored in the map
 */
template <typename KeyType>
class HashIndex final : public Index {
  friend class IndexBuilder;

 private:
  HashIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type, IndexMetadata metadata)
      : Index(constraint_type, std::move(metadata)),
        hash_map_{new cuckoohash_map<KeyType, cuckoohash_map<TupleSlot, TupleSlot>>(256)} {}

  cuckoohash_map<KeyType, cuckoohash_map<TupleSlot, TupleSlot>> *const hash_map_;

 public:
  ~HashIndex() final { delete hash_map_; }

  void PerformGarbageCollection() final{};

  bool Insert(transaction::TransactionContext *const txn, const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT,
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);

    bool UNUSED_ATTRIBUTE upsert_result = false;

    auto upsert_fn = [location, &upsert_result](cuckoohash_map<TupleSlot, TupleSlot> &value_map) -> bool {
      upsert_result = value_map.upsert(
          location, [](const TupleSlot &) -> void {}, location);
      return false;
    };
    bool UNUSED_ATTRIBUTE uprase_result =
        hash_map_->uprase_fn(index_key, upsert_fn, cuckoohash_map<TupleSlot, TupleSlot>({{location, location}}, 1));

    TERRIER_ASSERT(upsert_result != uprase_result,
                   "Either a new key was inserted (uprase), or the value_map already existed and a new value was "
                   "inserted (upsert).");

    // Register an abort action with the txn context in case of rollback
    txn->RegisterAbortAction([=]() {
      const bool UNUSED_ATTRIBUTE update_result =
          hash_map_->update_fn(index_key, [location](cuckoohash_map<TupleSlot, TupleSlot> &value_map) {
            const bool UNUSED_ATTRIBUTE erase_result = value_map.erase(location);
            TERRIER_ASSERT(erase_result, "Erase on the index failed.");
          });
      TERRIER_ASSERT(update_result, "Update on the index failed.");
    });

    return true;
  }

  bool InsertUnique(transaction::TransactionContext *const txn, const ProjectedRow &tuple,
                    const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::UNIQUE,
                   "This Insert is designed for indexes with uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);
    bool predicate_satisfied = false;
    bool result = false;

    // The predicate checks if any matching keys have write-write conflicts or are still visible to the calling txn.
    auto predicate = [&](const TupleSlot slot) -> bool {
      const auto *const data_table = slot.GetBlock()->data_table_;
      const auto has_conflict = data_table->HasConflict(*txn, slot);
      const auto is_visible = data_table->IsVisible(*txn, slot);
      return has_conflict || is_visible;
    };

    auto insert_fn = [location, &predicate_satisfied, &result,
                      predicate](cuckoohash_map<TupleSlot, TupleSlot> &value_map) -> bool {
      auto locked_value_map = value_map.lock_table();

      for (const auto i : locked_value_map) {
        predicate_satisfied = predicate_satisfied || predicate(i.first);
      }

      if (!predicate_satisfied) {
        result = locked_value_map.insert(location, location).second;
        TERRIER_ASSERT(result,
                       " index shouldn't fail to insert after predicate check. If it did, something went wrong deep "
                       "inside the hash map itself.");
      }

      locked_value_map.unlock();
      return false;
    };
    result = result || hash_map_->uprase_fn(index_key, insert_fn,
                                            cuckoohash_map<TupleSlot, TupleSlot>({{location, location}}, 1));

    TERRIER_ASSERT(result != predicate_satisfied, "Predicate satisfied is equivalent to insertion failing.");

    if (result) {
      // Register an abort action with the txn context in case of rollback
      txn->RegisterAbortAction([=]() {
        const bool UNUSED_ATTRIBUTE update_result =
            hash_map_->update_fn(index_key, [location](cuckoohash_map<TupleSlot, TupleSlot> &value_map) {
              const bool UNUSED_ATTRIBUTE erase_result = value_map.erase(location);
              TERRIER_ASSERT(erase_result, "Erase on the index failed.");
            });
        TERRIER_ASSERT(update_result, "Update on the index failed.");
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
        const bool UNUSED_ATTRIBUTE update_result =
            hash_map_->update_fn(index_key, [location](cuckoohash_map<TupleSlot, TupleSlot> &value_map) {
              const bool UNUSED_ATTRIBUTE erase_result = value_map.erase(location);
              TERRIER_ASSERT(erase_result, "Erase on the index failed.");
            });
        TERRIER_ASSERT(update_result, "Update on the index failed.");
      });
    });
  }

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

    // Build search key
    KeyType index_key;
    index_key.SetFromProjectedRow(key, metadata_);

    const bool UNUSED_ATTRIBUTE find_result =
        hash_map_->update_fn(index_key, [value_list, &txn](cuckoohash_map<TupleSlot, TupleSlot> &value_map) -> void {
          auto locked_value_map = value_map.lock_table();
          for (auto i = locked_value_map.cbegin(); i != locked_value_map.cend(); i++) {
            // Perform visibility check on result
            if (IsVisible(txn, i->first)) value_list->emplace_back(i->first);
          }
          locked_value_map.unlock();
        });

    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT ||
                       (GetConstraintType() == ConstraintType::UNIQUE && value_list->size() <= 1),
                   "Invalid number of results for unique index.");
  }

  void ScanAscending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                     const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(false, "Range scans not supported on HashIndex.");
  }

  void ScanDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                      const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(false, "Range scans not supported on HashIndex.");
  }

  void ScanLimitAscending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                          const ProjectedRow &high_key, std::vector<TupleSlot> *value_list,
                          const uint32_t limit) final {
    TERRIER_ASSERT(false, "Range scans not supported on HashIndex.");
  }

  void ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                           const ProjectedRow &high_key, std::vector<TupleSlot> *value_list,
                           const uint32_t limit) final {
    TERRIER_ASSERT(false, "Range scans not supported on HashIndex.");
  }
};

}  // namespace terrier::storage::index
