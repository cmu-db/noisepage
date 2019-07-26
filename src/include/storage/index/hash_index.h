#pragma once

#include <functional>
#include <unordered_set>
#include <utility>
#include <variant>
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
  using ValueMap = std::unordered_set<TupleSlot>;
  using ValueType = std::variant<TupleSlot, ValueMap>;

  HashIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type, IndexMetadata metadata)
      : Index(constraint_type, std::move(metadata)), hash_map_{new cuckoohash_map<KeyType, ValueType>(256)} {}

  cuckoohash_map<KeyType, ValueType> *const hash_map_;

 public:
  ~HashIndex() final { delete hash_map_; }

  void PerformGarbageCollection() final{};

  bool Insert(transaction::TransactionContext *const txn, const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT,
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);

    bool UNUSED_ATTRIBUTE insert_result = false;

    auto key_found_fn = [location, &insert_result](ValueType &value) -> bool {
      if (std::holds_alternative<TupleSlot>(value)) {
        // replace it with a ValueMap
        const auto existing_location = std::get<TupleSlot>(value);
        value = ValueMap({{location}, {existing_location}}, 2);
        insert_result = true;
      } else {
        // insert the location to the cuckoohash_map
        auto &value_map = std::get<ValueMap>(value);
        insert_result = value_map.emplace(location).second;
      }
      return false;
    };

    const bool UNUSED_ATTRIBUTE uprase_result = hash_map_->uprase_fn(index_key, key_found_fn, location);

    TERRIER_ASSERT(insert_result != uprase_result,
                   "Either a new key was inserted (uprase_result), or the value already existed and a new value was "
                   "inserted (insert_result).");

    // Register an abort action with the txn context in case of rollback
    txn->RegisterAbortAction([=]() {
      auto abort_key_found_fn = [location](ValueType &value) -> bool {
        if (std::holds_alternative<TupleSlot>(value)) {
          // It's just a TupleSlot, functor should return true for uprase to erase it
          return true;
        } else {
          auto &value_map = std::get<ValueMap>(value);
          if (value_map.size() == 1) {
            // ValueMap only has 1 element, functor should return true for uprase to erase it
            TERRIER_ASSERT(value_map.count(location) == 1, "location must be the only value in the ValueMap.");
            return true;
          } else {
            // ValueMap contains multiple elements, erase the element for location
            const bool UNUSED_ATTRIBUTE erase_result = value_map.erase(location);
            TERRIER_ASSERT(erase_result, "Erasing from the ValueMap should not fail.");
            return false;
          }
        }
      };

      const bool UNUSED_ATTRIBUTE uprase_result = hash_map_->uprase_fn(index_key, abort_key_found_fn);
      TERRIER_ASSERT(!uprase_result, "This operation should NOT insert a new key into the cuckoohash_map.");
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

    // The predicate checks if any matching keys have write-write conflicts or are still visible to the calling txn.
    auto predicate = [txn](const TupleSlot slot) -> bool {
      const auto *const data_table = slot.GetBlock()->data_table_;
      const auto has_conflict = data_table->HasConflict(*txn, slot);
      const auto is_visible = data_table->IsVisible(*txn, slot);
      return has_conflict || is_visible;
    };

    bool UNUSED_ATTRIBUTE insert_result = false;

    auto key_found_fn = [location, &insert_result, &predicate_satisfied, predicate](ValueType &value) -> bool {
      if (std::holds_alternative<TupleSlot>(value)) {
        const auto existing_location = std::get<TupleSlot>(value);
        predicate_satisfied = predicate(existing_location);
        if (!predicate_satisfied) {
          // existing location is not visible, replace with a ValueMap
          value = ValueMap({{location}, {existing_location}}, 2);
          insert_result = true;
        }
      } else {
        auto &value_map = std::get<ValueMap>(value);

        for (const auto i : value_map) {
          predicate_satisfied = predicate_satisfied || predicate(i);
        }

        if (!predicate_satisfied) {
          // insert the location to the cuckoohash_map
          insert_result = value_map.emplace(location).second;
          TERRIER_ASSERT(insert_result,
                         " index shouldn't fail to insert after predicate check. If it did, something went wrong deep "
                         "inside the hash map itself.");
        }
      }
      return false;
    };

    const bool UNUSED_ATTRIBUTE uprase_result = hash_map_->uprase_fn(index_key, key_found_fn, location);

    TERRIER_ASSERT(predicate_satisfied || (insert_result != uprase_result),
                   "Either a new key was inserted (uprase_result), or the value already existed and a new value was "
                   "inserted (insert_result).");

    const bool UNUSED_ATTRIBUTE overall_result = insert_result || uprase_result;

    if (overall_result) {
      txn->RegisterAbortAction([=]() {
        auto abort_key_found_fn = [location](ValueType &value) -> bool {
          if (std::holds_alternative<TupleSlot>(value)) {
            // It's just a TupleSlot, functor should return true for uprase to erase it
            return true;
          } else {
            auto &value_map = std::get<ValueMap>(value);
            if (value_map.size() == 1) {
              // ValueMap only has 1 element, functor should return true for uprase to erase it
              TERRIER_ASSERT(value_map.count(location) == 1, "location must be the only value in the ValueMap.");
              return true;
            } else {
              // ValueMap contains multiple elements, erase the element for location
              const bool UNUSED_ATTRIBUTE erase_result = value_map.erase(location);
              TERRIER_ASSERT(erase_result, "Erasing from the ValueMap should not fail.");
              return false;
            }
          }
        };

        const bool UNUSED_ATTRIBUTE uprase_result = hash_map_->uprase_fn(index_key, abort_key_found_fn);
        TERRIER_ASSERT(!uprase_result, "This operation should NOT insert a new key into the cuckoohash_map.");
      });
    } else {
      // Presumably you've already made modifications to a DataTable (the source of the TupleSlot argument to this
      // function) however, the index found a constraint violation and cannot allow that operation to succeed. For MVCC
      // correctness, this txn must now abort for the GC to clean up the version chain in the DataTable correctly.
      txn->MustAbort();
    }

    return overall_result;
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
        auto abort_key_found_fn = [location](ValueType &value) -> bool {
          if (std::holds_alternative<TupleSlot>(value)) {
            // It's just a TupleSlot, functor should return true for uprase to erase it
            return true;
          } else {
            auto &value_map = std::get<ValueMap>(value);
            if (value_map.size() == 1) {
              // ValueMap only has 1 element, functor should return true for uprase to erase it
              TERRIER_ASSERT(value_map.count(location) == 1, "location must be the only value in the ValueMap.");
              return true;
            } else {
              // ValueMap contains multiple elements, erase the element for location
              const bool UNUSED_ATTRIBUTE erase_result = value_map.erase(location);
              TERRIER_ASSERT(erase_result, "Erasing from the ValueMap should not fail.");
              return false;
            }
          }
        };

        const bool UNUSED_ATTRIBUTE uprase_result = hash_map_->uprase_fn(index_key, abort_key_found_fn);
        TERRIER_ASSERT(!uprase_result, "This operation should NOT insert a new key into the cuckoohash_map.");
      });
    });
  }

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

    // Build search key
    KeyType index_key;
    index_key.SetFromProjectedRow(key, metadata_);

    auto key_found_fn = [value_list, &txn](const ValueType &value) -> void {
      if (std::holds_alternative<TupleSlot>(value)) {
        const auto existing_location = std::get<TupleSlot>(value);
        if (IsVisible(txn, existing_location)) value_list->emplace_back(existing_location);
      } else {
        const auto &value_map = std::get<ValueMap>(value);

        for (const auto i : value_map) {
          if (IsVisible(txn, i)) value_list->emplace_back(i);
        }
      }
    };

    const bool UNUSED_ATTRIBUTE find_result = hash_map_->find_fn(index_key, key_found_fn);

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
