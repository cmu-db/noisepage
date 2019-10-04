#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <unordered_set>
#include <utility>
#include <variant>  // NOLINT (Matt): lint thinks this C++17 header is a C header because it only knows C++11
#include <vector>
#include "libcuckoo/cuckoohash_map.hh"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "xxHash/xxh3.h"

namespace terrier::storage::index {

// TODO(Matt): unclear at the moment if we would want this to be tunable via the SettingsManager. Alternatively, it
// might be something that is a per-index hint based on the table size (cardinality?), rather than a global setting
constexpr uint16_t INITIAL_CUCKOOHASH_MAP_SIZE = 256;

/**
 * Wrapper around libcuckoo's hash map. The MVCC is logic is similar to our reference index (BwTreeIndex). Much of the
 * logic here is related to the cuckoohash_map not being a multimap. We get around this by making the value type a
 * std::variant that can either be a TupleSlot if there's only a single value for a given key, or a std::unordered_set
 * of TupleSlots if a single key needs to map to multiple TupleSlots.
 * @tparam KeyType the type of keys stored in the map
 */
template <typename KeyType>
class HashIndex final : public Index {
  friend class IndexBuilder;

 private:
  struct TupleSlotHash {
    std::size_t operator()(const TupleSlot &slot) const {
      return XXH3_64bits(reinterpret_cast<const void *>(&slot), sizeof(TupleSlot));
    }
  };

  using ValueMap = std::unordered_set<TupleSlot, TupleSlotHash>;
  using ValueType = std::variant<TupleSlot, ValueMap>;

  explicit HashIndex(IndexMetadata metadata)
      : Index(std::move(metadata)), hash_map_{new cuckoohash_map<KeyType, ValueType>(INITIAL_CUCKOOHASH_MAP_SIZE)} {}

  const std::unique_ptr<cuckoohash_map<KeyType, ValueType>> hash_map_;

  /**
   * The lambda below is used for aborted inserts as well as committed deletes to perform the erase logic. Macros are
   * ugly but you can't define a macro that captures location outside of the scope of that variable
   */
#define ERASE_KEY_ACTION                                                                                               \
  [=]() {                                                                                                              \
    /* See the underlying container's API for more details, but the lambda below is invoked when the key is found. */  \
    auto key_found_fn = [location](ValueType &value) -> bool {                                                         \
      if (std::holds_alternative<TupleSlot>(value)) {                                                                  \
        /* It's just a TupleSlot, functor should return true for cuckoohash_map's uprase_fn to erase key/value pair */ \
        return true;                                                                                                   \
      }                                                                                                                \
      auto &value_map = std::get<ValueMap>(value);                                                                     \
      if (value_map.size() == 2) {                                                                                     \
        /* functor should replace the ValueMap with a TupleSlot for the other location */                              \
        for (const auto i : value_map) {                                                                               \
          if (i != location) {                                                                                         \
            value = i; /* Assigning TupleSlot type will change the std::variant to TupleSlot and free the ValueMap */  \
            TERRIER_ASSERT(std::holds_alternative<TupleSlot>(value), "value should now be a TupleSlot.");              \
            return false; /* Return false so cuckoohash_map's uprase_fn doesn't erase key/value pair */                \
          }                                                                                                            \
        }                                                                                                              \
      }                                                                                                                \
      /* ValueMap contains more than 2 elements, erase location from the ValueMap */                                   \
      const auto UNUSED_ATTRIBUTE erase_result = value_map.erase(location);                                            \
      TERRIER_ASSERT(erase_result == 1, "Erasing from the ValueMap should not fail.");                                 \
      TERRIER_ASSERT(value_map.size() > 1, "ValueMap should not have fewer than 2 elements.");                         \
      return false; /* Return false so cuckoohash_map's uprase_fn doesn't erase key/value pair */                      \
    };                                                                                                                 \
    const bool UNUSED_ATTRIBUTE uprase_result = hash_map_->uprase_fn(index_key, key_found_fn);                         \
    TERRIER_ASSERT(!uprase_result, "This operation should NOT insert a new key into the cuckoohash_map.");             \
  }

 public:
  IndexType Type() const final { return IndexType::HASHMAP; }

  bool Insert(transaction::TransactionContext *const txn, const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(!(metadata_.GetSchema().Unique()),
                   "This Insert is designed for secondary indexes with no uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, metadata_);

    bool UNUSED_ATTRIBUTE insert_result = false;

    /**
     * See the underlying container's API for more details, but the lambda below is invoked when the key is found.
     *
     * Captures:
     * location the TupleSlot (value) being added to index
     * insert_result captured by reference so it can be updated in outer scope. true if insert succeeded
     *
     * Args:
     * value the current value for this key value (found by underlying containiner on lookup, then passed to
     * key_found_fn)
     *
     * return true if cuckoohash_map's uprase_fn should delete the key/value pair. For inserts we always return false.
     */
    auto key_found_fn = [location, &insert_result](ValueType &value) -> bool {
      if (std::holds_alternative<TupleSlot>(value)) {
        // replace the TupleSlot with a ValueMap containing both the old and new inserted value
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
    txn->RegisterAbortAction(ERASE_KEY_ACTION);

    return true;
  }

  bool InsertUnique(transaction::TransactionContext *const txn, const ProjectedRow &tuple,
                    const TupleSlot location) final {
    TERRIER_ASSERT(metadata_.GetSchema().Unique(), "This Insert is designed for indexes with uniqueness constraints.");
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

    /**
     * See the underlying container's API for more details, but the lambda below is invoked when the key is found.
     *
     * Captures:
     * location the TupleSlot (value) being added to index
     * insert_result captured by reference so it can be updated in outer scope. true if insert succeeded
     * predicate_satisfied captured by reference so it can be updated in outer scope. true if uniqueness
     * predicate was satisfied (i.e. there's a conflict with the insert)
     * predicate std::function to evaluate for key uniqueness
     *
     * Args:
     * value the current value for this key value (found by underlying containiner on lookup, then passed to
     * key_found_fn)
     *
     * return true if cuckoohash_map's uprase_fn should delete the key/value pair. For inserts we always return false.
     */
    auto key_found_fn = [location, &insert_result, &predicate_satisfied, predicate](ValueType &value) -> bool {
      if (std::holds_alternative<TupleSlot>(value)) {
        const auto existing_location = std::get<TupleSlot>(value);
        predicate_satisfied = predicate(existing_location);
        if (!predicate_satisfied) {
          // replace the TupleSlot with a ValueMap containing both the old and new inserted value
          value = ValueMap({{location}, {existing_location}}, 2);
          insert_result = true;
        }
      } else {
        auto &value_map = std::get<ValueMap>(value);

        predicate_satisfied = std::any_of(value_map.cbegin(), value_map.cend(), predicate);

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
    const bool UNUSED_ATTRIBUTE overall_result = insert_result || uprase_result;

    TERRIER_ASSERT(predicate_satisfied != overall_result,
                   "Cant have satisfied the predicate and also succeeded to insert.");
    TERRIER_ASSERT(predicate_satisfied || (insert_result != uprase_result),
                   "Either a new key was inserted (uprase_result), or the value already existed and a new value was "
                   "inserted (insert_result).");

    if (overall_result) {
      txn->RegisterAbortAction(ERASE_KEY_ACTION);
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
    txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
      deferred_action_manager->RegisterDeferredAction(ERASE_KEY_ACTION);
    });
  }

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final {
    TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

    // Build search key
    KeyType index_key;
    index_key.SetFromProjectedRow(key, metadata_);

    /**
     * See the underlying container's API for more details, but the lambda below is invoked when the key is found.
     *
     * Captures:
     * value_list pointer to the vector to be populated with result values
     * txn reference to the calling txn in order to do visibility checks
     *
     * Args:
     * value the current value for this key value (found by underlying containiner on lookup, then passed to
     * key_found_fn)
     */
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

    TERRIER_ASSERT(!(metadata_.GetSchema().Unique()) || (metadata_.GetSchema().Unique() && value_list->size() <= 1),
                   "Invalid number of results for unique index.");
  }

#undef ERASE_KEY_ACTION
};

}  // namespace terrier::storage::index
