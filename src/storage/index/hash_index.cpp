#include "storage/index/hash_index.h"

#include "libcuckoo/cuckoohash_map.hh"
#include "storage/index/generic_key.h"
#include "storage/index/hash_key.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "xxHash/xxh3.h"

namespace terrier::storage::index {

template <typename KeyType>
struct HashIndex<KeyType>::TupleSlotHash {
  std::size_t operator()(const TupleSlot &slot) const {
    return XXH3_64bits(reinterpret_cast<const void *>(&slot), sizeof(TupleSlot));
  }
};

template <typename KeyType>
HashIndex<KeyType>::HashIndex(IndexMetadata metadata)
    : Index(std::move(metadata)),
      hash_map_(std::make_unique<cuckoohash_map<KeyType, ValueType>>(INITIAL_CUCKOOHASH_MAP_SIZE)) {}

template <typename KeyType>
size_t HashIndex<KeyType>::EstimateHeapUsage() const {
  // This is a back-of-the-envelope calculation that could be innacurate: in the case of duplicate keys, we switch the
  // value type to be an unordered_set, which this will not account for. If we implement an element counter at the the
  // wrapper level, however, then we don't know anything about the over-provisioning taking place at the underlying data
  // structure to maintain its load factor. In theory we could take max(wrapper elements * (sizeof(key) +
  // sizeof(value)), underlying capacity * (sizeof(key) + sizeof(value)) if we think the current calculation is off too
  // far.
  return hash_map_->capacity() * (sizeof(KeyType) + sizeof(TupleSlot));
}

template <typename KeyType>
uint64_t HashIndex<KeyType>::GetSize() const {
  return hash_map_->size();
}

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

template <typename KeyType>
bool HashIndex<KeyType>::Insert(const common::ManagedPointer<transaction::TransactionContext> txn,
                                const ProjectedRow &tuple, const TupleSlot location) {
  TERRIER_ASSERT(!(metadata_.GetSchema().Unique()),
                 "This Insert is designed for secondary indexes with no uniqueness constraints.");
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

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
  // TODO(wuwenw): transaction context is not thread safe for now, and a latch is used here to protect it, may need
  // a better way
  common::SpinLatch::ScopedSpinLatch guard(&transaction_context_latch_);
  // Register an abort action with the txn context in case of rollback
  txn->RegisterAbortAction(ERASE_KEY_ACTION);

  return true;
}
template <typename KeyType>
bool HashIndex<KeyType>::InsertUnique(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const ProjectedRow &tuple, const TupleSlot location) {
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
    // TODO(wuwenw): transaction context is not thread safe for now, and a latch is used here to protect it, may need
    // a better way
    common::SpinLatch::ScopedSpinLatch guard(&transaction_context_latch_);
    txn->RegisterAbortAction(ERASE_KEY_ACTION);
  } else {
    // Presumably you've already made modifications to a DataTable (the source of the TupleSlot argument to this
    // function) however, the index found a constraint violation and cannot allow that operation to succeed. For MVCC
    // correctness, this txn must now abort for the GC to clean up the version chain in the DataTable correctly.
    txn->SetMustAbort();
  }

  return overall_result;
}
template <typename KeyType>
void HashIndex<KeyType>::Delete(const common::ManagedPointer<transaction::TransactionContext> txn,
                                const ProjectedRow &tuple, const TupleSlot location) {
  KeyType index_key;
  index_key.SetFromProjectedRow(tuple, metadata_, metadata_.GetSchema().GetColumns().size());

  TERRIER_ASSERT(!(location.GetBlock()->data_table_->HasConflict(*txn, location)) &&
                     !(location.GetBlock()->data_table_->IsVisible(*txn, location)),
                 "Called index delete on a TupleSlot that has a conflict with this txn or is still visible.");

  // Register a deferred action for the GC with txn manager. See base function comment.
  txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction(ERASE_KEY_ACTION);
  });
}
template <typename KeyType>
void HashIndex<KeyType>::ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
                                 std::vector<TupleSlot> *value_list) {
  TERRIER_ASSERT(value_list->empty(), "Result set should begin empty.");

  // Build search key
  KeyType index_key;
  index_key.SetFromProjectedRow(key, metadata_, metadata_.GetSchema().GetColumns().size());

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

template class HashIndex<HashKey<8>>;
template class HashIndex<HashKey<16>>;
template class HashIndex<HashKey<32>>;
template class HashIndex<HashKey<64>>;
template class HashIndex<HashKey<128>>;
template class HashIndex<HashKey<256>>;

template class HashIndex<GenericKey<64>>;
template class HashIndex<GenericKey<128>>;
template class HashIndex<GenericKey<256>>;

}  // namespace terrier::storage::index
