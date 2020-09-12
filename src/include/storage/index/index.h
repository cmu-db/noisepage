#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "storage/data_table.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"

namespace terrier::transaction {
class TransactionContext;
}  // namespace terrier::transaction

namespace terrier::storage::index {

/**
 * Wrapper class for the various types of indexes in our system. Semantically, we expect updates on indexed attributes
 * to be modeled as a delete and an insert (see bwtree_index_test.cpp CommitUpdate1, CommitUpdate2, etc.). This
 * guarantees our snapshot isolation semantics by relying on the DataTable to enforce write-write conflicts and
 * visibility issues.
 *
 * Any future indexes should mimic the logic of bwtree_index.h, performing the same checks on all operations before
 * modifying the underlying structure or returning results.
 */
class Index {
 private:
  friend class IndexKeyTests;
  friend class storage::RecoveryManager;

 protected:
  /**
   * Cached metadata that allows for performance optimizations in the index keys.
   */
  const IndexMetadata metadata_;

  /**
   * Determine if a tuple is visible by asking the DataTable associated with the TupleSlot. Used for scans.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to check visibility on
   * @return true if tuple is visible to this txn, false otherwise
   */
  static bool IsVisible(const transaction::TransactionContext &txn, const TupleSlot slot) {
    const auto *const data_table = slot.GetBlock()->data_table_;
    return data_table->IsVisible(txn, slot);
  }

  /**
   * Creates a new index wrapper.
   * @param metadata index description
   */
  explicit Index(IndexMetadata metadata) : metadata_(std::move(metadata)) {}

 public:
  virtual ~Index() = default;

  /**
   * @return type of the index. Note that this is the physical type, not extracted from the underlying schema or other
   * catalog metadata. This is mostly used for debugging purposes.
   */
  virtual IndexType Type() const = 0;

  /** @return The number of keys in the index. */
  virtual uint64_t GetSize() const = 0;

  /**
   * Invoke garbage collection on the index. For some underlying index types this may be a no-op.
   */
  virtual void PerformGarbageCollection() {}

  /**
   * @return approximate number of bytes allocated on the heap for this index data structure
   */
  virtual size_t EstimateHeapUsage() const = 0;

  /**
   * Inserts a new key-value pair into the index, used for non-unique key indexes.
   * @param txn txn context for the calling txn, used to register abort actions
   * @param tuple key
   * @param location value
   * @return false if the value already exists, true otherwise
   */
  virtual bool Insert(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
                      TupleSlot location) = 0;

  /**
   * Inserts a key-value pair only if any matching keys have TupleSlots that don't conflict with the calling txn
   * @param txn txn context for the calling txn, used for visibility and write-write, and to register abort actions
   * @param tuple key
   * @param location value
   * @return true if the value was inserted, false otherwise
   *         (either because value exists, or predicate returns true for one of the existing values)
   */
  virtual bool InsertUnique(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
                            TupleSlot location) = 0;

  /**
   * Doesn't immediately call delete on the index. Registers a commit action in the txn that will eventually register a
   * deferred action for the GC to safely call delete on the index when no more transactions need to access the key.
   * @param txn txn context for the calling txn, used to register commit actions for deferred GC actions
   * @param tuple key
   * @param location value
   */
  virtual void Delete(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
                      TupleSlot location) = 0;

  /**
   * Finds all the values associated with the given key in our index.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param key the key to look for
   * @param[out] value_list the values associated with the key
   */
  virtual void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
                       std::vector<TupleSlot> *value_list) = 0;

  /**
   * Finds all the values between the given keys in our index, sorted in ascending order.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param scan_type Scan Type
   * @param num_attrs Number of attributes to compare
   * @param low_key the key to start at
   * @param high_key the key to end at
   * @param limit if any
   * @param[out] value_list the values associated with the keys
   */
  virtual void ScanAscending(const transaction::TransactionContext &txn, ScanType scan_type, uint32_t num_attrs,
                             ProjectedRow *low_key, ProjectedRow *high_key, uint32_t limit,
                             std::vector<TupleSlot> *value_list) {
    TERRIER_ASSERT(false, "You called a method on an index type that hasn't implemented it.");
  }

  /**
   * Finds all the values between the given keys in our index, sorted in descending order.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param low_key the key to end at
   * @param high_key the key to start at
   * @param[out] value_list the values associated with the keys
   */
  virtual void ScanDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                              const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) {
    TERRIER_ASSERT(false, "You called a method on an index type that hasn't implemented it.");
  }

  /**
   * Finds the first limit # of values between the given keys in our index, sorted in descending order.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param low_key the key to end at
   * @param high_key the key to start at
   * @param[out] value_list the values associated with the keys
   * @param limit upper bound of number of values to return
   */
  virtual void ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                                   const ProjectedRow &high_key, std::vector<TupleSlot> *value_list, uint32_t limit) {
    TERRIER_ASSERT(false, "You called a method on an index type that hasn't implemented it.");
  }

  /**
   * @return mapping from key oid to projected row offset
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &GetKeyOidToOffsetMap() const {
    return metadata_.GetKeyOidToOffsetMap();
  }

  /**
   * @return projected row initializer for the given key schema
   */
  const ProjectedRowInitializer &GetProjectedRowInitializer() const { return metadata_.GetProjectedRowInitializer(); }

  /**
   * @return IndexKeyKind selected by the IndexBuilder at index construction
   */
  IndexKeyKind KeyKind() const { return metadata_.KeyKind(); }
};

}  // namespace terrier::storage::index
