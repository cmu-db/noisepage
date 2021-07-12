#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"

namespace noisepage::storage::index {
template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker,
          typename ValueEqualityChecker>
class BPlusTree;
template <uint8_t KeySize>
class CompactIntsKey;
template <uint16_t KeySize>
class GenericKey;

/**
 * Wrapper around B+ Tree.
 * @tparam KeyType the type of keys stored in the B+ Tree
 */
template <typename KeyType>
class BPlusTreeIndex final : public Index {
  friend class IndexBuilder;

 private:
  explicit BPlusTreeIndex(IndexMetadata &&metadata);

  const std::unique_ptr<BPlusTree<KeyType, TupleSlot,
                                  std::less<KeyType>,      // NOLINT transparent functors can't figure out template
                                  std::equal_to<KeyType>,  // NOLINT transparent functors can't figure out template
                                  std::equal_to<TupleSlot>>>
      bplustree_;
  mutable common::SpinLatch transaction_context_latch_;  // latch used to protect transaction context

 public:
  /**
   * @return type of the index. Note that this is the physical type, not extracted from the underlying schema or other
   * catalog metadata. This is mostly used for debugging purposes.
   */
  IndexType Type() const final { return IndexType::BPLUSTREE; }

  /**
   * Sets the B+Tree's inner node upper threshold (split)
   * @param threshold Threshold to use for inner split
   */
  void SetInnerNodeSizeUpperThreshold(int threshold);

  /**
   * Sets the B+Tree's inner node lower threshold (merge)
   * @param threshold Threshold to use for inner merge
   */
  void SetInnerNodeSizeLowerThreshold(int threshold);

  /** @return inner node upper threshold (split) */
  int GetInnerNodeSizeUpperThreshold() const;

  /** @return inner node lower threshold (merge) */
  int GetInnerNodeSizeLowerThreshold() const;

  /**
   * @return approximate number of bytes allocated on the heap for this index data structure
   */
  size_t EstimateHeapUsage() const final;

  /**
   * Inserts a new key-value pair into the index, used for non-unique key indexes.
   * @param txn txn context for the calling txn, used to register abort actions
   * @param tuple key
   * @param location value
   * @return false if the value already exists, true otherwise
   */
  bool Insert(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
              TupleSlot location) final;

  /**
   * Inserts a key-value pair only if any matching keys have TupleSlots that don't conflict with the calling txn
   * @param txn txn context for the calling txn, used for visibility and write-write, and to register abort actions
   * @param tuple key
   * @param location value
   * @return true if the value was inserted, false otherwise
   *         (either because value exists, or predicate returns true for one of the existing values)
   */
  bool InsertUnique(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
                    TupleSlot location) final;

  /**
   * Doesn't immediately call delete on the index. Registers a commit action in the txn that will eventually register a
   * deferred action for the GC to safely call delete on the index when no more transactions need to access the key.
   * @param txn txn context for the calling txn, used to register commit actions for deferred GC actions
   * @param tuple key
   * @param location value
   */
  void Delete(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
              TupleSlot location) final;

  /**
   * Finds all the values associated with the given key in our index.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param key the key to look for
   * @param[out] value_list the values associated with the key
   */
  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final;

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
  void ScanAscending(const transaction::TransactionContext &txn, ScanType scan_type, uint32_t num_attrs,
                     ProjectedRow *low_key, ProjectedRow *high_key, uint32_t limit,
                     std::vector<TupleSlot> *value_list) final;

  /**
   * Finds all the values between the given keys in our index, sorted in descending order.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param low_key the key to end at
   * @param high_key the key to start at
   * @param[out] value_list the values associated with the keys
   */
  void ScanDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                      const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) final;

  /**
   * Finds the first limit # of values between the given keys in our index, sorted in descending order.
   * @param txn txn context for the calling txn, used for visibility checks
   * @param low_key the key to end at
   * @param high_key the key to start at
   * @param[out] value_list the values associated with the keys
   * @param limit upper bound of number of values to return
   */
  void ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                           const ProjectedRow &high_key, std::vector<TupleSlot> *value_list, uint32_t limit) final;

  /** @return The number of keys in the index. */
  uint64_t GetSize() const final;
};

extern template class BPlusTreeIndex<CompactIntsKey<8>>;
extern template class BPlusTreeIndex<CompactIntsKey<16>>;
extern template class BPlusTreeIndex<CompactIntsKey<24>>;
extern template class BPlusTreeIndex<CompactIntsKey<32>>;

extern template class BPlusTreeIndex<GenericKey<64>>;
extern template class BPlusTreeIndex<GenericKey<128>>;
extern template class BPlusTreeIndex<GenericKey<256>>;
extern template class BPlusTreeIndex<GenericKey<512>>;

}  // namespace noisepage::storage::index
