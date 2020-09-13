#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"

namespace terrier::transaction {
class TransactionContext;
}

namespace third_party::bwtree {  // NOLINT: check censored doesn't like this namespace name
template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker,
          typename KeyHashFunc, typename ValueEqualityChecker, typename ValueHashFunc>
class BwTree;
}

namespace terrier::storage::index {
template <uint8_t KeySize>
class CompactIntsKey;
template <uint16_t KeySize>
class GenericKey;

/**
 * Wrapper around Ziqi's OpenBwTree.
 * @tparam KeyType the type of keys stored in the BwTree
 */
template <typename KeyType>
class BwTreeIndex final : public Index {
  friend class IndexBuilder;

 private:
  explicit BwTreeIndex(IndexMetadata metadata);

  const std::unique_ptr<third_party::bwtree::BwTree<
      KeyType, TupleSlot, std::less<KeyType>,  // NOLINT transparent functors can't figure out template
      std::equal_to<KeyType>,                  // NOLINT transparent functors can't figure out template
      std::hash<KeyType>, std::equal_to<TupleSlot>, std::hash<TupleSlot>>>
      bwtree_;
  mutable common::SpinLatch transaction_context_latch_;  // latch used to protect transaction context

 public:
  IndexType Type() const final { return IndexType::BWTREE; }

  void PerformGarbageCollection() final;

  size_t EstimateHeapUsage() const final;

  bool Insert(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
              TupleSlot location) final;

  bool InsertUnique(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
                    TupleSlot location) final;

  void Delete(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &tuple,
              TupleSlot location) final;

  void ScanKey(const transaction::TransactionContext &txn, const ProjectedRow &key,
               std::vector<TupleSlot> *value_list) final;

  void ScanAscending(const transaction::TransactionContext &txn, ScanType scan_type, uint32_t num_attrs,
                     ProjectedRow *low_key, ProjectedRow *high_key, uint32_t limit,
                     std::vector<TupleSlot> *value_list) final;

  void ScanDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                      const ProjectedRow &high_key, std::vector<TupleSlot> *value_list) final;

  void ScanLimitDescending(const transaction::TransactionContext &txn, const ProjectedRow &low_key,
                           const ProjectedRow &high_key, std::vector<TupleSlot> *value_list, uint32_t limit) final;

  uint64_t GetSize() const final;
};

extern template class BwTreeIndex<CompactIntsKey<8>>;
extern template class BwTreeIndex<CompactIntsKey<16>>;
extern template class BwTreeIndex<CompactIntsKey<24>>;
extern template class BwTreeIndex<CompactIntsKey<32>>;

extern template class BwTreeIndex<GenericKey<64>>;
extern template class BwTreeIndex<GenericKey<128>>;
extern template class BwTreeIndex<GenericKey<256>>;
extern template class BwTreeIndex<GenericKey<512>>;

}  // namespace terrier::storage::index
