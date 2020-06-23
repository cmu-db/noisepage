#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

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
}  // namespace terrier::storage::index

namespace third_party::bwtree {
template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>, typename ValueHashFunc = std::hash<ValueType>>
class BwTree;
}

namespace terrier::storage::index {

/**
 * Wrapper around Ziqi's OpenBwTree.
 * @tparam KeyType the type of keys stored in the BwTree
 */
template <typename KeyType>
class BwTreeIndex final : public Index {
  friend class IndexBuilder;

 private:
  explicit BwTreeIndex(IndexMetadata metadata);

  const std::unique_ptr<third_party::bwtree::BwTree<KeyType, TupleSlot>> bwtree_;

 public:
  IndexType Type() const final { return IndexType::BWTREE; }

  void PerformGarbageCollection() final;

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
};

extern template class BwTreeIndex<CompactIntsKey<8>>;
extern template class BwTreeIndex<CompactIntsKey<16>>;
extern template class BwTreeIndex<CompactIntsKey<24>>;
extern template class BwTreeIndex<CompactIntsKey<32>>;

extern template class BwTreeIndex<GenericKey<64>>;
extern template class BwTreeIndex<GenericKey<128>>;
extern template class BwTreeIndex<GenericKey<256>>;

}  // namespace terrier::storage::index
