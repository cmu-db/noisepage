#pragma once

#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

/**
 * Wrapper class for the various types of indexes in our system.
 */
class Index {
 private:
  // make friends with our keys so that they can see our metadata
  friend class CompactIntsKey<1>;
  friend class CompactIntsKey<2>;
  friend class CompactIntsKey<3>;
  friend class CompactIntsKey<4>;
  friend class GenericKey<64>;
  friend class GenericKey<128>;
  friend class GenericKey<256>;
  friend class BwTreeIndexTests;
  friend class IndexPopulator;

  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;

 protected:
  /**
   * Cached metadata that allows for performance optimizations in the index keys.
   */
  const IndexMetadata metadata_;

  /**
   * Creates a new index wrapper.
   * @param oid identifier for the index
   * @param constraint_type type of index
   * @param metadata index description
   */
  Index(const catalog::index_oid_t oid, const ConstraintType constraint_type, IndexMetadata metadata)
      : oid_{oid}, constraint_type_{constraint_type}, metadata_(std::move(metadata)) {}

 public:
  virtual ~Index() = default;

  /**
   * Inserts a new key-value pair into the index.
   * @param tuple key
   * @param location value
   * @return false if the value already exists, true otherwise
   */
  virtual bool Insert(const ProjectedRow &tuple, TupleSlot location) = 0;

  /**
   * Removes a key-value pair from the index.
   * @param tuple key
   * @param location value
   * @return false if the key-value pair did not exist, true if the deletion succeeds
   */
  virtual bool Delete(const ProjectedRow &tuple, TupleSlot location) = 0;

  /**
   * Inserts a key-value pair only if the predicate fails on all existing values.
   * @param tuple key
   * @param location value
   * @param predicate predicate to check against all existing values
   * @return true if the value was inserted, false otherwise
   *         (either because value exists, or predicate returns true for one of the existing values)
   */
  virtual bool ConditionalInsert(const ProjectedRow &tuple, TupleSlot location,
                                 std::function<bool(const TupleSlot)> predicate) = 0;

  /**
   * Finds all the values associated with the given key in our index.
   * @param key the key to look for
   * @param[out] value_list the values associated with the key
   */
  virtual void ScanKey(const ProjectedRow &key, std::vector<TupleSlot> *value_list) = 0;

  /**
   * @return type of this index
   */
  ConstraintType GetConstraintType() const { return constraint_type_; }

  /**
   * @return oid of this index
   */
  catalog::index_oid_t GetOid() const { return oid_; }

  /**
   * @return reference to the metadata of this index
   */
  const IndexMetadata &GetIndexMetadata() const { return metadata_; }
};

}  // namespace terrier::storage::index
