#pragma once

#include <boost/functional/hash.hpp>
#include <cstring>
#include <functional>
#include <vector>

#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

#define GENERICKEY_MAX_SIZE 256

/*
 * class GenericKey - Key used for indexing with opaque data
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instanciated
 * with a template argument.
 */
template <uint16_t KeySize>
class GenericKey {
 public:
  // This is the actual byte size of the key
  static constexpr size_t key_size_byte = KeySize;

  /*
   * ZeroOut() - Sets all bits to zero
   */
  void ZeroOut() { TERRIER_MEMSET(key_data, 0x00, key_size_byte); }

  void SetFromProjectedRow(const storage::ProjectedRow &from, const std::vector<uint8_t> &attr_sizes,
                           const std::vector<uint16_t> &attr_offsets) {
    TERRIER_ASSERT(attr_sizes.size() == attr_offsets.size(), "attr_sizes and attr_offsets must be equal in size.");
    TERRIER_ASSERT(!attr_sizes.empty(), "attr_sizes has too few values.");
    TERRIER_ASSERT(attr_sizes.size() <= INTSKEY_MAX_SLOTS, "attr_sizes has too many values for this type.");
    TERRIER_ASSERT(attr_sizes.size() <= from.NumColumns(), "Key cannot have more attributes than the ProjectedRow.");
    ZeroOut();

    uint8_t offset = 0;
    for (uint8_t i = 0; i < attr_offsets.size(); i++) {
      CopyAttrFromProjection(from, attr_offsets[i], attr_sizes[i], offset);
      TERRIER_ASSERT(offset <= key_size_byte, "offset went out of bounds");
    }
  }

  byte key_data[KeySize];

  const catalog::Schema *schema;
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <uint16_t KeySize>
class FastGenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    type::Value lhs_value;
    type::Value rhs_value;

    for (oid_t col_itr = 0; col_itr < schema->GetColumnCount(); col_itr++) {
      const char *lhs_data = lhs.GetRawData(schema, col_itr);
      const char *rhs_data = rhs.GetRawData(schema, col_itr);
      type::Type type = schema->GetType(col_itr);
      bool inlined = schema->IsInlined(col_itr);

      if (type::TypeUtil::CompareLessThanRaw(type, lhs_data, rhs_data, inlined) == CmpBool::CmpTrue)
        return true;
      else if (type::TypeUtil::CompareGreaterThanRaw(type, lhs_data, rhs_data, inlined) == CmpBool::CmpTrue)
        return false;
    }

    return false;
  }

  FastGenericComparator(const FastGenericComparator &) = default;
  FastGenericComparator() = default;
};

/**
 * Equality-checking function object
 */
template <uint16_t KeySize>
class GenericEqualityChecker {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    storage::Tuple lhTuple(schema);
    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
    storage::Tuple rhTuple(schema);
    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
    return lhTuple.EqualsNoSchemaCheck(rhTuple);
  }

  GenericEqualityChecker(const GenericEqualityChecker &) = default;
  GenericEqualityChecker() = default;
};

/**
 * Hash function object for an array of SlimValues
 */
template <uint16_t KeySize>
struct GenericHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<KeySize> const &p) const {
    auto schema = p.schema;

    storage::Tuple pTuple(schema);
    pTuple.MoveToTuple(reinterpret_cast<const void *>(&p));
    return pTuple.HashCode();
  }

  GenericHasher(const GenericHasher &) = default;
  GenericHasher() = default;
};

}  // namespace terrier::storage::index
