#pragma once

#include <algorithm>
#include <cstring>
#include <functional>
#include <vector>

#include "common/hash_util.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

// This is the maximum number of bytes to pack into a single GenericKey template. This contraint is arbitrary and can be
// increased if 256-bytes is too small for future workloads.
#define GENERICKEY_MAX_SIZE 256

/**
 * GenericKey is a slower key type than CompactIntsKey for use when the constraints of CompactIntsKey make it
 * unsuitable. For example, GenericKey supports VARLEN and NULLable attributes.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
class GenericKey {
 public:
  /**
   * key size in bytes, exposed for hasher and comparators
   */
  static constexpr size_t key_size_byte = KeySize;
  static_assert(KeySize > 0 && KeySize <= GENERICKEY_MAX_SIZE);  // size must be no greater than 256-bits

  /**
   * Set the GenericKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate GenericKey representation of
   * @param metadata index information, key_schema used to interpret PR data correctly
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata) {
    TERRIER_ASSERT(from.NumColumns() == metadata.GetKeySchema().size(),
                   "ProjectedRow should have the same number of columns at the original key schema.");

    metadata_ = &metadata;
    std::memset(key_data_, 0, key_size_byte);
    std::memcpy(GetProjectedRow(), &from, from.Size());
  }

  /**
   * GenericKey needs a destructor since it owns the memory of any VARLEN entries it points to.
   */
  ~GenericKey() {
    if (metadata_ == nullptr) return;

    const auto &key_schema = metadata_->GetKeySchema();

    for (uint16_t i = 0; i < key_schema.size(); i++) {
      const auto type_id = key_schema[i].type_id;

      if (type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY) {
        const auto *const pr = GetProjectedRow();
        const auto offset = static_cast<uint16_t>(pr->ColumnIds()[i]);
        const byte *const attr = pr->AccessWithNullCheck(offset);
        if (attr == nullptr) {
          // attribute is NULL, nothing to clean up
          continue;
        }

        const auto varlen = *reinterpret_cast<const VarlenEntry *const>(attr);

        if (varlen.NeedReclaim()) {
          delete[] varlen.Content();
        }
      }
    }
  }

  /**
   * @return Aligned pointer to the key's internal ProjectedRow, exposed for hasher and comparators
   */
  const ProjectedRow *GetProjectedRow() const {
    const auto *pr = reinterpret_cast<const ProjectedRow *>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                   "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() < reinterpret_cast<uintptr_t>(this) + key_size_byte,
                   "ProjectedRow will access out of bounds.");
    return pr;
  }

  /**
   * @return metadata of the index for this key, exposed for hasher and comparators
   */
  const IndexMetadata &GetIndexMetadata() const {
    TERRIER_ASSERT(metadata_ != nullptr, "This key has no metadata.");
    return *metadata_;
  }

  /**
   * Utility class to evaluate comparisons of embedded types within a ProjectedRow. This is not exposed somewhere like
   * type/type_util.h becauase these do not enforce SQL comparison semantics (i.e. NULL comparisons evaluate to NULL).
   * These comparisons are merely for ordering semantics. If it makes sense for this to be moved somewhere more general
   * purpose in the future, feel free to do so, but for now the only use is in GenericKey and I don't want to encourage
   * their misuse elsewhere in the system.
   */
  struct TypeComparators {
    TypeComparators() = delete;

    /**
     * @param lhs_varlen first VarlenEntry to be compared
     * @param rhs_varlen second VarlenEntry to be compared
     * @return std::memcmp semantics: < 0 means first is less than second, 0 means equal, > 0 means first is greater
     * than second
     */
    static int CompareVarlens(const VarlenEntry &lhs_varlen, const VarlenEntry &rhs_varlen) {
      const uint32_t lhs_size = lhs_varlen.Size();
      const uint32_t rhs_size = rhs_varlen.Size();
      const auto smallest_size = std::min(lhs_size, rhs_size);

      auto prefix_result =
          std::memcmp(lhs_varlen.Prefix(), rhs_varlen.Prefix(), std::min(smallest_size, VarlenEntry::PrefixSize()));

      if (prefix_result == 0 && smallest_size <= VarlenEntry::PrefixSize()) {
        // strings compared as equal, but they have different lengths and one fit within prefix, decide based on length
        return lhs_size - rhs_size;
      }
      if (prefix_result != 0) {
        // strings compared as non-equal with the prefix, we can use that result without inspecting any more
        return prefix_result;
      }

      // get the pointers to the content, handling if the content is inlined or not
      const byte *const lhs_content = lhs_varlen.IsInlined()
                                          ? lhs_varlen.Content()
                                          : *reinterpret_cast<const byte *const *const>(lhs_varlen.Content());
      const byte *const rhs_content = rhs_varlen.IsInlined()
                                          ? rhs_varlen.Content()
                                          : *reinterpret_cast<const byte *const *const>(rhs_varlen.Content());
      auto result = std::memcmp(lhs_content, rhs_content, smallest_size);
      if (result == 0 && lhs_size != rhs_size) {
        // strings compared as equal, but they have different lengths. Decide based on length
        result = lhs_size - rhs_size;
      }
      return result;
    }

#define COMPARE_FUNC(OP)                                                                                              \
  switch (type_id) {                                                                                                  \
    case type::TypeId::BOOLEAN:                                                                                       \
    case type::TypeId::TINYINT:                                                                                       \
      return *reinterpret_cast<const int8_t *const>(lhs_attr) OP * reinterpret_cast<const int8_t *const>(rhs_attr);   \
    case type::TypeId::SMALLINT:                                                                                      \
      return *reinterpret_cast<const int16_t *const>(lhs_attr) OP * reinterpret_cast<const int16_t *const>(rhs_attr); \
    case type::TypeId::INTEGER:                                                                                       \
      return *reinterpret_cast<const int32_t *const>(lhs_attr) OP * reinterpret_cast<const int32_t *const>(rhs_attr); \
    case type::TypeId::DATE:                                                                                          \
      return *reinterpret_cast<const uint32_t *const>(lhs_attr) OP *                                                  \
             reinterpret_cast<const uint32_t *const>(rhs_attr);                                                       \
    case type::TypeId::BIGINT:                                                                                        \
      return *reinterpret_cast<const int64_t *const>(lhs_attr) OP * reinterpret_cast<const int64_t *const>(rhs_attr); \
    case type::TypeId::DECIMAL:                                                                                       \
      return *reinterpret_cast<const double *const>(lhs_attr) OP * reinterpret_cast<const double *const>(rhs_attr);   \
    case type::TypeId::TIMESTAMP:                                                                                     \
      return *reinterpret_cast<const uint64_t *const>(lhs_attr) OP *                                                  \
             reinterpret_cast<const uint64_t *const>(rhs_attr);                                                       \
    case type::TypeId::VARCHAR:                                                                                       \
    case type::TypeId::VARBINARY: {                                                                                   \
      const auto lhs_varlen = *reinterpret_cast<const VarlenEntry *const>(lhs_attr);                                  \
      const auto rhs_varlen = *reinterpret_cast<const VarlenEntry *const>(rhs_attr);                                  \
      return CompareVarlens(lhs_varlen, rhs_varlen) OP 0;                                                             \
    }                                                                                                                 \
    default:                                                                                                          \
      throw std::runtime_error("Unknown TypeId in terrier::storage::index::GenericKey::TypeComparators.");            \
  }

    /**
     * @param type_id TypeId to interpret both pointers as
     * @param lhs_attr first value to be compared
     * @param rhs_attr second value to be compared
     * @return true if first is less than second
     */
    static bool CompareLessThan(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) {
      COMPARE_FUNC(<)  // NOLINT
    }

    /**
     * @param type_id TypeId to interpret both pointers as
     * @param lhs_attr first value to be compared
     * @param rhs_attr second value to be compared
     * @return true if first is greater than second
     */
    static bool CompareGreaterThan(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) {
      COMPARE_FUNC(>)  // NOLINT
    }

    /**
     * @param type_id TypeId to interpret both pointers as
     * @param lhs_attr first value to be compared
     * @param rhs_attr second value to be compared
     * @return true if first is equal to second
     */
    static bool CompareEquals(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) {
      COMPARE_FUNC(==)
    }
  };

 private:
  ProjectedRow *GetProjectedRow() {
    auto *pr = reinterpret_cast<ProjectedRow *>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                   "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() < reinterpret_cast<uintptr_t>(this) + key_size_byte,
                   "ProjectedRow will access out of bounds.");
    return pr;
  }

  byte key_data_[key_size_byte];
  const IndexMetadata *metadata_ = nullptr;
};

}  // namespace terrier::storage::index

namespace std {

/**
 * Implements std::hash for GenericKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
struct hash<terrier::storage::index::GenericKey<KeySize>> {
 public:
  /**
   * @param key key to be hashed
   * @return hash of the key's underlying data
   */
  size_t operator()(terrier::storage::index::GenericKey<KeySize> const &key) const {
    const auto &metadata = key.GetIndexMetadata();

    const auto &key_schema = metadata.GetKeySchema();
    const auto &attr_sizes = metadata.GetAttributeSizes();

    uint64_t running_hash = terrier::common::HashUtil::Hash(metadata);

    const auto *const pr = key.GetProjectedRow();

    running_hash = terrier::common::HashUtil::CombineHashes(running_hash, terrier::common::HashUtil::Hash(pr->Size()));
    running_hash =
        terrier::common::HashUtil::CombineHashes(running_hash, terrier::common::HashUtil::Hash(pr->NumColumns()));

    for (uint16_t i = 0; i < key_schema.size(); i++) {
      const auto type_id = key_schema[i].type_id;
      const auto offset = static_cast<uint16_t>(pr->ColumnIds()[i]);
      const byte *const attr = pr->AccessWithNullCheck(offset);
      if (attr == nullptr) {
        // attribute is NULL, just hash the nullptr to contribute something to the hash
        running_hash = terrier::common::HashUtil::CombineHashes(running_hash, terrier::common::HashUtil::Hash(attr));
        continue;
      }

      if (type_id == terrier::type::TypeId::VARCHAR || type_id == terrier::type::TypeId::VARBINARY) {
        const auto varlen = *reinterpret_cast<const terrier::storage::VarlenEntry *const>(attr);
        if (!varlen.IsInlined()) {
          const auto *const content = varlen.Content();
          TERRIER_ASSERT(content != nullptr, "Varlen's non-inlined content cannot point to null.");
          terrier::common::HashUtil::CombineHashes(running_hash,
                                                   terrier::common::HashUtil::HashBytes(content, varlen.Size()));
          continue;
        }
      }

      // just hash the attribute bytes for inlined attributes
      terrier::common::HashUtil::CombineHashes(
          running_hash, terrier::common::HashUtil::HashBytes(attr, static_cast<uint8_t>(attr_sizes[i] & INT8_MAX)));
    }

    return running_hash;
  }
};

/**
 * Implements std::equal_to for GenericKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
struct equal_to<terrier::storage::index::GenericKey<KeySize>> {
  /**
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is equal to the second key
   */
  bool operator()(const terrier::storage::index::GenericKey<KeySize> &lhs,
                  const terrier::storage::index::GenericKey<KeySize> &rhs) const {
    const auto &key_schema = lhs.GetIndexMetadata().GetKeySchema();

    for (uint16_t i = 0; i < key_schema.size(); i++) {
      const auto *const lhs_pr = lhs.GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = static_cast<uint16_t>(lhs_pr->ColumnIds()[i]);
      TERRIER_ASSERT(lhs_pr->ColumnIds()[i] == rhs_pr->ColumnIds()[i], "Comparison orders should be the same.");

      const byte *const lhs_attr = lhs_pr->AccessWithNullCheck(offset);
      const byte *const rhs_attr = rhs_pr->AccessWithNullCheck(offset);

      if (lhs_attr == nullptr) {
        if (rhs_attr == nullptr) {
          // attributes are both NULL (equal), continue
          continue;
        }
        // lhs is NULL, rhs is non-NULL, return non-equal
        return false;
      }

      if (rhs_attr == nullptr) {
        // lhs is non-NULL, rhs is NULL, return non-equal
        return false;
      }

      const terrier::type::TypeId type_id = key_schema[i].type_id;

      if (!terrier::storage::index::GenericKey<KeySize>::TypeComparators::CompareEquals(type_id, lhs_attr, rhs_attr)) {
        // one of the attrs didn't match, return non-equal
        return false;
      }

      // attributes are equal, continue
    }

    // keys are equal
    return true;
  }
};

/**
 * Implements std::less for GenericKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
struct less<terrier::storage::index::GenericKey<KeySize>> {
  /**
   * Due to the KeySize constraints, this should be optimized to a single SIMD instruction.
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is less than the second key
   */
  bool operator()(const terrier::storage::index::GenericKey<KeySize> &lhs,
                  const terrier::storage::index::GenericKey<KeySize> &rhs) const {
    const auto &key_schema = lhs.GetIndexMetadata().GetKeySchema();

    for (uint16_t i = 0; i < key_schema.size(); i++) {
      const auto *const lhs_pr = lhs.GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = static_cast<uint16_t>(lhs_pr->ColumnIds()[i]);
      TERRIER_ASSERT(lhs_pr->ColumnIds()[i] == rhs_pr->ColumnIds()[i], "Comparison orders should be the same.");

      const byte *const lhs_attr = lhs_pr->AccessWithNullCheck(offset);
      const byte *const rhs_attr = rhs_pr->AccessWithNullCheck(offset);

      if (lhs_attr == nullptr) {
        if (rhs_attr == nullptr) {
          // attributes are both NULL (equal), continue
          continue;
        }
        // lhs is NULL, rhs is non-NULL, lhs is less than
        return true;
      }

      if (rhs_attr == nullptr) {
        // lhs is non-NULL, rhs is NULL, lhs is greater than
        return false;
      }

      const terrier::type::TypeId type_id = key_schema[i].type_id;

      if (terrier::storage::index::GenericKey<KeySize>::TypeComparators::CompareLessThan(type_id, lhs_attr, rhs_attr))
        return true;
      if (terrier::storage::index::GenericKey<KeySize>::TypeComparators::CompareGreaterThan(type_id, lhs_attr,
                                                                                            rhs_attr))
        return false;

      // attributes are equal, continue
    }

    // keys are equal
    return false;
  }
};
}  // namespace std
