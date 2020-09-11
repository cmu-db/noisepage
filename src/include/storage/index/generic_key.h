#pragma once

#include <algorithm>
#include <cstring>
#include <functional>
#include <vector>

#include "common/hash_util.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"
#include "xxHash/xxh3.h"

namespace terrier::storage::index {

// This is the maximum number of bytes to pack into a single GenericKey template. This constraint is arbitrary and can
// be increased if 512 bytes is too small for future workloads.
constexpr uint16_t GENERICKEY_MAX_SIZE = 512;

/**
 * GenericKey is a slower key type than CompactIntsKey for use when the constraints of CompactIntsKey make it
 * unsuitable. For example, GenericKey supports VARLEN and NULLable attributes.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
class GenericKey {
 public:
  static_assert(KeySize > 0 && KeySize <= GENERICKEY_MAX_SIZE);

  /**
   * Set the GenericKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate GenericKey representation of
   * @param metadata index information, key_schema used to interpret PR data correctly
   * @param num_attrs Number of attributes
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata, size_t num_attrs) {
    TERRIER_ASSERT(from.NumColumns() == metadata.GetSchema().GetColumns().size(),
                   "ProjectedRow should have the same number of columns at the original key schema.");
    metadata_ = &metadata;
    std::memset(key_data_, 0, KeySize);

    if (metadata.MustInlineVarlen()) {
      const auto &key_schema = metadata.GetSchema();
      const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();

      const ProjectedRowInitializer &generic_key_initializer = metadata.GetInlinedPRInitializer();

      auto *const pr = GetProjectedRow();
      generic_key_initializer.InitializeRow(pr);

      UNUSED_ATTRIBUTE const auto &key_cols = key_schema.GetColumns();
      TERRIER_ASSERT(num_attrs > 0 && num_attrs <= key_cols.size(), "Number of attributes violates invariant");

      for (uint16_t i = 0; i < num_attrs; i++) {
        const auto offset = from.ColumnIds()[i].UnderlyingValue();
        TERRIER_ASSERT(offset == pr->ColumnIds()[i].UnderlyingValue(), "PRs must have the same comparison order!");
        const byte *const from_attr = from.AccessWithNullCheck(offset);
        if (from_attr == nullptr) {
          pr->SetNull(offset);
        } else {
          const auto inlined_attr_size = inlined_attr_sizes[i];
          // TODO(Gus): Magic number
          if (inlined_attr_size <= 16) {
            std::memcpy(pr->AccessForceNotNull(offset), from_attr, inlined_attr_sizes[i]);
          } else {
            // Convert the VarlenEntry to be inlined
            const auto varlen = *reinterpret_cast<const VarlenEntry *const>(from_attr);
            byte *const to_attr = pr->AccessForceNotNull(offset);
            const auto varlen_size = varlen.Size();
            *reinterpret_cast<uint32_t *const>(to_attr) = varlen_size;
            TERRIER_ASSERT(reinterpret_cast<uintptr_t>(to_attr) + sizeof(uint32_t) + varlen_size <=
                               reinterpret_cast<uintptr_t>(this) + KeySize,
                           "ProjectedRow will access out of bounds.");
            std::memcpy(to_attr + sizeof(uint32_t), varlen.Content(), varlen_size);
          }
        }
      }
    } else {
      TERRIER_ASSERT(
          reinterpret_cast<uintptr_t>(GetProjectedRow()) + from.Size() <= reinterpret_cast<uintptr_t>(this) + KeySize,
          "ProjectedRow will access out of bounds.");
      // We recast GetProjectedRow() as a workaround for -Wclass-memaccess
      std::memcpy(static_cast<void *>(GetProjectedRow()), &from, from.Size());
    }
  }

  /**
   * @return Aligned pointer to the key's internal ProjectedRow, exposed for hasher and comparators
   */
  const ProjectedRow *GetProjectedRow() const {
    const auto *pr = reinterpret_cast<const ProjectedRow *>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                   "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() <= reinterpret_cast<uintptr_t>(this) + KeySize,
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
     * @param lhs_attr first VarlenEntry to be compared
     * @param rhs_attr second VarlenEntry to be compared
     * @return std::memcmp semantics: < 0 means first is less than second, 0 means equal, > 0 means first is greater
     * than second
     */
    static int CompareVarlens(const byte *const lhs_attr, const byte *const rhs_attr) {
      const uint32_t lhs_size = *reinterpret_cast<const uint32_t *const>(lhs_attr);
      const uint32_t rhs_size = *reinterpret_cast<const uint32_t *const>(rhs_attr);
      const auto smallest_size = std::min(lhs_size, rhs_size);

      // get the pointers to the content
      const byte *const lhs_content = lhs_attr + sizeof(uint32_t);
      const byte *const rhs_content = rhs_attr + sizeof(uint32_t);
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
      return CompareVarlens(lhs_attr, rhs_attr) OP 0;                                                                 \
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

  /**
   * Returns whether this key is less than another key up to num_attrs for comparison.
   * @param rhs other key to compare against
   * @param metadata IndexMetadata
   * @param num_attrs attributes to compare against
   * @returns whether this is less than other
   */
  bool PartialLessThan(const GenericKey<KeySize> &rhs, UNUSED_ATTRIBUTE const IndexMetadata *metadata,
                       size_t num_attrs) const {
    const auto &key_schema = GetIndexMetadata().GetSchema();
    UNUSED_ATTRIBUTE const auto &key_cols = key_schema.GetColumns();
    TERRIER_ASSERT(num_attrs > 0 && num_attrs <= key_cols.size(), "Invalid num_attrs for generic key");

    for (uint16_t i = 0; i < num_attrs; i++) {
      const auto *const lhs_pr = GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = lhs_pr->ColumnIds()[i].UnderlyingValue();
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

      const terrier::type::TypeId type_id = key_schema.GetColumns()[i].Type();

      if (terrier::storage::index::GenericKey<KeySize>::TypeComparators::CompareLessThan(type_id, lhs_attr, rhs_attr))
        return true;
      if (terrier::storage::index::GenericKey<KeySize>::TypeComparators::CompareGreaterThan(type_id, lhs_attr,
                                                                                            rhs_attr))
        return false;

      // attributes are equal, continue
    }

    // keys are equal
    return true;
  }

 private:
  ProjectedRow *GetProjectedRow() {
    auto *pr = reinterpret_cast<ProjectedRow *>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                   "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() < reinterpret_cast<uintptr_t>(this) + KeySize,
                   "ProjectedRow will access out of bounds.");
    return pr;
  }

  byte key_data_[KeySize];
  const IndexMetadata *metadata_ = nullptr;
};

extern template class GenericKey<64>;
extern template class GenericKey<128>;
extern template class GenericKey<256>;

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

    const auto &key_schema = metadata.GetSchema();
    const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();

    uint64_t running_hash = 0;

    const auto *const pr = key.GetProjectedRow();

    const auto &key_cols = key_schema.GetColumns();
    for (uint16_t i = 0; i < key_cols.size(); i++) {
      const auto offset = pr->ColumnIds()[i].UnderlyingValue();
      const byte *const attr = pr->AccessWithNullCheck(offset);
      if (attr == nullptr) {
        continue;
      }

      running_hash = XXH3_64bits_withSeed(reinterpret_cast<const void *>(attr), inlined_attr_sizes[i], running_hash);
    }

    return running_hash;
  }
};

/**
 * Implements std::equal_to for GenericKey. Allows the class to be used with containers that expect STL interface.
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
    const auto &key_schema = lhs.GetIndexMetadata().GetSchema();

    const auto &key_cols = key_schema.GetColumns();
    for (uint16_t i = 0; i < key_cols.size(); i++) {
      const auto *const lhs_pr = lhs.GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = lhs_pr->ColumnIds()[i].UnderlyingValue();
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

      const terrier::type::TypeId type_id = key_schema.GetColumns()[i].Type();

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
 * Implements std::less for GenericKey. Allows the class to be used with containers that expect STL interface.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
struct less<terrier::storage::index::GenericKey<KeySize>> {
  /**
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is less than the second key
   */
  bool operator()(const terrier::storage::index::GenericKey<KeySize> &lhs,
                  const terrier::storage::index::GenericKey<KeySize> &rhs) const {
    const auto &key_schema = lhs.GetIndexMetadata().GetSchema();
    const auto &key_cols = key_schema.GetColumns();

    for (uint16_t i = 0; i < key_cols.size(); i++) {
      const auto *const lhs_pr = lhs.GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = lhs_pr->ColumnIds()[i].UnderlyingValue();
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

      const terrier::type::TypeId type_id = key_schema.GetColumns()[i].Type();

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
