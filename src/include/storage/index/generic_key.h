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

#define GENERICKEY_MAX_SIZE 256

template <uint16_t KeySize>
class GenericKeyEqualityChecker;
template <uint16_t KeySize>
class GenericKeyComparator;
template <uint16_t KeySize>
class GenericKeyHasher;

template <uint16_t KeySize>
class GenericKey {
 public:
  // This is the actual byte size of the key
  static constexpr size_t key_size_byte = KeySize;

  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata) {
    TERRIER_ASSERT(from.NumColumns() == metadata.GetKeySchema().size(),
                   "ProjectedRow should have the same number of columns at the original key schema.");

    ZeroOut();

    metadata_ = &metadata;
    std::memcpy(GetProjectedRow(), &from, from.Size());
  }

  ~GenericKey() {
    if (metadata_ == nullptr) return;

    const auto &key_schema = metadata_->GetKeySchema();

    for (uint16_t i = 0; i < key_schema.size(); i++) {
      const auto type_id = key_schema[i].type_id;

      if (type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY) {
        auto *const pr = GetProjectedRow();
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

 private:
  friend class GenericKeyEqualityChecker<KeySize>;
  friend class GenericKeyComparator<KeySize>;
  friend class GenericKeyHasher<KeySize>;

  void ZeroOut() { std::memset(key_data_, 0x00, key_size_byte); }

  ProjectedRow *const GetProjectedRow() const {
    auto *const pr = reinterpret_cast<ProjectedRow *const>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                   "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() < reinterpret_cast<uintptr_t>(this) + key_size_byte,
                   "ProjectedRow will access out of bounds.");
    return pr;
  }

  byte key_data_[key_size_byte];

  const IndexMetadata *metadata_ = nullptr;
};

template <uint16_t KeySize>
class TypeComparators {
 public:
  TypeComparators() = delete;

 private:
  friend class GenericKeyEqualityChecker<KeySize>;
  friend class GenericKeyComparator<KeySize>;
  friend class GenericKeyHasher<KeySize>;

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
      throw std::runtime_error("Unknown TypeId in terrier::storage::index::TypeComparators.");                        \
  }

  static bool CompareLessThan(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) {
    COMPARE_FUNC(<)  // NOLINT
  }

  static bool CompareGreaterThan(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) {
    COMPARE_FUNC(>)  // NOLINT
  }

  static bool CompareEquals(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) {
    COMPARE_FUNC(==)
  }
};

template <uint16_t KeySize>
class GenericKeyComparator {
 public:
  bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    TERRIER_ASSERT(lhs.metadata_ == rhs.metadata_, "Keys must have the same metadata.");

    const auto &key_schema = lhs.metadata_->GetKeySchema();

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

      const type::TypeId type_id = key_schema[i].type_id;

      if (TypeComparators<KeySize>::CompareLessThan(type_id, lhs_attr, rhs_attr)) return true;
      if (TypeComparators<KeySize>::CompareGreaterThan(type_id, lhs_attr, rhs_attr)) return false;

      // attributes are equal, continue
    }

    // keys are equal
    return false;
  }

  GenericKeyComparator(const GenericKeyComparator &) = default;
  GenericKeyComparator() = default;
};

template <uint16_t KeySize>
class GenericKeyEqualityChecker {
 public:
  bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    TERRIER_ASSERT(lhs.metadata_ == rhs.metadata_, "Keys must have the same metadata.");

    const auto &key_schema = lhs.metadata_->GetKeySchema();

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

      const type::TypeId type_id = key_schema[i].type_id;

      if (!TypeComparators<KeySize>::CompareEquals(type_id, lhs_attr, rhs_attr)) {
        // one of the attrs didn't match, return non-equal
        return false;
      }

      // attributes are equal, continue
    }

    // keys are equal
    return true;
  }

  GenericKeyEqualityChecker(const GenericKeyEqualityChecker &) = default;
  GenericKeyEqualityChecker() = default;
};

template <uint16_t KeySize>
class GenericKeyHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
 public:
  size_t operator()(GenericKey<KeySize> const &p) const {
    return common::HashUtil::HashBytes(p.key_data_, GenericKey<KeySize>::key_size_byte);
    //
    //
    //    const auto &key_schema = p.metadata_->GetKeySchema();
    //
    //    for (uint16_t i = 0; i < key_schema.size(); i++) {
    //      const auto *const pr = p.GetProjectedRow();
    //
    //      const auto offset = static_cast<uint16_t>(pr->ColumnIds()[i]);
    //
    //      const byte *const attr = pr->AccessWithNullCheck(offset);
    //
    //      if (attr == nullptr) {
    //        if (rhs_attr == nullptr) {
    //          // attributes are both NULL (equal), continue
    //          continue;
    //        }
    //        // lhs is NULL, rhs is non-NULL, return non-equal
    //        return false;
    //      }
    //
    //      if (rhs_attr == nullptr) {
    //        // lhs is non-NULL, rhs is NULL, return non-equal
    //        return false;
    //      }
    //
    //      const type::TypeId type_id = key_schema[i].type_id;
    //
    //      if (!TypeComparators<KeySize>::CompareEquals(type_id, lhs_attr, rhs_attr)) {
    //        // one of the attrs didn't match, return non-equal
    //        return false;
    //      }
    //
    //      // attributes are equal, continue
    //    }
    //
    //    // keys are equal
    //    return true;
  }

  GenericKeyHasher(const GenericKeyHasher &) = default;
  GenericKeyHasher() = default;
};

}  // namespace terrier::storage::index
