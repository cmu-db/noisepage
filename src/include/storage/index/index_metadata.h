#pragma once

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "common/macros.h"
#include "storage/block_layout.h"
#include "storage/index/index_defs.h"
#include "storage/projected_row.h"
#include "storage/storage_util.h"
#include "type/type_util.h"

namespace noisepage::storage::index {

/**
 * Precomputes index-related metadata that can be used to optimize the operations of the various index key types.
 */
class IndexMetadata {
 public:
  /**
   * Move-construct the index metadata object.
   * @param other move source
   */
  IndexMetadata(IndexMetadata &&other) noexcept
      : key_schema_(std::move(other.key_schema_)),
        attr_sizes_(std::move(other.attr_sizes_)),
        inlined_attr_sizes_(std::move(other.inlined_attr_sizes_)),
        must_inline_varlen_(other.must_inline_varlen_),
        compact_ints_offsets_(std::move(other.compact_ints_offsets_)),
        key_oid_to_offset_(std::move(other.key_oid_to_offset_)),
        initializer_(std::move(other.initializer_)),
        inlined_initializer_(std::move(other.inlined_initializer_)),
        key_size_(other.key_size_),
        key_kind_(other.key_kind_) {}

  /**
   * Precomputes metadata for the given key schema.
   * @param key_schema index key schema
   */
  explicit IndexMetadata(catalog::IndexSchema key_schema)
      : key_schema_(std::move(key_schema)),
        attr_sizes_(ComputeAttributeSizes(key_schema_)),
        inlined_attr_sizes_(ComputeInlinedAttributeSizes(key_schema_)),
        must_inline_varlen_(ComputeMustInlineVarlen(key_schema_)),
        compact_ints_offsets_(ComputeCompactIntsOffsets(attr_sizes_)),
        key_oid_to_offset_(ComputeKeyOidToOffset(key_schema_, ComputePROffsets(inlined_attr_sizes_))),
        initializer_(
            ProjectedRowInitializer::Create(GetRealAttrSizes(attr_sizes_), ComputePROffsets(inlined_attr_sizes_))),
        inlined_initializer_(
            ProjectedRowInitializer::Create(inlined_attr_sizes_, ComputePROffsets(inlined_attr_sizes_))),
        key_size_(ComputeKeySize(key_schema_)) {}

  /**
   * @return index key schema
   */
  const catalog::IndexSchema &GetSchema() const { return key_schema_; }

  /**
   * @return unsorted index attribute sizes (key schema order), varlens are marked
   */
  const std::vector<uint16_t> &GetAttributeSizes() const { return attr_sizes_; }

  /**
   * @return actual inlined index attribute sizes
   */
  const std::vector<uint16_t> &GetInlinedAttributeSizes() const { return inlined_attr_sizes_; }

  /**
   * @return true if the varlens must be inlined, i.e. we cannot rely on VarlenEntry inlining as varlens are too big
   */
  bool MustInlineVarlen() const { return must_inline_varlen_; }

  /**
   * @return offsets to write into for compact ints (key schema order)
   */
  const std::vector<uint8_t> &GetCompactIntsOffsets() const { return compact_ints_offsets_; }

  /**
   * @return mapping from key oid to projected row offset
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &GetKeyOidToOffsetMap() const {
    return key_oid_to_offset_;
  }

  /**
   * @return projected row initializer for the given key schema
   */
  const ProjectedRowInitializer &GetProjectedRowInitializer() const { return initializer_; }

  /**
   * @return projected row initializer for the inlined version of the key schema
   */
  const ProjectedRowInitializer &GetInlinedPRInitializer() const { return inlined_initializer_; }

  /**
   * @return sum of attribute sizes, NOT inlined attribute sizes
   */
  uint16_t KeySize() const { return key_size_; }

  /**
   * @return IndexKeyKind selected by the IndexBuilder at index construction
   */
  IndexKeyKind KeyKind() const { return key_kind_; }

  /**
   * This should only be used by the IndexBuilder class, and this metadata is only used for testing purposes. We don't
   * expect to need to make this durable since the IndexBuilder should select the same key type every time.
   * @param key_kind IndexKeyKind for the index that was built
   */
  void SetKeyKind(const IndexKeyKind key_kind) { key_kind_ = key_kind; }

 private:
  DISALLOW_COPY(IndexMetadata);
  FRIEND_TEST(IndexKeyTests, IndexMetadataCompactIntsKeyTest);
  FRIEND_TEST(IndexKeyTests, IndexMetadataGenericKeyNoMustInlineVarlenTest);
  FRIEND_TEST(IndexKeyTests, IndexMetadataGenericKeyMustInlineVarlenTest);

  catalog::IndexSchema key_schema_;                                             // for GenericKey
  std::vector<uint16_t> attr_sizes_;                                            // for CompactIntsKey
  std::vector<uint16_t> inlined_attr_sizes_;                                    // for GenericKey
  bool must_inline_varlen_;                                                     // for GenericKey
  std::vector<uint8_t> compact_ints_offsets_;                                   // for CompactIntsKey
  std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> key_oid_to_offset_;  // for execution layer
  ProjectedRowInitializer initializer_;                                         // user-facing initializer
  ProjectedRowInitializer inlined_initializer_;                                 // for GenericKey, internal only
  uint16_t key_size_;                                                           // for IndexBuilder
  IndexKeyKind key_kind_;                                                       // for testing

  /**
   * Computes the attribute sizes as given by the key schema.
   * e.g.   if key_schema is {INTEGER, INTEGER, BIGINT, TINYINT, SMALLINT}
   *        then attr_sizes returned is {4, 4, 8, 1, 2}
   */
  static std::vector<uint16_t> ComputeAttributeSizes(const catalog::IndexSchema &key_schema) {
    std::vector<uint16_t> attr_sizes;
    auto key_cols = key_schema.GetColumns();
    attr_sizes.reserve(key_cols.size());
    for (const auto &key : key_cols) {
      attr_sizes.emplace_back(type::TypeUtil::GetTypeSize(key.Type()));
    }
    return attr_sizes;
  }

  /**
   * Computes attribute size sum, not inlined
   */
  static uint16_t ComputeKeySize(const catalog::IndexSchema &key_schema) {
    uint16_t key_size = 0;
    auto key_cols = key_schema.GetColumns();
    for (const auto &key : key_cols) {
      key_size = static_cast<uint16_t>(key_size + AttrSizeBytes(type::TypeUtil::GetTypeSize(key.Type())));
    }
    return key_size;
  }

  /**
   * Computes the attribute sizes as given by the key schema if everything were inlined.
   * Note varchars are inlined as VarlenEntry if they fit, and as (4 bytes of size + varlen content) otherwise.
   * e.g.   if key_schema is {INTEGER, VARCHAR(8), VARCHAR(0), TINYINT, VARCHAR(12)}
   *        then attr_sizes returned is {4, 16, 16, 1, 16}
   */
  static std::vector<uint16_t> ComputeInlinedAttributeSizes(const catalog::IndexSchema &key_schema) {
    std::vector<uint16_t> inlined_attr_sizes;
    auto key_cols = key_schema.GetColumns();
    inlined_attr_sizes.reserve(key_cols.size());
    for (const auto &key : key_cols) {
      auto key_type = key.Type();
      switch (key_type) {
        case type::TypeId::VARBINARY:
        case type::TypeId::VARCHAR: {
          // Add 4 bytes because we'll prepend a size field. If we're too small, we'll just use a VarlenEntry.
          auto varlen_size =
              std::max(static_cast<uint16_t>(key.MaxVarlenSize() + 4), static_cast<uint16_t>(sizeof(VarlenEntry)));
          inlined_attr_sizes.emplace_back(varlen_size);
          break;
        }
        default:
          inlined_attr_sizes.emplace_back(type::TypeUtil::GetTypeSize(key_type));
          break;
      }
    }
    return inlined_attr_sizes;
  }

  /**
   * Computes whether we need to manually inline varlen attributes, i.e. too big for VarlenEntry::CreateInline.
   */
  static bool ComputeMustInlineVarlen(const catalog::IndexSchema &key_schema) {
    auto key_cols = key_schema.GetColumns();
    return std::any_of(key_cols.begin(), key_cols.end(), [](const auto &key) -> bool {
      switch (key.Type()) {
        case type::TypeId::VARBINARY:
        case type::TypeId::VARCHAR:
          return key.MaxVarlenSize() > VarlenEntry::InlineThreshold();
        default:
          break;
      }
      return false;
    });
  }

  /**
   * Computes the compact int offsets for the given vector of attribute sizes.
   * e.g.   if attr_sizes {4, 4, 8, 1, 2}
   *        exclusive scan {0, 4, 8, 16, 17}
   *        since offset[i] = where to write sorted attr i in a compact ints key
   */
  static std::vector<uint8_t> ComputeCompactIntsOffsets(const std::vector<uint16_t> &attr_sizes) {
    // exclusive scan
    std::vector<uint8_t> scan;
    scan.reserve(attr_sizes.size());
    scan.emplace_back(0);
    for (uint16_t i = 1; i < attr_sizes.size(); i++) {
      scan.emplace_back(scan[i - 1] + attr_sizes[i - 1]);
    }
    return scan;
  }

  /**
   * Computes the projected row offsets given the attribute sizes.
   * e.g.   if attr_sizes is {4, 4, 8, 1, 2}
   *        then returned offsets are {1, 2, 0, 4, 3}
   *        since offset[i] = where attr i ended up after sorting
   */
  static std::vector<uint16_t> ComputePROffsets(const std::vector<uint16_t> &attr_sizes) {
    // tuple of size and original index
    std::vector<std::pair<uint16_t, uint16_t>> size_idx;
    size_idx.reserve(attr_sizes.size());
    for (uint16_t i = 0; i < attr_sizes.size(); i++) {
      size_idx.emplace_back(std::make_pair(attr_sizes[i], i));
    }
    // sort by the sizes
    std::stable_sort(size_idx.begin(), size_idx.end(),
                     [](const auto &u, const auto &v) -> bool { return u.first > v.first; });
    // read off the pr_offsets
    std::vector<uint16_t> pr_offsets(attr_sizes.size());
    for (uint16_t i = 0; i < size_idx.size(); i++) {
      // rank[original index] = position after sort
      pr_offsets[size_idx[i].second] = i;
    }
    return pr_offsets;
  }

  /**
   * Computes the mapping from key oid to projected row offset.
   */
  static std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> ComputeKeyOidToOffset(
      const catalog::IndexSchema &key_schema, const std::vector<uint16_t> &pr_offsets) {
    std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> key_oid_to_offset;
    const auto &key_cols = key_schema.GetColumns();
    key_oid_to_offset.reserve(key_cols.size());
    for (uint16_t i = 0; i < key_cols.size(); i++) {
      key_oid_to_offset[key_cols[i].Oid()] = pr_offsets[i];
    }
    return key_oid_to_offset;
  }

  /**
   * By default, the uint16_t attr_sizes that we pass around in our system are not the real attribute sizes.
   * The MSB is set to indicate whether a column is VARLEN or otherwise. We mask these off to get the real sizes.
   */
  static std::vector<uint16_t> GetRealAttrSizes(std::vector<uint16_t> attr_sizes) {
    std::transform(attr_sizes.begin(), attr_sizes.end(), attr_sizes.begin(),
                   [](uint16_t elem) -> uint16_t { return AttrSizeBytes(elem); });
    return attr_sizes;
  }
};

}  // namespace noisepage::storage::index
