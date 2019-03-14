#pragma once

#include <algorithm>
#include <functional>
#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "storage/index/index_defs.h"
#include "storage/storage_util.h"
#include "type/type_util.h"

namespace terrier::storage::index {

class IndexMetadata {
 public:
  DISALLOW_COPY(IndexMetadata);

  IndexMetadata(IndexMetadata &&other) noexcept
      : key_schema_(std::move(other.key_schema_)),
        attr_sizes_(std::move(other.attr_sizes_)),
        pr_offsets_(std::move(other.pr_offsets_)),
        cmp_order_(std::move(other.cmp_order_)),
        compact_ints_offsets_(std::move(other.compact_ints_offsets_)),
        key_oid_to_offset_(std::move(other.key_oid_to_offset_)) {}

  explicit IndexMetadata(KeySchema key_schema)
      : key_schema_(std::move(key_schema)),
        attr_sizes_(ComputeAttributeSizes(key_schema_)),
        pr_offsets_(ComputePROffsets(attr_sizes_)),
        cmp_order_(ComputeComparisonOrder(attr_sizes_)),
        compact_ints_offsets_(ComputeCompactIntsOffsets(attr_sizes_)),
        key_oid_to_offset_(ComputeKeyOidToOffset(key_schema_, pr_offsets_)) {}

  const std::vector<KeyData> &GetKeySchema() const { return key_schema_; }
  const std::vector<uint16_t> &GetProjectedRowOffsets() const { return pr_offsets_; }
  const std::vector<uint16_t> &GetComparisonOrder() const { return cmp_order_; }
  const std::vector<uint8_t> &GetAttributeSizes() const { return attr_sizes_; }
  const std::vector<uint8_t> &GetCompactIntsOffsets() const { return compact_ints_offsets_; }
  const std::unordered_map<key_oid_t, uint32_t> &GetKeyOidToOffsetMap() const { return key_oid_to_offset_; }

 private:
  std::vector<KeyData> key_schema_;                            // for GenericKey
  std::vector<uint8_t> attr_sizes_;                            // for CompactIntsKey
  std::vector<uint16_t> pr_offsets_;                           // not needed long term?
  std::vector<uint16_t> cmp_order_;                            // not needed long term?
  std::vector<uint8_t> compact_ints_offsets_;                  // for CompactIntsKey
  std::unordered_map<key_oid_t, uint32_t> key_oid_to_offset_;  // for execution layer

  /**
   * Computes the attribute sizes as given by the key schema.
   * e.g.   if key_schema is {BIGINT, TINYINT, INTEGER}
   *        then attr_sizes returned is {8, 1, 4}
   */
  static std::vector<uint8_t> ComputeAttributeSizes(const KeySchema &key_schema) {
    std::vector<uint8_t> attr_sizes;
    attr_sizes.reserve(key_schema.size());
    for (const auto &key : key_schema) {
      attr_sizes.emplace_back(type::TypeUtil::GetTypeSize(key.type_id));
    }
    return attr_sizes;
  }

  /**
   * Computes the projected row offsets given the attribute sizes, i.e. where they end up after reshuffling.
   * e.g.   if attr_sizes is {1, 4, 8, 2, 1}
   *        then returned offsets are {3, 1, 0, 2, 4}
   */
  static std::vector<uint16_t> ComputePROffsets(const std::vector<uint8_t> &attr_sizes) {
    auto starting_offsets = StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, 0);

    std::vector<uint16_t> pr_offsets;
    pr_offsets.reserve(attr_sizes.size());

    for (const auto &size : attr_sizes) {
      switch (size) {
        case VARLEN_COLUMN:
          pr_offsets.emplace_back(starting_offsets[0]++);
          break;
        case 8:
          pr_offsets.emplace_back(starting_offsets[1]++);
          break;
        case 4:
          pr_offsets.emplace_back(starting_offsets[2]++);
          break;
        case 2:
          pr_offsets.emplace_back(starting_offsets[3]++);
          break;
        case 1:
          pr_offsets.emplace_back(starting_offsets[4]++);
          break;
        default:
          throw std::runtime_error("unexpected switch case value");
      }
    }

    return pr_offsets;
  }

  /**
   * Computes the final comparison order for the given vector of attribute sizes.
   * e.g.   if attr_sizes {4, 4, 8, 1, 2},
   *        begin cmp_order is always assumed to be {0, 1, 2, 3, 4},
   *        final cmp_order is then {2, 0, 1, 4, 3}.
   */
  static std::vector<uint16_t> ComputeComparisonOrder(const std::vector<uint8_t> &attr_sizes) {
    // note: at most uint16_t num_columns in ProjectedRow
    std::vector<uint16_t> cmp_order(attr_sizes.size());
    std::iota(cmp_order.begin(), cmp_order.end(), 0);
    std::stable_sort(cmp_order.begin(), cmp_order.end(),
                     [&](const uint16_t &i, const uint16_t &j) -> bool { return attr_sizes[i] > attr_sizes[j]; });
    return cmp_order;
  }

  /**
   * Computes the compact int offsets for the given vector of attribute sizes.
   * e.g.   if attr_sizes {4, 4, 8, 1, 2}
   *        exclusive scan {0, 4, 8, 16, 17}
   *        gives where you should write the attrs in a compact ints key
   */
  static std::vector<uint8_t> ComputeCompactIntsOffsets(const std::vector<uint8_t> &attr_sizes) {
    TERRIER_ASSERT(std::all_of(attr_sizes.begin(), attr_sizes.end(),
                               [](uint8_t size) { return size == 1 || size == 2 || size == 4 || size == 8; }),
                   "Can only contain CompactInts compatible sizes.");
    // exclusive scan on a copy
    std::vector<uint8_t> scan = attr_sizes;
    std::exclusive_scan(scan.begin(), scan.end(), scan.begin(), 0u);
    return scan;
  }

  /**
   * Computes the mapping from key oid to projected row offset.
   */
  static std::unordered_map<key_oid_t, uint32_t> ComputeKeyOidToOffset(const KeySchema &key_schema,
                                                                       const std::vector<uint16_t> &pr_offsets) {
    std::unordered_map<key_oid_t, uint32_t> key_oid_to_offset;
    key_oid_to_offset.reserve(key_schema.size());
    for (uint16_t i = 0; i < key_schema.size(); i++) {
      key_oid_to_offset[key_schema[i].key_oid] = pr_offsets[i];
    }
    return key_oid_to_offset;
  }
};

}  // namespace terrier::storage::index
