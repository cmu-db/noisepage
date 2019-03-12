#pragma once

#include <algorithm>
#include <functional>
#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/index/index_defs.h"
#include "storage/storage_util.h"
#include "type/type_util.h"

namespace terrier::storage::index {

class IndexMetadata {
 public:
  IndexMetadata &operator=(const IndexMetadata &) = delete;
  IndexMetadata(const IndexMetadata &) = delete;

  IndexMetadata(IndexMetadata &&other) noexcept
      : key_schema_(std::move(other.key_schema_)),
        pr_offsets_(std::move(other.pr_offsets_)),
        cmp_order_(std::move(other.cmp_order_)),
        attr_sizes_(std::move(other.attr_sizes_)),
        attr_offsets_(std::move(other.attr_offsets_)),
        key_oid_to_offset_(std::move(other.key_oid_to_offset_)) {}

  IndexMetadata(KeySchema key_schema, const std::vector<uint8_t> &attr_sizes)
      : key_schema_(std::move(key_schema)),
        pr_offsets_(StorageUtil::ComputeAttributeOffsets(attr_sizes, 0)),
        cmp_order_(ComputeComparisonOrder(attr_sizes)),
        attr_sizes_(attr_sizes),
        attr_offsets_(ComputeAttributeOffsets(attr_sizes, pr_offsets_)),
        key_oid_to_offset_(ComputeKeyOidToOffset(key_schema_, pr_offsets_)) {}

  const std::vector<KeyData> &GetKeySchema() const { return key_schema_; }
  const std::vector<uint16_t> &GetProjectedRowOffsets() const { return pr_offsets_; }
  const std::vector<uint16_t> &GetComparisonOrder() const { return cmp_order_; }
  const std::vector<uint8_t> &GetAttributeSizes() const { return attr_sizes_; }
  const std::vector<uint8_t> &GetAttributeOffsets() const { return attr_offsets_; }
  const std::unordered_map<key_oid_t, uint32_t> &GetKeyOidToOffsetMap() const { return key_oid_to_offset_; }

 private:
  std::vector<KeyData> key_schema_;
  std::vector<uint16_t> pr_offsets_;
  std::vector<uint16_t> cmp_order_;
  std::vector<uint8_t> attr_sizes_;
  std::vector<uint8_t> attr_offsets_;
  std::unordered_map<key_oid_t, uint32_t> key_oid_to_offset_;

  /**
   * Computes the final comparison order for the given vector of attribute sizes.
   * e.g.   if attr_sizes {4, 4, 8, 1, 2},
   *        begin cmp_order is always assumed to be {0, 1, 2, 3, 4},
   *        final cmp_order is then {2, 0, 1, 4, 3}.
   */
  static std::vector<uint16_t> ComputeComparisonOrder(const std::vector<uint8_t> &attr_sizes) {
    // note: at most uint16_t num_columns in ProjectedRow
    std::vector<uint16_t> cmp_order;
    cmp_order.reserve(attr_sizes.size());
    std::iota(cmp_order.begin(), cmp_order.end(), 0);
    std::stable_sort(cmp_order.begin(), cmp_order.end(),
                     [&](const uint16_t &i, const uint16_t &j) -> bool { return attr_sizes[i] > attr_sizes[j]; });
    return cmp_order;
  }

  /**
   * Computes the attribute offsets for the given vector of attribute sizes.
   * e.g.   if attr_sizes {4, 4, 8, 1, 2}
   *        sort the sizes {8, 4, 4, 2, 1}
   *        exclusive scan {0, 8, 12, 16, 18}
   *        scan[pr_offsets] gives attr_offsets {8, 12, 0, 18, 16}
   */
  static std::vector<uint8_t> ComputeAttributeOffsets(const std::vector<uint8_t> &attr_sizes,
                                                      const std::vector<uint16_t> &pr_offsets) {
    // exclusive scan on a copy
    std::vector<uint8_t> scan = attr_sizes;
    std::sort(scan.begin(), scan.end(), std::greater<>());
    std::exclusive_scan(scan.begin(), scan.end(), scan.begin(), 0u);
    // compute attribute offsets
    std::vector<uint8_t> attr_offsets;
    attr_offsets.reserve(attr_sizes.size());
    for (auto i = 0; i < attr_sizes.size(); i++) {
      attr_offsets.emplace_back(scan[pr_offsets[i]]);
    }
    return attr_offsets;
  }

  /**
   * Computes the mapping from key oid to projected row offset.
   * @warning Modifies pr_offsets.
   */
  static std::unordered_map<key_oid_t, uint32_t> ComputeKeyOidToOffset(const KeySchema &key_schema,
                                                                       const std::vector<uint16_t> &pr_offsets) {
    std::unordered_map<key_oid_t, uint32_t> key_oid_to_offset;
    key_oid_to_offset.reserve(key_schema.size());
    uint16_t offsets[5] = {0, 0, 0, 0, 0};
    for (const auto &k : key_schema) {
      switch (type::TypeUtil::GetTypeSize(k.type_id)) {
        case VARLEN_COLUMN:
          key_oid_to_offset[k.key_oid] = static_cast<uint16_t>(pr_offsets[0] + offsets[0]++);
          break;
        case 8:
          key_oid_to_offset[k.key_oid] = static_cast<uint16_t>(pr_offsets[1] + offsets[1]++);
          break;
        case 4:
          key_oid_to_offset[k.key_oid] = static_cast<uint16_t>(pr_offsets[2] + offsets[2]++);
          break;
        case 2:
          key_oid_to_offset[k.key_oid] = static_cast<uint16_t>(pr_offsets[3] + offsets[3]++);
          break;
        case 1:
          key_oid_to_offset[k.key_oid] = static_cast<uint16_t>(pr_offsets[4] + offsets[4]++);
          break;
        default:
          throw std::runtime_error("unexpected switch case value");
      }
    }
    return key_oid_to_offset;
  }
};

}  // namespace terrier::storage::index
