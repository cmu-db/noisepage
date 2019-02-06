#pragma once
#include "storage/block_layout.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"

namespace terrier::storage {

struct ArrowVarlenColumn {
  // TODO(Tianyu): Provide a constructor that calls allocate if needed
  // TODO(Tianyu): The current GC framework only supports dealloaction of simple byte arrays, which means
  // when deallocating this column we have to extract the pointers instead of writing a destructor. This
  // is less than ideal but will do for now.

  void Allocate(uint32_t num_values, uint32_t total_size) {
    offsets_ = new uint32_t[num_values + 1];
    values_ = common::AllocationUtil::AllocateAligned(total_size);
  }

  uint32_t *offsets_ = nullptr;
  byte *values_ = nullptr;
};
/**
 * This class encapsulates all the information needed by arrow to interpret a block, such as
 * length, null counts, and the start of varlen columns, etc. (non varlen columns start can be
 * computed from block start and offset for that column, as they are essentially embedded in the
 * block itself)
 *
 * Notice that the information stored in the metadata maybe outdated if the block is hot. (Things like
 * null counts are not well defined independent of a transaction when the block is versioned)
 */
class ArrowBlockMetadata {
 public:
  MEM_REINTERPRETATION_ONLY(ArrowBlockMetadata);

  static uint32_t Size(uint16_t num_cols) {
    return StorageUtil::PadUpToSize(sizeof(uint64_t), sizeof(uint32_t) * (num_cols + 1)) +
           num_cols * sizeof(ArrowVarlenColumn);
  }

  void Initialize(uint16_t num_cols) {
    // Need to 0 out this block to make sure all the counts are 0 and all the pointers are nullptrs
    memset(this, 0, Size(num_cols));
  }

  uint32_t &NumRecords() { return num_records_; }

  uint32_t NumRecords() const { return num_records_; }

  uint32_t &NullCount(col_id_t col_id) { return reinterpret_cast<uint32_t *>(varlen_content_)[!col_id]; }

  uint32_t NullCount(col_id_t col_id) const { return reinterpret_cast<const uint32_t *>(varlen_content_)[!col_id]; }

  ArrowVarlenColumn &GetVarlenColumn(const BlockLayout &layout, col_id_t col_id) {
    byte *null_count_end =
        storage::StorageUtil::AlignedPtr(sizeof(uint64_t), varlen_content_ + sizeof(uint32_t) * layout.NumColumns());
    return reinterpret_cast<ArrowVarlenColumn *>(null_count_end)[!col_id];
  }

 private:
  uint32_t num_records_;  // number of actual records
  // null_count[num_cols] (32-bit) | padding up to 8 byte-aligned | arrow_varlen_buffers[num_cols] |
  byte varlen_content_[];
};
}  // namespace terrier::storage
