#pragma once
#include <map>
#include <unordered_set>
#include "storage/block_layout.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"

namespace terrier::storage {

// TODO(Tianyu): Can use to specify cases where we don't concat per-block in the future (e.g. no need to put
//  blob into Arrow)
/**
 * Type of Arrow column
 */
enum class ArrowColumnType : uint8_t { FIXED_LENGTH = 0, GATHERED_VARLEN, DICTIONARY_COMPRESSED };

/**
 * Stores information about accessing a column using the Arrow format. This includes the type of the column,
 * and pointers to various buffers if the column is variable length or compressed.
 */
struct ArrowColumnInfo {
  ArrowColumnType type_;
  /* the block is only meaningful for gathered varlen and dictionary. */
  uint32_t varlen_size_;         // total size of varlen values
  byte *values_ = nullptr;       // pointer to the values array
  uint32_t *offsets_ = nullptr;  // pointer to the offsets array
  /* end block */
  uint32_t *indices_ = nullptr;  // only meaningful for dictionary, pointer to the indices array
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

  /**
   * @param num_cols number of columns stored in the block
   * @return size of the metadata object given the number of columns
   */
  static uint32_t Size(uint16_t num_cols) {
    return StorageUtil::PadUpToSize(sizeof(uint64_t), static_cast<uint32_t>(sizeof(uint32_t)) * (num_cols + 1)) +
           num_cols * static_cast<uint32_t>(sizeof(ArrowColumnInfo));
  }

  void Initialize(uint16_t num_cols) {
    // Need to 0 out this block to make sure all the counts are 0 and all the pointers are nullptrs
    memset(this, 0, Size(num_cols));
  }

  uint32_t &NumRecords() { return num_records_; }

  uint32_t NumRecords() const { return num_records_; }

  uint32_t &NullCount(col_id_t col_id) { return reinterpret_cast<uint32_t *>(varlen_content_)[!col_id]; }

  uint32_t NullCount(col_id_t col_id) const { return reinterpret_cast<const uint32_t *>(varlen_content_)[!col_id]; }

  ArrowColumnInfo &GetColumnInfo(const BlockLayout &layout, col_id_t col_id) {
    byte *null_count_end =
        storage::StorageUtil::AlignedPtr(sizeof(uint64_t), varlen_content_ + sizeof(uint32_t) * layout.NumColumns());
    return reinterpret_cast<ArrowColumnInfo *>(null_count_end)[!col_id];
  }

  ArrowColumnInfo &GetColumnInfo(const BlockLayout &layout, col_id_t col_id) const {
    byte *null_count_end =
        storage::StorageUtil::AlignedPtr(sizeof(uint64_t), varlen_content_ + sizeof(uint32_t) * layout.NumColumns());
    return reinterpret_cast<ArrowColumnInfo *>(null_count_end)[!col_id];
  }

  void Deallocate(const BlockLayout &layout, col_id_t col_id) {
    auto &col_info = GetColumnInfo(layout, col_id);
    delete[] col_info.indices_;
    delete[] col_info.offsets_;
    delete[] col_info.values_;
  }

 private:
  uint32_t num_records_;  // number of actual records
  // null_count[num_cols] (32-bit) | padding up to 8 byte-aligned | arrow_varlen_buffers[num_cols] |
  byte varlen_content_[];
};
}  // namespace terrier::storage
