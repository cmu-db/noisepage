#pragma once
#include <map>
#include <unordered_set>
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
    varlen_size_ = total_size;
    num_offsets = num_values + 1;
    offsets_ = new uint32_t[num_values + 1];
    values_ = common::AllocationUtil::AllocateAligned(total_size);
  }

  uint32_t num_offsets;
  uint32_t varlen_size_;
  uint32_t *offsets_ = nullptr;
  byte *values_ = nullptr;
};

// TODO(Tianyu): Can use to specify cases where we don't concat per-block in the future (e.g. no need to put
//  blob into Arrow)
enum class ArrowColumnType : uint8_t { FIXED_LENGTH = 0, GATHERED_VARLEN, DICTIONARY_COMPRESSED };

struct ArrowColumnInfo {
  ArrowColumnType type_;
  ArrowVarlenColumn varlen_column_;  // For varlen and dictionary
  uint32_t *indices_ = nullptr;      // for dictionary
};

/**
 * @param column_info the ArrowColumnInfo of the current column
 * @param varlen the string to be located
 * @return the code of the smallest element >= to varlen.
 */
inline uint32_t Locate(ArrowColumnInfo *column_info, VarlenEntry *varlen) {
  TERRIER_ASSERT(column_info->type_ == ArrowColumnType::DICTIONARY_COMPRESSED,
                 "Can only call Locate on dictionary compressed column");
  uint32_t lo = 0;
  uint32_t hi = column_info->varlen_column_.num_offsets;
  VarlenContentCompare comparator;
  VarlenEntry rhs;
  // TODO(Amadou): Early stopping is possible if the comparator could distinguish >= and ==.
  while (hi - lo > 1) {
    uint32_t mid = lo + (hi - lo) / 2;
    uint32_t size = column_info->varlen_column_.offsets_[mid + 1] - column_info->varlen_column_.offsets_[mid];
    rhs = VarlenEntry::Create(column_info->varlen_column_.values_ + mid, size, false);
    if (comparator(*varlen, rhs)) {
      hi = mid;
    } else {
      lo = mid;
    }
  }
  return lo;
}

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
    delete[] col_info.varlen_column_.offsets_;
    delete[] col_info.varlen_column_.values_;
  }

 private:
  uint32_t num_records_;  // number of actual records
  // null_count[num_cols] (32-bit) | padding up to 8 byte-aligned | arrow_varlen_buffers[num_cols] |
  byte varlen_content_[];
};
}  // namespace terrier::storage
