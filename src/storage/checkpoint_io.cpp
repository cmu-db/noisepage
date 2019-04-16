#include "storage/checkpoint_io.h"
#include <catalog/schema.h>
#include <vector>

namespace terrier::storage {

void BufferedTupleWriter::SerializeTuple(ProjectedRow *row, const catalog::Schema &schema,
                                         const ProjectionMap &proj_map) {
  // find all varchars
  uint32_t varlen_size = 0;
  std::vector<const VarlenEntry *> varlen_entries;
  for (const catalog::Schema::Column &column : schema.GetColumns()) {
    type::TypeId col_type = column.GetType();
    if (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY) {
      // is varlen
      const uint16_t offset = proj_map.at(column.GetOid());
      const byte *col_ptr = row->AccessWithNullCheck(offset);
      if (col_ptr != nullptr) {
        auto *entry = reinterpret_cast<const VarlenEntry *>(col_ptr);
        if (!entry->IsInlined()) {
          // counting the size of the varlens with padding.
          varlen_size +=
              StorageUtil::PadUpToSize(alignof(uint32_t), static_cast<uint32_t>(sizeof(uint32_t)) + entry->Size());
          varlen_entries.push_back(entry);
        }
      }
    }
  }

  // Serialize the row
  // TODO(mengyang): find a way to deal with huge rows.
  //  Currently we assume the size of a row is less than the size of page.
  TERRIER_ASSERT(row->Size() + varlen_size <= block_size_ - sizeof(CheckpointFilePage),
                 "row size should not be larger than page size.");
  AlignBufferOffset<uint64_t>();  // align for ProjectedRow
  if (page_offset_ + row->Size() + varlen_size > block_size_) {
    PersistBuffer();
  }

  std::memcpy(buffer_ + page_offset_, row, row->Size());
  page_offset_ += row->Size();

  for (auto *entry : varlen_entries) {
    AlignBufferOffset<uint32_t>();  // align for size field of varlen.
    uint32_t size = entry->Size();
    TERRIER_ASSERT(size > VarlenEntry::InlineThreshold(), "Small varlens should be inlined.");
    std::memcpy(buffer_ + page_offset_, &size, sizeof(size));
    page_offset_ += static_cast<uint32_t>(sizeof(size));
    std::memcpy(buffer_ + page_offset_, entry->Content(), size);
    page_offset_ += size;
  }
}

}  // namespace terrier::storage
