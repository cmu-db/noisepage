#include "storage/checkpoint_io.h"
namespace terrier::storage {

void BufferedTupleWriter::SerializeTuple(ProjectedColumns::RowView &row, ProjectedRow *row_buffer,
                    const storage::BlockLayout &layout) {
  // First construct tuple and calculate total size required
  int32_t varlen_offset_ = 0;
  std::vector<const VarlenEntry*> varlen_entries;
  for (uint16_t projection_list_idx = 0; projection_list_idx < row.NumColumns(); projection_list_idx++) {
    if (row.IsNull(projection_list_idx)) {
      row_buffer->SetNull(projection_list_idx);
    } else {
      row_buffer->SetNotNull(projection_list_idx);
      storage::col_id_t col = row.ColumnIds()[projection_list_idx];
      if (layout.IsVarlen(col)) {
        const VarlenEntry *varlen_entry = reinterpret_cast<VarlenEntry *>(row.AccessForceNotNull(projection_list_idx));
        if (varlen_entry->IsInlined()) {
          std::memcpy(row_buffer->AccessForceNotNull(projection_list_idx), varlen_entry, sizeof(VarlenEntry));
        } else {
          // use the content_ field in VarlenEntry as the offset to the varlen file.
          uint32_t size = varlen_entry->Size();
          *reinterpret_cast<VarlenEntry *>(row_buffer->AccessForceNotNull(projection_list_idx)) = VarlenEntry::CreateCheckpoint(varlen_offset_, size);
          varlen_entries.push_back(varlen_entry);
          // TODO(Mengyang): used a magic number here, because sizeof(uint32_t) produces long unsigned int instead of uint32_t.
          varlen_offset_ += (4 + varlen_entry->Size());
        }
      } else {
        std::memcpy(row_buffer->AccessForceNotNull(projection_list_idx),
                    row.AccessForceNotNull(projection_list_idx),
                    layout.AttrSize(col));
      }
    }
  }
  int32_t row_size = row_buffer->Size();
  int32_t tot_size = varlen_offset_ + row_size;
  if (cur_buffer_size_ + tot_size > block_size_) {
    WriteBuffer();
  }
  // ASSUME that the row can always fit in the block
  std::memcpy(buffer_ + cur_buffer_size_, row_buffer, row_size);
  varlen_offset_ = cur_buffer_size_ + row_size;
  for (auto entry: varlen_entries) {
    // TODO(Zhaozhes): double check the offsets are correct
    uint32_t varlen_size = entry->Size();
    TERRIER_ASSERT(varlen_size <= VarlenEntry::InlineThreshold(), "Small varlens should be inlined.");
    std::memcpy(buffer_ + varlen_offset_, &varlen_size, sizeof(varlen_size));
    varlen_offset_ += sizeof(varlen_size);
    std::memcpy(buffer_ + varlen_offset_, entry->Content(), varlen_size);
    // TODO(Mengyang): used a magic number here, because sizeof(uint32_t) produces long unsigned int instead of uint32_t.
    varlen_offset_ += varlen_size;
  }
  cur_buffer_size_ += tot_size;
}

}  //namespace terrier::storage
