#include "storage/checkpoint_io.h"
#include <catalog/schema.h>
#include <vector>

namespace terrier::storage {

void AsyncBlockWriter::Open(const char *log_file_path, int buffer_num) {
  out_ = PosixIoWrappers::Open(log_file_path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
  block_size_ = CHECKPOINT_BLOCK_SIZE;

  for (int i = 0; i < buffer_num; ++i) {
    auto *buf = new byte[block_size_]();
    free_.Enqueue(buf);
  }

  writer_thread_ = new std::thread(&AsyncBlockWriter::RunWriter, this);
}

void AsyncBlockWriter::Close() {
  byte *buf;

  // use a nullptr to notify writer thread to stop
  pending_.Enqueue(nullptr);

  writer_thread_->join();
  delete writer_thread_;

  PosixIoWrappers::Close(out_);

  while (free_.Dequeue(&buf)) {
    delete[] buf;
  }
}

void AsyncBlockWriter::RunWriter() {
  byte *buf;

  // TODO(Yuning): Maybe use blocking queue?
  while (!pending_.Dequeue(&buf)) {
    // spin
  }

  while (buf != nullptr) {
    PosixIoWrappers::WriteFully(out_, buf, block_size_);

    free_.Enqueue(buf);

    // TODO(Yuning): Maybe use blocking queue?
    while (!pending_.Dequeue(&buf)) {
      // spin
    }
  }
}

void BufferedTupleWriter::SerializeTuple(ProjectedRow *row, const TupleSlot *slot, const catalog::Schema &schema,
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
  // First serialize row, then tupleslot, finally varlens (if any).
  // TODO(mengyang): find a way to deal with huge rows.
  //  Currently we assume the size of a row is less than the size of page.
  TERRIER_ASSERT(row->Size() + varlen_size <= block_size_ - sizeof(CheckpointFilePage),
                 "row size should not be larger than page size.");
  TERRIER_ASSERT(sizeof(TupleSlot) % sizeof(uint32_t) == 0, "TupleSlot is expected to be 8 aligned. If not, pad it in "
                                                            "the code.");
  AlignBufferOffset<uint64_t>();  // align for ProjectedRow
  uint32_t checkpoint_record_size = row->Size() + sizeof(TupleSlot) + varlen_size;
  if (page_offset_ + checkpoint_record_size > block_size_) {
    Persist();
  }

  // Move row to buffer
  std::memcpy(buffer_ + page_offset_, row, row->Size());
  page_offset_ += row->Size();
  
  // Move tupleslot to buffer
  AlignBufferOffset<uint32_t>();
  std::memcpy(buffer_ + page_offset_, slot, sizeof(TupleSlot));
  page_offset_ += sizeof(TupleSlot);
  
  // Move varlens to buffer
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
