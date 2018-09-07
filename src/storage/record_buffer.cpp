#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_manager.h"
namespace terrier::storage {
byte *UndoBuffer::NewEntry(const uint32_t size) {
  if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
    // we are out of space in the buffer. Get a new buffer segment.
    BufferSegment *new_segment = buffer_pool_->Get();
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0, "a delta entry should be aligned to 8 bytes");
    buffers_.push_back(new_segment);
  }
  return buffers_.back()->Reserve(size);
}

byte *RedoBuffer::NewEntry(const uint32_t size) {
  if (buffer_seg_ == nullptr) {
    // this is the first write
    buffer_seg_ = log_manager_->NewLogBuffer();
  } else if (!buffer_seg_->HasBytesLeft(size)) {
    // old log buffer is full
    log_manager_->AddBufferToFlushQueue(buffer_seg_);
    buffer_seg_ = log_manager_->NewLogBuffer();
  }
  TERRIER_ASSERT(buffer_seg_->HasBytesLeft(size),
                 "Staged write does not fit into redo buffer (even after a fresh one is requested)");
  return buffer_seg_->Reserve(size);
}

void RedoBuffer::Finish() {
  log_manager_->AddBufferToFlushQueue(buffer_seg_);
  buffer_seg_ = nullptr;
}

void RedoBuffer::Discard() {
  log_manager_->DiscardBuffer(buffer_seg_);
  buffer_seg_ = nullptr;
}
}  // namespace terrier::storage
