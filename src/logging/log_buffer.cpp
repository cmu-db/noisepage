#include "logging/log_buffer.h"

namespace terrier::logging {
void LogBuffer::WriteRecord(const LogRecord &record) {
  // Reserve space of a unit32_t type for the frame length
  uint32_t start = buffer_.Position();
  buffer_.WriteInt(0);

  auto type = record.Type();
  buffer_.WriteEnumInSingleByte(
    static_cast<int>(type));

  buffer_.WriteTimestamp(record.TxnId());
  buffer_.WriteTimestamp(record.CommitId());

  buffer_.WriteLong(reinterpret_cast<int64_t>(record.TupleSlot().GetBlock())
                    | record.TupleSlot().GetOffset());

  switch (type) {
    case LogRecordType::INSERT:
    case LogRecordType::UPDATE: {
      buffer_.WriteInt(record.ProjectedRowSize());
      buffer_.WriteBytes(record.ProjectedRow(), record.ProjectedRowSize());
      break;
    }
    case LogRecordType::DELETE: {
      break;
    }
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT: {
      break;
    }
    default: {
      PELOTON_ASSERT(false, "the log record type is not supported");
    }
  }

  // Fill the reserved space with the frame length
  uint32_t length = buffer_.Position() - start - sizeof(uint32_t);
  buffer_.WriteIntAt(start, length);
}
}  // namespace terrier::logging
