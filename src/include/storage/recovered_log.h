#pragma once
#include <iostream>
#include <fstream>

#include "storage/record_buffer.h"
#include "storage/log_record.h"

namespace terrier::storage {
// TODO(Tianyu): Clearly this is subject to change based on log strucutre. We also do no validity checking for now.
class RecoveredLog {
 public:
  // TODO(Tianyu): Need to also deal with varlens. This probably involves having access to all the varlen pools
  RecoveredLog(const std::string &log_file_name) : in_(log_file_name, std::ios::binary | std::ios::in) {}

  bool HasNext() {
    return in_.eof();
  }

  uint32_t NextRecord() {
    curr_record_size_ = ReadValue<uint32_t>();
    return curr_record_size_;
  }

  void ReadRecord(void *position) {
    auto type = ReadValue<LogRecordType>();
    auto txn_begin = ReadValue<timestamp_t>();
    switch (type) {
      case LogRecordType::COMMIT: {
        auto txn_commit = ReadValue<timestamp_t>();
        CommitRecord::Initialize(position, txn_begin, txn_commit);
        return;
      }
      case LogRecordType::REDO: {
        auto *record_header = LogRecord::InitializeHeader(position, LogRecordType::REDO, curr_record_size_, txn_begin);
        auto *record_body = record_header->GetUnderlyingRecordBodyAs<RedoRecord>();
        // TODO(Tianyu): Need to get the SQL Table pointer from oid
        auto oid UNUSED_ATTRIBUTE = ReadValue<oid_t>();
        record_body->tuple_id_ = ReadValue<tuple_id_t>();
        // TODO(Tianyu): Hacky read that assumes the internal structure of a projected row, remove later
        auto projected_row_size = ReadValue<uint32_t>();
        *reinterpret_cast<uint32_t *>(record_body->Delta()) = projected_row_size;
        in_.read(reinterpret_cast<char *>(record_body->Delta()) + sizeof(uint32_t),
                 projected_row_size - sizeof(uint32_t));
        return;
      }
    }
  }

 private:
  std::fstream in_;
  uint32_t curr_record_size_ = 0;

  template<class T>
  T ReadValue() {
    byte result[sizeof(T)];
    in_.read(reinterpret_cast<char *>(&result), sizeof(T));
    return *reinterpret_cast<T *>(result);
  }
};
}  // namespace terrier::storage