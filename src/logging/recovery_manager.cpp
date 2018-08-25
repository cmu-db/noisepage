#include <utility>
#include <map>
#include "logging/recovery_manager.h"

namespace terrier::logging {
void RecoveryManager::Recover() {
  auto size = ParseFile();
  LoadFile(size);
  Replay();
}

void RecoveryManager::ReplayTxn(timestamp_t txn_id) {

}

void RecoveryManager::Replay() {
  for (auto it = offsets_.begin(); it != offsets_.end(); it++) {
    ReplayTxn(it->first);
  }
}

uint32_t RecoveryManager::ParseFile() {
  uint32_t offset = 0;
  uint32_t total_size = 0;
  uint32_t buffer_capacity = RECOVERY_BUFFER_CAPACITY;
  uint32_t buffer_unread_size = 0;
  uint32_t buffer_read_pos = 0;
  auto *buffer = new byte[buffer_capacity];
  bool parsing_finished = false;

  file_.clear();
  file_.seekg(0, std::ios::beg);

  while (!parsing_finished) {
    uint32_t record_len;
    uint32_t buffer_unused_size = buffer_capacity - buffer_unread_size;
    uint32_t read_size = ReadBytes(file_, buffer + buffer_read_pos, buffer_unused_size);
    STORAGE_LOG_INFO("Read %d bytes from the log file stream", read_size);
    buffer_unread_size += read_size;
    if (read_size != buffer_unused_size && file_.eof()) {
        parsing_finished = true;
    }

    while (buffer_unread_size >= sizeof(record_len)) {
      CopySerializeInput length_input(buffer+buffer_read_pos, 4);
      record_len = static_cast<uint32_t >(length_input.ReadInt());
      if (buffer_unread_size >= record_len + sizeof(record_len)) {
        uint32_t record_offset = buffer_read_pos + static_cast<uint32_t >(sizeof(record_len));
        CopySerializeInput record_input(buffer+record_offset, record_len);
        auto record_type = static_cast<LogRecordType>(record_input.ReadEnumInSingleByte());
        auto txn_id = record_input.ReadTimestamp();
        uint32_t requested_size = record_len + static_cast<uint32_t >(sizeof(record_len));

        switch (record_type) {
          case LogRecordType::BEGIN: {
            if (txns_.find(txn_id) != txns_.end()) {
              STORAGE_LOG_ERROR("Duplicate transactions found in recovery");
            }
            txns_.insert(std::make_pair(txn_id, PackTypeLength(record_type, requested_size)));
            break;
          }
          case LogRecordType::COMMIT: {
            // keeps track of the memory that needs to be allocated for all committed transactions.
            total_size += (requested_size + ExtractLength(txns_[txn_id]));
          }
          // No break here, fall through intentionally
          case LogRecordType::ABORT:
          case LogRecordType::DELETE:
          case LogRecordType::UPDATE:
          case LogRecordType::INSERT: {
            if (txns_.find(txn_id) == txns_.end()) {
              STORAGE_LOG_ERROR("The transaction is illegal in recovery");
            }
            txns_[txn_id] = PackTypeLength(record_type,
              ExtractLength(txns_[txn_id]) + requested_size);
            break;
          }
          default: {
            STORAGE_LOG_ERROR("Unknown log record type in recovery.");
          }
        }
        buffer_read_pos += record_len + static_cast<uint32_t >(sizeof(record_len));
        buffer_unread_size -= (record_len + static_cast<uint32_t >(sizeof(record_len)));
      } else {
        break;
      }
    }

    TERRIER_MEMMOVE(buffer, buffer + buffer_read_pos, buffer_unread_size);
    TERRIER_MEMSET(buffer + buffer_unread_size, 0, buffer_capacity - buffer_unread_size);
    buffer_read_pos = buffer_unread_size;
  }

  delete[] buffer;

  for (auto &txn : txns_) {
    if (ExtractType(txn.second) != LogRecordType::COMMIT) {
      continue;
    }
    offsets_.insert(std::make_pair(txn.first, offset));
    offset += ExtractLength(txn.second);
  }

  return total_size;
}

void RecoveryManager::LoadFile(uint32_t size) {
  recovery_memory_  = new byte[size];
  uint32_t buffer_capacity = RECOVERY_BUFFER_CAPACITY;
  uint32_t buffer_unread_size = 0;
  uint32_t buffer_read_pos = 0;
  auto *buffer = new byte[buffer_capacity];
  bool parsing_finished = false;
  std::map<timestamp_t, uint32_t> offsets_copy(offsets_);

  file_.clear();
  file_.seekg(0, std::ios::beg);

  while (!parsing_finished) {
    uint32_t record_len;
    uint32_t buffer_unused_size = buffer_capacity - buffer_unread_size;
    uint32_t read_size = ReadBytes(file_, buffer + buffer_read_pos, buffer_unused_size);
    STORAGE_LOG_INFO("Read %d bytes from the log file stream", read_size);
    buffer_unread_size += read_size;
    if (read_size != buffer_unused_size && file_.eof()) {
      parsing_finished = true;
    }

    while (buffer_unread_size >= sizeof(record_len)) {
      CopySerializeInput length_input(buffer+buffer_read_pos, 4);
      record_len = static_cast<uint32_t >(length_input.ReadInt());
      if (buffer_unread_size >= record_len + static_cast<uint32_t>(sizeof(record_len))) {
        uint32_t record_offset = buffer_read_pos + static_cast<uint32_t>(sizeof(record_len));
        CopySerializeInput record_input(buffer + record_offset, record_len);
        record_input.ReadEnumInSingleByte();
        timestamp_t txn_id = record_input.ReadTimestamp();
        if (offsets_copy.find(txn_id) != offsets_copy.end()) {
          uint32_t current_offset = offsets_copy[txn_id];
          STORAGE_LOG_INFO("Transaction %lu writes a log record and its length"
                           " from byte %u to byte %u in the recovery memory area",
                           static_cast<uint64_t >(txn_id), current_offset,
                           current_offset + record_len + sizeof(record_len)-1);
          TERRIER_MEMCPY(recovery_memory_ + current_offset,
            buffer + buffer_read_pos, record_len + sizeof(record_len));
          offsets_copy[txn_id] += record_len + static_cast<uint32_t >(sizeof(record_len));
        }
        buffer_read_pos += (record_len + static_cast<uint32_t >(sizeof(record_len)));
        buffer_unread_size -= (record_len + static_cast<uint32_t >(sizeof(record_len)));
      } else {
        break;
      }
    }

    TERRIER_MEMMOVE(buffer, buffer + buffer_read_pos, buffer_unread_size);
    TERRIER_MEMSET(buffer + buffer_unread_size, 0, buffer_capacity - buffer_unread_size);
    buffer_read_pos = buffer_unread_size;
  }

  delete[] buffer;
}
}  // namespace terrier::logging
