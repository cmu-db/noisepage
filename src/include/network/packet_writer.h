#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "network/network_io_utils.h"
#include "network/postgres/postgres_defs.h"

namespace noisepage::network {

/**
 * Wrapper around an I/O layer WriteQueue to provide network protocol
 * helper methods.
 */
class PacketWriter {
 public:
  /**
   * Instantiates a new PacketWriter backed by the given WriteQueue
   */
  explicit PacketWriter(const common::ManagedPointer<WriteQueue> write_queue) : queue_(write_queue) {}

  ~PacketWriter() {
    // Make sure no packet is being written on destruction, otherwise we are
    // malformed write buffer
    NOISEPAGE_ASSERT(curr_packet_len_ == nullptr, "packet length is not null");
  }

  /**
   * Checks whether the packet is null
   * @return whether packet is null
   */
  bool IsPacketEmpty() { return curr_packet_len_ == nullptr; }

  /**
   * Write out a single type
   * @param type to write to the queue
   */
  void WriteType(NetworkMessageType type) { queue_->BufferWriteRawValue(type); }

  /**
   * Write out a packet with a single type
   * @param type Type of message to write out
   */
  void WriteSingleTypePacket(NetworkMessageType type) {
    // Make sure no active packet being constructed
    NOISEPAGE_ASSERT(IsPacketEmpty(), "packet length is null");
    BeginPacket(type).EndPacket();
  }

  /**
   * Begin writing a new packet. Caller can use other append methods to write
   * contents to the packet. An explicit call to end packet must be made to
   * make these writes valid.
   * @param type
   * @return self-reference for chaining
   */
  PacketWriter &BeginPacket(NetworkMessageType type) {
    // No active packet being constructed
    NOISEPAGE_ASSERT(IsPacketEmpty(), "packet length is null");
    if (type != NetworkMessageType::NO_HEADER) WriteType(type);
    // Remember the size field since we will need to modify it as we go along.
    // It is important that our size field is contiguous and not broken between
    // two buffers.
    queue_->BufferWriteRawValue<int32_t>(0, false);
    WriteBuffer &tail = *(queue_->buffers_[queue_->buffers_.size() - 1]);
    curr_packet_len_ = reinterpret_cast<uint32_t *>(&tail.buf_[tail.size_ - sizeof(int32_t)]);
    return *this;
  }

  /**
   * Append raw bytes from specified memory location into the write queue.
   * There must be a packet active in the writer.
   * @param src memory location to write from
   * @param len number of bytes to write
   * @return self-reference for chaining
   */
  PacketWriter &AppendRaw(const void *src, size_t len) {
    NOISEPAGE_ASSERT(!IsPacketEmpty(), "packet length is null");
    queue_->BufferWriteRaw(src, len);
    // Add the size field to the len of the packet. Be mindful of byte
    // ordering. We switch to network ordering only when the packet is finished
    *curr_packet_len_ += static_cast<uint32_t>(len);
    return *this;
  }

  /**
   * Append a value onto the write queue. There must be a packet active in the
   * writer. No byte order conversion is performed. It is up to the caller to
   * do so if needed.
   * @tparam T type of value to write
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PacketWriter &AppendRawValue(T val) {
    return AppendRaw(&val, sizeof(T));
  }

  /**
   * Append a value of specified length onto the write queue. (1, 2, 4, or 8
   * bytes). It is assumed that these bytes need to be converted to network
   * byte ordering.
   * @tparam T type of value to read off. Has to be size 1, 2, 4, or 8.
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PacketWriter &AppendValue(const T val) {
    // We only want to allow for certain type sizes to be used
    // After the static assert, the compiler should be smart enough to throw
    // away the other cases and only leave the relevant return statement.
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "Invalid size for integer");

    if constexpr (std::is_floating_point_v<T>) {
      const auto *const double_val = reinterpret_cast<const uint64_t *const>(&val);
      return AppendRawValue(htobe64(*double_val));
    } else {  // NOLINT: false positive on indentation with clang-tidy, fixed in upstream check-clang-tidy
      switch (sizeof(T)) {
        case 1:
          return AppendRawValue(val);
        case 2:
          return AppendRawValue(static_cast<T>(htobe16(static_cast<uint16_t>(val))));
        case 4:
          return AppendRawValue(static_cast<T>(htobe32(static_cast<uint32_t>(val))));
        case 8:
          return AppendRawValue(static_cast<T>(htobe64(static_cast<uint64_t>(val))));
          // Will never be here due to compiler optimization
        default:
          throw NETWORK_PROCESS_EXCEPTION("invalid size for integer");
      }
    }
  }

  /**
   * Append a string onto the write queue.
   * @param str the string to append
   * @param nul_terminate whether the nul terminator should be written as well
   * @return self-reference for chaining
   */
  PacketWriter &AppendString(const std::string &str, bool nul_terminate) {
    return AppendRaw(str.data(), nul_terminate ? str.size() + 1 : str.size());
  }

  /**
   * Append a string_view onto the write queue.
   * @param str the string to append
   * @param nul_terminate whether the nul terminator should be written as well
   * @return self-reference for chaining
   */
  PacketWriter &AppendStringView(const std::string_view str, bool nul_terminate) {
    return nul_terminate ? AppendRaw(str.data(), str.size()).AppendRawValue<uchar>(0)
                         : AppendRaw(str.data(), str.size());
  }

  /**
   * Writes the startup message, used by clients
   */
  void WriteStartupRequest(const std::unordered_map<std::string, std::string> &config, int16_t major_version = 3) {
    // Build header, assume minor version is always 0
    BeginPacket(NetworkMessageType::NO_HEADER).AppendValue<int16_t>(major_version).AppendValue<int16_t>(0);
    for (const auto &pair : config) AppendString(pair.first, true).AppendString(pair.second, true);
    AppendRawValue<uchar>(0);  // Startup message should have (byte+1) length
    EndPacket();
  }

  /**
   * End the packet. A packet write must be in progress and said write is not
   * well-formed until this method is called.
   */
  void EndPacket() {
    NOISEPAGE_ASSERT(!IsPacketEmpty(), "packet length is null");
    // Switch to network byte ordering, add the 4 bytes of size field
    *curr_packet_len_ = htonl(*curr_packet_len_ + static_cast<uint32_t>(sizeof(uint32_t)));
    curr_packet_len_ = nullptr;
  }

 private:
  // We need to keep track of the size field of the current packet,
  // so we can update it as more bytes are written into this packet.
  uint32_t *curr_packet_len_ = nullptr;
  // Underlying WriteQueue backing this writer
  const common::ManagedPointer<WriteQueue> queue_;
};

}  // namespace noisepage::network
