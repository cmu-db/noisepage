#pragma once

#include <arpa/inet.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "network/network_defs.h"
#include "util/portable_endian.h"

namespace terrier::network {

/**
 * A plain old buffer with a movable cursor, the meaning of which is dependent
 * on the use case.
 *
 * The buffer has a fix capacity and one can write a variable amount of
 * meaningful bytes into it. We call this amount "size" of the buffer.
 */
class Buffer {
 public:
  Buffer() { buf_.resize(capacity_); }

  /**
   * Instantiates a new buffer and reserve capacity many bytes.
   */
  explicit Buffer(size_t capacity) : capacity_(capacity) { buf_.resize(capacity_); }

  /**
   * Reset the buffer pointer and clears content
   */
  void Reset() {
    size_ = 0;
    offset_ = 0;
  }

  /**
   * @param bytes The number of bytes to skip for the cursor
   */
  void Skip(size_t bytes) { offset_ += bytes; }

  /**
   * @param bytes The amount of bytes to check between the cursor and the end
   *              of the buffer (defaults to any)
   * @return Whether there is any more bytes between the cursor and
   *         the end of the buffer
   */
  bool HasMore(size_t bytes = 1) { return offset_ + bytes <= size_; }

  /**
   * @return Whether the buffer is at capacity. (All usable space is filled
   *          with meaningful bytes)
   */
  bool Full() { return size_ == Capacity(); }

  /**
   * @return Iterator to the beginning of the buffer
   */
  ByteBuf::const_iterator Begin() { return std::begin(buf_); }

  /**
   * @return Capacity of the buffer (not actual size)
   */
  size_t Capacity() const { return capacity_; }

  /**
   * Shift contents to align the current cursor with start of the buffer,
   * remove all bytes before the cursor.
   */
  void MoveContentToHead() {
    auto unprocessed_len = size_ - offset_;
    std::memmove(&buf_[0], &buf_[offset_], unprocessed_len);
    size_ = unprocessed_len;
    offset_ = 0;
  }

 protected:
  /**
   * Number of bytes the buffer holds
   */
  size_t size_ = 0;

  /**
   * Offset of current cursor position of buffer
   */
  size_t offset_ = 0;

  /**
   * Capacity of the buffer
   */
  const size_t capacity_ = SOCKET_BUFFER_CAPACITY;

  /**
   * Actual character buffer where bytes are held
   */
  ByteBuf buf_;

 private:
  friend class WriteQueue;
  friend class PacketWriter;
};

// Helper method for reading nul-terminated string for the read buffer
static std::string ReadCString(ByteBuf::const_iterator begin, ByteBuf::const_iterator end) {
  // search for the nul terminator
  for (auto head = begin; head != end; ++head)
    if (*head == 0) return std::string(begin, head);
  // No nul terminator found
  throw NETWORK_PROCESS_EXCEPTION("Expected nil in read buffer, none found");
}

/**
 * A view of the read buffer that has its own read head.
 */
class ReadBufferView {
 public:
  /**
   * Creates a new ReadBufferView
   * @param size The size of the view
   * @param begin
   */
  ReadBufferView(size_t size, ByteBuf::const_iterator begin) : size_(size), begin_(begin) {}
  /**
   * Read the given number of bytes into destination, advancing cursor by that
   * number. It is up to the caller to ensure that there are enough bytes
   * available in the read buffer at this point.
   * @param bytes Number of bytes to read
   * @param dest Desired memory location to read into
   */
  void Read(size_t bytes, void *dest) {
    std::copy(begin_ + offset_, begin_ + offset_ + bytes, reinterpret_cast<uchar *>(dest));
    offset_ += bytes;
  }

  /**
   * Read a value of specified length off of the read buffer (1, 2,
   * 4, or 8 bytes). It is assumed that the bytes in the buffer are in network
   * byte ordering and will be converted to the correct host ordering. It is up
   * to the caller to ensure that there are enough bytes available in the read
   * buffer at this point.
   * @tparam T type of value to read off. Has to be size 1, 2, 4, or 8.
   * @return value of numeric switched from network byte order
   */
  template <typename T>
  T ReadValue() {
    // We only want to allow for certain type sizes to be used
    // After the static assert, the compiler should be smart enough to throw
    // away the other cases and only leave the relevant return statement.
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "Invalid size for numeric.");
    if constexpr (std::is_floating_point_v<T>) {
      switch (sizeof(T)) {
        case 4: {
          const auto raw_bytes = be32toh(ReadRawValue<uint32_t>());
          const auto float_bytes = reinterpret_cast<const T *const>(&raw_bytes);
          return *float_bytes;
        }
        case 8: {
          const auto raw_bytes = be64toh(ReadRawValue<uint64_t>());
          const auto double_bytes = reinterpret_cast<const T *const>(&raw_bytes);
          return *double_bytes;
        }
          // Will never be here due to compiler optimization
        default:
          throw NETWORK_PROCESS_EXCEPTION("Invalid size for floating point.");
      }
    } else {  // NOLINT: false positive on indentation with clang-tidy, fixed in upstream check-clang-tidy
      const auto val = ReadRawValue<T>();
      switch (sizeof(T)) {
        case 1:
          return val;
        case 2:
          return static_cast<T>(be16toh(static_cast<uint16_t>(val)));
        case 4:
          return static_cast<T>(be32toh(static_cast<uint32_t>(val)));
        case 8:
          return static_cast<T>(be64toh(static_cast<uint64_t>(val)));
          // Will never be here due to compiler optimization
        default:
          throw NETWORK_PROCESS_EXCEPTION("Invalid size for integer.");
      }
    }
  }

  /**
   * Read a nul-terminated string off the read buffer, or throw an exception
   * if no nul-terminator is found within packet range.
   * @return string at head of read buffer
   */
  std::string ReadString() {
    std::string result = ReadCString(begin_ + offset_, begin_ + size_);
    // extra byte of nul-terminator
    offset_ += result.size() + 1;
    return result;
  }

  /**
   * Read a not nul-terminated string off the read buffer of specified length
   * @return string at head of read buffer
   */
  std::string ReadString(size_t len) {
    std::string result(begin_ + offset_, begin_ + offset_ + len);
    offset_ += len;
    return result;
  }

  /**
   * Read a value of type T off of the buffer, advancing cursor by appropriate
   * amount. Does NOT convert from network bytes order. It is the caller's
   * responsibility to do so if needed.
   * @tparam T type of value to read off. Preferably a primitive type.
   * @return the value of type T
   */
  template <typename T>
  T ReadRawValue() {
    T result;
    Read(sizeof(result), &result);
    return result;
  }

 private:
  size_t offset_ = 0, size_;
  ByteBuf::const_iterator begin_;
};

/**
 * A buffer specialize for read
 */
class ReadBuffer : public Buffer {
 public:
  /**
   * Instantiates a new buffer and reserve capacity many bytes.
   */
  explicit ReadBuffer(size_t capacity = SOCKET_BUFFER_CAPACITY) : Buffer(capacity) {}

  /**
   * Read as many bytes as possible using Posix from an fd
   * @param fd the file descriptor to  read from
   * @return the return value of posix read
   */
  int FillBufferFrom(int fd) {
    ssize_t bytes_read = read(fd, &buf_[size_], Capacity() - size_);
    if (bytes_read > 0) size_ += bytes_read;
    return static_cast<int>(bytes_read);
  }

  /**
   * Read the specified amount of bytes off from a ReadBufferView. The bytes
   * will be consumed (cursor moved) on the view and appended to the end
   * of this buffer
   * @param other The view to read from
   * @param size Number of bytes to read
   */
  void FillBufferFrom(ReadBufferView other, size_t size) {
    other.Read(size, &buf_[size_]);
    size_ += size;
  }

  /**
   * Read the specified amount of bytes off from another read buffer. The bytes
   * will be consumed (cursor moved) on the other buffer and appended to the end
   * of this buffer
   * @param other The other buffer to read from
   * @param size Number of bytes to read
   */
  void FillBufferFrom(const common::ManagedPointer<ReadBuffer> other, const size_t size) {
    FillBufferFrom(other->ReadIntoView(size), size);
  }

  /**
   * The number of bytes available to be consumed (i.e. meaningful bytes after
   * current read cursor)
   * @return The number of bytes available to be consumed
   */
  size_t BytesAvailable() { return size_ - offset_; }

  /**
   * Mark a chunk of bytes as read and return a view to the bytes read.
   *
   * This is necessary because a caller may not read all the bytes in a packet
   * before exiting (exception occurs, etc.). Reserving a view of the bytes in
   * a packet makes sure that the remaining bytes in a buffer is not malformed.
   *
   * No copying is performed in this process, however, so modifying the read buffer
   * when a view is in scope will cause undefined behavior on the view's methods
   *
   * @param bytes number of butes to read
   * @return a view of the bytes read.
   */
  ReadBufferView ReadIntoView(size_t bytes) {
    ReadBufferView result = ReadBufferView(bytes, buf_.begin() + offset_);
    offset_ += bytes;
    return result;
  }

  /**
   * Reads a generic value from the ReadBuffer
   * @tparam T The type to read
   * @return The read value
   */
  template <typename T>
  T ReadValue() {
    return ReadIntoView(sizeof(T)).ReadValue<T>();
  }

  /**
   * Reads a nul-terminated string from the head of the buffer
   * @return The read string
   */
  std::string ReadString() {
    std::string result = ReadCString(buf_.begin() + offset_, buf_.begin() + size_);
    offset_ += result.size() + 1;
    return result;
  }
};

/**
 * A buffer specialized for write
 */
class WriteBuffer : public Buffer {
 public:
  /**
   * Write as many bytes as possible using Posix write to fd
   * @param fd File descriptor to write out to
   * @return return value of Posix write
   */
  int WriteOutTo(int fd) {
    ssize_t bytes_written = write(fd, &buf_[offset_], size_ - offset_);
    if (bytes_written > 0) offset_ += bytes_written;
    return static_cast<int>(bytes_written);
  }

  /**
   * The remaining capacity of this buffer. This value is equal to the
   * maximum capacity minus the capacity already in use.
   * @return Remaining capacity
   */
  size_t RemainingCapacity() { return Capacity() - size_; }

  /**
   * @param bytes Desired number of bytes to write
   * @return Whether the buffer can accommodate the number of bytes given
   */
  bool HasSpaceFor(size_t bytes) { return RemainingCapacity() >= bytes; }

  /**
   * Append the desired range into current buffer.
   * @param src beginning of range
   * @param len length of range, in bytes
   */
  void AppendRaw(const void *src, size_t len) {
    if (len == 0) return;
    auto bytes_src = reinterpret_cast<const uchar *>(src);
    std::copy(bytes_src, bytes_src + len, std::begin(buf_) + size_);
    size_ += len;
  }

  // TODO(Tianyu): Just for io wrappers for now. Probably can remove later.
  /**
   *
   * @param src
   * @param len
   */
  void AppendRaw(ByteBuf::const_iterator src, size_t len) {
    if (len == 0) return;
    std::copy(src, src + len, std::begin(buf_) + size_);
    size_ += len;
  }

  /**
   * Append the given value into the current buffer. Does NOT convert to
   * network byte order. It is up to the caller to do so.
   * @tparam T input type
   * @param val value to write into buffer
   */
  template <typename T>
  void AppendRaw(T val) {
    AppendRaw(&val, sizeof(T));
  }
};

/**
 * A WriteQueue is a series of WriteBuffers that can buffer an uncapped amount
 * of writes without the need to copy and resize.
 *
 * It is expected that a specific protocol will wrap this to expose a better
 * API for protocol-specific behavior.
 */
class WriteQueue {
 public:
  /**
   * Instantiates a new WriteQueue. By default this holds one buffer.
   */
  WriteQueue() { Reset(); }

  /**
   * Reset the write queue to its default state.
   */
  void Reset() {
    buffers_.resize(1);
    offset_ = 0;
    flush_ = false;
    if (buffers_[0] == nullptr)
      buffers_[0] = std::make_unique<WriteBuffer>();
    else
      buffers_[0]->Reset();
  }

  /**
   * @return The head of the WriteQueue
   */
  common::ManagedPointer<WriteBuffer> FlushHead() {
    if (buffers_.size() > offset_) return common::ManagedPointer(buffers_[offset_]);
    return nullptr;
  }

  /**
   * Marks the head of the queue as flushed
   */
  void MarkHeadFlushed() { offset_++; }

  /**
   * Force this WriteQueue to be flushed next time the network layer
   * is available to do so.
   */
  void ForceFlush() { flush_ = true; }

  /**
   * Whether this WriteQueue should be flushed out to network or not.
   * A WriteQueue should be flushed either when the first buffer is full
   * or when manually set to do so (e.g. when the client is waiting for
   * a small response)
   * @return whether we should flush this write queue
   */
  bool ShouldFlush() { return flush_ || buffers_.size() > 1; }

  /**
   * Write len many bytes starting from src into the write queue, allocating
   * a new buffer if need be. The write is split up between two buffers
   * if breakup is set to true (which is by default)
   * @param src write head
   * @param len number of bytes to write
   * @param breakup whether to split write into two buffers if need be.
   */
  void BufferWriteRaw(const void *src, size_t len, bool breakup = true) {
    WriteBuffer &tail = *(buffers_[buffers_.size() - 1]);
    if (tail.HasSpaceFor(len)) {
      tail.AppendRaw(src, len);
    } else {
      // Only write partially if we are allowed to
      size_t written = breakup ? tail.RemainingCapacity() : 0;
      tail.AppendRaw(src, written);
      buffers_.push_back(std::make_unique<WriteBuffer>());
      BufferWriteRaw(reinterpret_cast<const uchar *>(src) + written, len - written);
    }
  }

  /**
   * Write val into the write queue, allocating a new buffer if need be.
   * The write is split up between two buffers if breakup is set to true
   * (which is by default). No conversion of byte ordering is performed. It is
   * up to the caller to do so if needed.
   * @tparam T type of value to write
   * @param val value to write
   * @param breakup whether to split write into two buffers if need be.
   */
  template <typename T>
  void BufferWriteRawValue(T val, bool breakup = true) {
    BufferWriteRaw(&val, sizeof(T), breakup);
  }

 private:
  friend class PacketWriter;
  std::vector<std::unique_ptr<WriteBuffer>> buffers_;
  size_t offset_ = 0;
  bool flush_ = false;
};

/**
 * Encapsulates an input packet
 */
struct InputPacket {
  ~InputPacket() {
    if (extended_) delete buf_;
  }

  /**
   * Type of message this packet encodes
   */
  NetworkMessageType msg_type_ = NetworkMessageType::NULL_COMMAND;

  /**
   * Length of this packet's contents
   */
  size_t len_ = 0;

  /**
   * ReadBuffer containing this packet's contents
   */
  ReadBuffer *buf_;

  /**
   * Whether or not this packet's header has been parsed yet
   */
  bool header_parsed_ = false;

  /**
   * Whether or not this packet's buffer was extended
   */
  bool extended_ = false;

  /**
   * Clears the packet's contents
   */
  virtual void Clear() {
    msg_type_ = NetworkMessageType::NULL_COMMAND;
    len_ = 0;

    if (extended_) delete buf_;
    buf_ = nullptr;
    header_parsed_ = false;
    extended_ = false;
  }
};

}  // namespace terrier::network
