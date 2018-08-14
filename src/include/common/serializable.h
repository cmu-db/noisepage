/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 * This software may be modified and distributed under the terms           *
 * of the MIT license.  See the LICENSE file for details.                  *
 *                                                                         *
 ***************************************************************************/
#pragma once

#include <limits>
#include <string>
#include <vector>
#include "common/byte_array.h"
#include "common/macros.h"

namespace terrier {

/**
 * A SerializeInput is an abstract class for reading-from-memory buffers
 */
class SerializeInput {
  /** No implicit copies */
  SerializeInput(const SerializeInput &) = delete;
  SerializeInput &operator=(const SerializeInput &) = delete;

 protected:
  /**
   * @brief Constructs a reading-from-memory buffer with no initialization.
   *
   * @warning It does no initialization. Subclasses must call initialize.
   */
  SerializeInput() : current_(NULL), end_(NULL) {}

  /**
   * @brief Initializes a reading-from-memory buffer with the given data and
   * length.
   *
   * @param data the data to initialize the reading-from-memory buffer
   * @param length the number of bytes of the data
   */
  void Initialize(const void *data, uint32_t length) {
    current_ = reinterpret_cast<const std::byte *>(data);
    end_ = current_ + length;
  }

 public:
  /**
   * @brief Pure virtual destructor to permit subclasses to customize
   * destruction.
   */
  virtual ~SerializeInput() {}

  /**
   * @brief Gets a pointer to the current read position of the internal data
   * buffer, and advances the read position by length.
   *
   * @param length the length by which the pointer is advanced
   *
   * @return The pointer advanced of the internal data buffer.
   */
  const void *getRawPointer(uint32_t length) {
    const void *result = current_;
    current_ += length;
    PELOTON_ASSERT(current_ <= end_,
                   "the current read position after being advanced must be less than or "
                   "equal to the end position of the buffer");
    return result;
  }

  /**
   * @brief Move the read position back by bytes.
   *
   * @param bytes the number of bytes by which the read position is moved back
   *
   * @warning this method is currently unverified and could result in reading
   * before the beginning of the buffer.
   */
  // TODO(Aaron): Change the implementation to validate this?
  void Unread(uint32_t bytes) { current_ -= bytes; }

  /*-----------------------------------------------------------------------
   * functions for deserialization
   *-----------------------------------------------------------------------*/
  /**
   * @brief Copies a char value from the buffer, advancing the read position by
   * bytes of a char type.
   *
   * @return the char value read
   */
  char ReadChar() { return ReadPrimitive<char>(); }

  /**
   * @brief Copies a byte value from the buffer, advancing the read position by
   * a byte.
   *
   * @return the byte value read
   */
  std::byte ReadByte() { return ReadPrimitive<std::byte>(); }

  /**
   * @brief Copies a short value from the buffer, advancing the read position by
   * bytes of a short type.
   *
   * @return the short value read
   */
  int16_t ReadShort() { return ReadPrimitive<int16_t>(); }

  /**
   * @brief Copies an int value from the buffer, advancing the read position by
   * bytes of a int type.
   *
   * @return the int value read
   */
  int32_t ReadInt() { return ReadPrimitive<int32_t>(); }

  /**
   * @brief Copies a bool value from the buffer, advancing the read position by
   * bytes of a bool type.
   *
   * @return the bool value read
   */
  bool ReadBool() { return static_cast<bool>(ReadByte()); }

  /**
   * @brief Copies a enum value in a single byte from the buffer, advancing the
   * read position by a byte.
   *
   * @return the byte value read
   */
  std::byte ReadEnumInSingleByte() { return ReadByte(); }

  /**
   * @brief Copies a long value from the buffer, advancing the read position by
   * bytes of a long type.
   *
   * @return the long value read
   */
  int64_t ReadLong() { return ReadPrimitive<int64_t>(); }

  /**
   * @brief Copies a float value from the buffer, advancing the read position by
   * bytes of a float type.
   *
   * @return the float value read
   */
  float ReadFloat() { return ReadPrimitive<float>(); }

  /**
   * @brief Copies a double value from the buffer, advancing the read position by
   * bytes of a double type.
   *
   * @return the double value read
   */
  double ReadDouble() { return ReadPrimitive<double>(); }

  /**
   * @brief Copies a string from the buffer.
   *
   * @return The string copied.
   */
  std::string ReadTextString() {
    int16_t stringLength = ReadShort();
    PELOTON_ASSERT(stringLength >= 0, "the length of the string copied is less than 0");
    return std::string(reinterpret_cast<const char *>(getRawPointer(stringLength)), stringLength);
  }

  /**
   * @brief Copies a ByteArray from the buffer.
   *
   * @return The ByteArray copied.
   */
  ByteArray ReadBinaryString() {
    int16_t stringLength = ReadShort();
    PELOTON_ASSERT(stringLength >= 0, "the length of the string copied is less than 0");
    return ByteArray(reinterpret_cast<const std::byte *>(getRawPointer(stringLength)), stringLength);
  }

  /**
   * @brief Copies the next length bytes from the buffer to destination.
   *
   * @param destination the memory address to which the bytes are copied
   * @param length the length of bytes copied
   */
  void ReadBytes(void *destination, uint32_t length) { PELOTON_MEMCPY(destination, getRawPointer(length), length); }

  /**
   * @brief Copies a vector from the buffer.
   *
   * @tparam T the type of the vector
   *
   * @param vec the memory address to which the vector is copied
   */
  template <typename T>
  void ReadSimpleTypeVector(std::vector<T> *vec) {
    int size = ReadInt();
    PELOTON_ASSERT(size >= 0, "the size of the vector copied is less than 0");
    vec->resize(size);
    for (int i = 0; i < size; ++i) {
      vec[i] = ReadPrimitive<T>();
    }
  }
  /*-----------------------------------------------------------------------*/

 private:
  /**
   * @brief Copies a value from the buffer, advancing the the read position by
   * bytes read.
   *
   * @tparam T the type of the value
   *
   * @return the value read
   */
  template <typename T>
  T ReadPrimitive() {
    T value;
    PELOTON_MEMCPY(&value, current_, sizeof(value));
    current_ += sizeof(value);
    return value;
  }

  /** Current read position */
  const std::byte *current_;
  /** End of the buffer. Valid byte range: current_ <= validPointer < end_. */
  const std::byte *end_;
};

/**
 * A SerializeOutput is an abstract class for writing-to-memory buffers
 * Subclasses may optionally support resizing.
 */
class SerializeOutput {
  /** No implicit copies */
  SerializeOutput(const SerializeOutput &) = delete;
  SerializeOutput &operator=(const SerializeOutput &) = delete;

 protected:
  /**
   * @brief Constructs a writing-to-memory buffer with no initialization.
   */
  SerializeOutput() : buffer_(NULL), position_(0), capacity_(0) {}

  /**
   * @brief Sets the buffer to buffer with capacity to initialize it.
   *
   * @param buffer the writing-to-memory buffer to be initialized
   * @param capacity the capacity of the buffer to be set
   *
   * @note this does not change the position.
   */
  void Initialize(void *buffer, uint32_t capacity) {
    buffer_ = reinterpret_cast<std::byte *>(buffer);
    PELOTON_ASSERT(position_ <= capacity, "the capacity must be greater than or equal to the current write position");
    capacity_ = capacity;
  }

  /**
   * @brief Sets the write position of the buffer.
   *
   * @param position the write position to be set
   */
  void SetPosition(uint32_t position) { this->position_ = position; }

 public:
  /**
   * @brief Pure virtual destructor to permit subclasses to customize
   * destruction.
   */
  virtual ~SerializeOutput() {}

  /**
   * @brief Gets a pointer to the beginning of the buffer, for reading the
   * serialized data.
   *
   * @return The pointer to the beginning of the buffer.
   */
  const std::byte *Data() const { return buffer_; }

  /**
   * @brief Gets the number of bytes written in to the buffer.
   *
   * @return the number of bytes written in to the buffer
   */
  uint32_t Size() const { return position_; }

  /**
   * @brief Gets the write position of the buffer.
   *
   * @return the write position of the buffer
   */
  uint32_t Position() const { return position_; }

  /**
   * @brief Resets the write position of the buffer to 0.
   */
  void Reset() { SetPosition(0); }

  /**
   * @brief Returns whether words are represented in little-endian
   *
   * @return true if words are represented in little-endian, false otherwise
   */
  static bool IsLittleEndian() {
    static const uint16_t s = 0x0001;
    uint8_t byte;
    PELOTON_MEMCPY(&byte, &s, 1);
    return byte != 0;
  }

  /**
   * @brief Reserves length bytes of space for writing.
   *
   * @param length the length of bytes reserved
   *
   * @return the offset to the start of the reserved bytes
   */
  uint32_t ReserveBytes(uint32_t length) {
    AssureExpand(length);
    uint32_t offset = position_;
    position_ += length;
    return offset;
  }

  /*-----------------------------------------------------------------------
   * functions for serialization
   *-----------------------------------------------------------------------*/
  /**
   * @brief Writes a char value to the buffer, advancing the write position by
   * bytes of a char type.
   *
   * @param value the char value to be written
   */
  void WriteChar(char value) { WritePrimitive(value); }

  /**
   * @brief Writes a byte value to the buffer, advancing the write position by
   * a byte.
   *
   * @param value the byte value to be written
   */
  void WriteByte(std::byte value) { WritePrimitive(value); }

  /**
   * @brief Writes a short value to the buffer, advancing the write position by
   * bytes of a short type.
   *
   * @param value the short value to be written
   */
  void WriteShort(int16_t value) { WritePrimitive(value); }

  /**
   * @brief Writes an int value to the buffer, advancing the write position by
   * bytes of an int type.
   *
   * @param value the int value to be written
   */
  void WriteInt(int32_t value) { WritePrimitive(value); }

  /**
   * @brief Writes a bool value to the buffer, advancing the write position by
   * bytes of a bool type.
   *
   * @param value the bool value to be written
   */
  void WriteBool(bool value) { WriteByte(value ? std::byte(1) : std::byte(0)); }

  /**
   * @brief Writes a long value to the buffer, advancing the write position by
   * bytes of a long type.
   *
   * @param value the long value to be written
   */
  void WriteLong(int64_t value) { WritePrimitive(value); }

  /**
   * @brief Writes a float value to the buffer, advancing the write position by
   * bytes of a float type.
   *
   * @param value the float value to be written
   */
  void WriteFloat(float value) { WritePrimitive(value); }

  /**
   * @brief Writes a double value to the buffer, advancing the write position by
   * bytes of a double type.
   *
   * @param value the double value to be written
   */
  void WriteDouble(double value) { WritePrimitive(value); }

  /**
   * @brief Writes a enum value in a single byte to the buffer, advancing the
   * write position by a byte.
   *
   * @param value the enum value to be written
   */
  void WriteEnumInSingleByte(int value) {
    PELOTON_ASSERT(std::numeric_limits<int8_t>::min() <= value && value <= std::numeric_limits<int8_t>::max(),
                   "the enum value written must be between the minimum and maximum value of type int8_t");
    WriteByte(static_cast<std::byte>(value));
  }

  /**
   * @brief A helper function which explicitly accepts char* and length
   * (or ByteArray), as std::string's implicit construction is unsafe.
   *
   * @param value the pointer to the string or ByteArray to be written
   * @param length the length of the string or ByteArray to be written
   */
  void WriteBinaryString(const void *value, int16_t length) {
    PELOTON_ASSERT(length <= std::numeric_limits<int16_t>::max(),
                   "the length must be less than or equal to the maximum value of type int16_t");
    int16_t stringLength = static_cast<int16_t>(length);
    AssureExpand(length + sizeof(stringLength));

    std::byte *current = buffer_ + position_;
    PELOTON_MEMCPY(current, &stringLength, sizeof(stringLength));
    current += sizeof(stringLength);
    PELOTON_MEMCPY(current, value, length);
    position_ += sizeof(stringLength) + length;
  }

  /**
   * @brief Writes a ByteArray to the buffer, advancing the write position by
   * bytes of a ByteArray.
   *
   * @param value the ByteArray to be written
   */
  void WriteBinaryString(const ByteArray &value) { WriteBinaryString(value.Data(), value.Length()); }

  /**
   * @brief Writes a string to the buffer, advancing the write position by
   * bytes of a string.
   *
   * @param value the string to be written
   */
  void WriteTextString(const std::string &value) { WriteBinaryString(value.data(), value.size()); }

  /**
   * @brief Writes bytes of the given length to the buffer, advancing the write
   * position by the number of bytes written.
   *
   * @param value the pointer to the bytes to be written
   * @param length the length of the bytes to be written
   */
  void WriteBytes(const void *value, uint32_t length) {
    AssureExpand(length);
    PELOTON_MEMCPY(buffer_ + position_, value, length);
    position_ += length;
  }

  /**
   * @brief Writes 0s of the given length to the buffer, advancing the write
   * position by the number of 0s written.
   *
   * @param length the length of the 0s to be written
   */
  void WriteZeros(uint32_t length) {
    AssureExpand(length);
    PELOTON_MEMSET(buffer_ + position_, 0, length);
    position_ += length;
  }

  /**
   * @brief Writes a vector to the buffer.
   *
   * @tparam T the type of the vector
   *
   * @param vec the reference of the vector to be written
   */
  template <typename T>
  void WriteSimpleTypeVector(const std::vector<T> &vec) {
    PELOTON_ASSERT(vec.size() <= std::numeric_limits<int>::max(),
                   "the size of the vector must be less than or equal to the maximum value of type int");
    int size = static_cast<int>(vec.size());

    // Resize the buffer once
    AssureExpand(sizeof(size) + size * sizeof(T));

    WriteInt(size);
    for (int i = 0; i < size; ++i) {
      WritePrimitive(vec[i]);
    }
  }

  /**
   * @brief Writes an int value to the buffer at given position.
   *
   * @param position the position where the value is written
   * @param value the int value to be written
   */
  uint32_t WriteIntAt(uint32_t position, int32_t value) { return WritePrimitiveAt<int32_t>(position, value); }

  /**
   * @brief Copies length bytes from value to this buffer, starting at offset.
   *
   * @param offset the offset to which the bytes are copied
   * @param value the value from which the bytes are copied
   * @param length the length of bytes to be copied
   *
   * @note Offset should have been obtained from reserveBytes. This does not
   * affect the current write position.
   *
   * @return the next write position where the copied value will not be overwritten
   */
  uint32_t WriteBytesAt(uint32_t offset, const void *value, uint32_t length) {
    PELOTON_ASSERT(offset + length <= position_, "It writes bytes beyond the current write position");
    PELOTON_MEMCPY(buffer_ + offset, value, length);
    return offset + length;
  }

  /**
   * @brief Copies the value to this buffer, starting at position.
   *
   * @tparam T the type of the value
   *
   * @param position the position to which the value is copied
   * @param value the value to be copied
   *
   * @return the next write position where the copied value will not be overwritten
   */
  template <typename T>
  uint32_t WritePrimitiveAt(uint32_t position, T value) {
    return WriteBytesAt(position, &value, sizeof(value));
  }
  /*-----------------------------------------------------------------------*/

 protected:
  /**
   * @brief Expands the buffer when trying to write past the end of it.
   * Subclasses can optionally resize the buffer by calling initialize.
   *
   * @param minimum_desired the minimum length the resized buffer needs to have.
   *
   * @warning If this function returns and size() < minimum_desired, the program
   * will crash.
   */
  virtual void Expand(uint32_t minimum_desired) = 0;

 private:
  /**
   * @brief Writes a value to the buffer, advancing the the write position by
   * bytes written.
   *
   * @tparam T the type of the value
   *
   * @return the value written
   */
  template <typename T>
  void WritePrimitive(T value) {
    AssureExpand(sizeof(value));
    PELOTON_MEMCPY(buffer_ + position_, &value, sizeof(value));
    position_ += sizeof(value);
  }

  /**
   * @brief Assures to expand the buffer to minimum desired size if its current
   * capacity is not enough.
   *
   * @param next_write the number of bytes the next write needs
   */
  void AssureExpand(uint32_t next_write) {
    uint32_t minimum_desired = position_ + next_write;
    if (minimum_desired > capacity_) {
      Expand(minimum_desired);
    }
    PELOTON_ASSERT(capacity_ >= minimum_desired, "the minimum desired size is greater than the buffer capacity");
  }

  /** Beginning of the buffer. */
  std::byte *buffer_;
  /** Current write position in the buffer. */
  uint32_t position_;
  /** Total bytes this buffer can contain. */
  uint32_t capacity_;
};

/**
 * A ReferenceSerializeInput is an implementation for SerializeInput that
 * references an existing buffer.
 */
class ReferenceSerializeInput : public SerializeInput {
 public:
  /**
   * @brief Constructs a reading-from-memory buffer referencing an existing one
   * by specifying its data and length
   *
   * @param data the data of the buffer to be referenced
   * @param length the length of the buffer to be referenced
   */
  ReferenceSerializeInput(const void *data, uint32_t length) { Initialize(data, length); }

  /**
   * @brief Destructor does nothing since there is nothing to clean up. Pure
   * virtual destructor to permit subclasses to customize destruction.
   */
  virtual ~ReferenceSerializeInput() {}
};

/**
 * A ReferenceSerializeOutput is an implementation for SerializeOutput that
 * references an existing buffer.
 */
class ReferenceSerializeOutput : public SerializeOutput {
 public:
  /**
   * @brief Constructs a writing-to-memory buffer referencing an existing one
   * by specifying its data and length
   *
   * @param data the data of the buffer to be referenced
   * @param length the length of the buffer to be referenced
   */
  ReferenceSerializeOutput(void *data, uint32_t length) : SerializeOutput() { Initialize(data, length); }

  /**
   * @brief Sets the buffer to the existing one with a specified capacity and
   * sets the position.
   *
   * @param buffer the existing buffer
   * @param capacity the capacity of the buffer
   * @param position the write position to be set for the buffer
   */
  void InitializeWithPosition(void *buffer, uint32_t capacity, uint32_t position) {
    SetPosition(position);
    Initialize(buffer, capacity);
  }

  /**
   * @brief Destructor does nothing since there is nothing to clean up. Pure
   * virtual destructor to permit subclasses to customize destruction.
   */
  virtual ~ReferenceSerializeOutput() {}

 protected:
  /** Reference output can't resize the buffer: just crash. */
  virtual void Expand(UNUSED_ATTRIBUTE uint32_t minimum_desired) {
    PELOTON_ASSERT(false, "Reference output can't resize the buffer");
    abort();
  }
};

/**
 * A CopySerializeInput is an implementation for SerializeInput that makes a
 * copy of existing data.
 */
class CopySerializeInput : public SerializeInput {
 public:
  /**
   * @brief Constructs a reading-from-memoty buffer by making a copy of existing
   * data, specifying the data and length
   *
   * @param data the data to be copied
   * @param length the length of the data to be copied
   */
  CopySerializeInput(const void *data, uint32_t length)
      : bytes_(reinterpret_cast<const std::byte *>(data), static_cast<int>(length)) {
    Initialize(bytes_.Data(), static_cast<int>(length));
  }

  /**
   * @brief Destructor frees the ByteArray.
   */
  virtual ~CopySerializeInput() {}

 private:
  ByteArray bytes_;
};

/**
 * A CopySerializeOutput is an implementation for SerializeOutput that makes a
 * new copy.
 */
class CopySerializeOutput : public SerializeOutput {
 public:
  /**
   * @brief Constructs a writing-to-memory buffer by making a new copy
   */
  CopySerializeOutput() : bytes_(0) { Initialize(NULL, 0); }

  /**
   * @brief Destructor frees the ByteArray.
   */
  virtual ~CopySerializeOutput() {}

 protected:
  /**
   * @brief Resize this buffer to contain twice the amount desired.
   *
   * @param minimum_desired the minimum amount of memory desired
   */
  virtual void Expand(uint32_t minimum_desired) {
    uint32_t next_capacity = (bytes_.Length() + minimum_desired) * 2;
    PELOTON_ASSERT(next_capacity < std::numeric_limits<int>::max(),
                   "the next capacity must be less than or equal to the maximum value of type int");
    bytes_.CopyAndExpand(static_cast<int>(next_capacity));
    Initialize(bytes_.Data(), next_capacity);
  }

 private:
  ByteArray bytes_;
};
}  // namespace terrier
