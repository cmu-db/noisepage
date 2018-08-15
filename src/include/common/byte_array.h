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

#include <cassert>
#include <cstring>
#include <memory>
#include "common/macros.h"

namespace terrier {
/**
 * @author Hideaki Kimura <hkimura@cs.brown.edu>
 *
 * @brief A safe and handy byte* container.
 *
 * std::string is a good container, but we have to be very careful
 * for a binary string that might include '\0' in arbitrary position
 * because of its implicit construction from const char*.
 * std::vector<char> or boost::array<char, N> works, but we can't pass
 * the objects without copying the elements, which has significant overhead.
 *
 * I made this class to provide same semantics as Java's "byte[]"
 * so that this class guarantees following properties.
 *
 * 1. ByteArray is always safe against '\0'.
 *  This class has no method that implicitly accepts std::string which can
 *  be automatically constructed from NULL-terminated const char*. Be careless!
 * 2. ByteArray has explicit "length" property.
 *  This is what boost::shared_array<char> can't provide.
 * 3. Passing ByteArray (not ByteArray* nor ByteArray&) has almost no cost.
 *  This is what boost::array<char, N> and std::vector<char> can't provide,
 *  which copies elements everytime.
 *  Copy constructor of this class just copies an internal smart pointer.
 *  You can pass around instances of this class just like smart pointer/iterator.
 * 4. No memory leaks.
 * 5. All methods are exception-safe. Nothing dangerouns happens even if Outofmemory happens.
 *
 * @warning NEVER make a constructor that accepts std::string! It demolishes all
 * the significance of this class.
 */
template <typename T>
class GenericArray {
 public:
  /**
   * @brief Constructs a GenericArray with reset data and length.
   *
   * Corresponds to "byte[] bar = null;" in Java
   */
  GenericArray() { Reset(); }

  /**
   * @brief Constructs a GenericArray with the given length.
   *
   * Corresponds to "byte[] bar = new byte[len];" in Java. Explicit because
   * ByteArray bar = 10; sounds really weird in the semantics.
   *
   * @param length the length of the GenericArray to be constructed
   */
  explicit GenericArray(int length) { ResetAndExpand(length); }

  /**
   * @brief Constructs a GenericArray with the given data and length.
   *
   * Corresponds to "byte[] bar = new byte[] {1,2,...,10};" in Java. This constructor
   * is safe because it explicitly receives length.
   *
   * @param data the data of the GenericArray to be constructed
   * @param length the length of the GenericArray to be constructed
   */
  GenericArray(const T *data, int length) {
    ResetAndExpand(length);
    Assign(data, 0, length);
  }

  /**
   * @brief A copy constructor to construct a GenericArray with the given
   * GenericArray.
   *
   * Corresponds to "byte[] bar = bar2;" in Java.
   *
   * @param rhs the given GenericArray used to construct a GenericArray
   *
   * @note This has almost no cost. Because the copy constructor just copies
   * internal smart pointers. You can pass around instances of this class just
   * like smart pointers/iterators.
   */
  GenericArray(const GenericArray<T> &rhs) {
    data_ = rhs.data_;
    length_ = rhs.length_;
  }

  /**
   * @brief Overloads = (assignment) operator to construct a GenericArray with
   * the given GenericArray.
   *
   * @param rhs the given GenericArray used to construct a GenericArray
   *
   * @return the constructed GenericArray
   *
   * @note This has almost no cost. Because the overloaded assignment operator
   * just copies internal smart pointers. You can pass around instances of this class just
   * like smart pointers/iterators.
   */
  GenericArray<T> &operator=(const GenericArray<T> &rhs) {
    data_ = rhs.data_;
    length_ = rhs.length_;
    return *this;
  }

  /**
   * @brief Returns whether the data of the GenericArray is null.
   *
   * Corresponds to "(bar == null)" in Java
   *
   * @return true if the data of the GenericArray is null, false otherwise
   */
  bool IsNull() const { return data_ == nullptr; }

  /**
   * @brief Resets the data and length of the GenericArray.
   *
   * Corresponds to "bar = null;" in Java
   */
  void Reset() {
    data_.reset();
    length_ = -1;
  }

  /**
   * @brief Resets the data and length of the GenericArray, and expandss the length
   * to the given one.
   *
   * Corresponds to "bar = new byte[len];" in Java
   *
   * @param new_length the length to be expanded to
   */
  void ResetAndExpand(int new_length) {
    PELOTON_ASSERT(new_length >= 0, "new length must be greater than or equal to 0");
    data_ = std::shared_ptr<T>(new T[new_length], std::default_delete<T[]>{});
    PELOTON_MEMSET(data_.get(), 0, new_length * sizeof(T));
    length_ = new_length;
  }

  /**
   * @brief Copies the data and length of the GenericArray, and expands the length
   * to the given one while retaining the data.
   *
   * Corresponds to "tmp = new byte[newlen]; System.arraycopy(bar to tmp); bar = tmp;" in Java
   *
   * @param new_length the length to be expanded to
   */
  void CopyAndExpand(int new_length) {
    PELOTON_ASSERT(new_length >= 0, "new length must be greater than or equal to 0");
    PELOTON_ASSERT(new_length > length_, "new length must be greater than the original one");
    std::shared_ptr<T> new_data(new T[new_length], std::default_delete<T[]>{});
    PELOTON_MEMSET(new_data.get(), 0, new_length * sizeof(T));  // makes valgrind happy.
    PELOTON_MEMCPY(new_data.get(), data_.get(), length_ * sizeof(T));
    data_ = new_data;
    length_ = new_length;
  }

  /**
   * @brief Returns the length of the GenericArray.
   *
   * Corresponds to "(bar.length)" in Java
   *
   * @return the length of the GenericArray
   */
  int Length() const { return length_; }

  /**
   * @brief Returns the data of the GenericArray.
   *
   * @return the data of the GenericArray
   */
  const T *Data() const { return data_.get(); }

  /**
   * @brief Returns the data of the GenericArray.
   *
   * @return the data of the GenericArray
   */
  T *Data() { return data_.get(); }

  /**
   * @brief A helper function for copying content of the assigned length from
   * the assigned data to the offset of the GenericArray
   *
   * @param assigned_data the source of data to be copied
   * @param offset the offset where the content is to be copied
   * @param assigned_length the assigned length of content to be copied
   */
  void Assign(const T *assigned_data, int offset, int assigned_length) {
    PELOTON_ASSERT(!IsNull(), "the data of the GenericArray is null");
    PELOTON_ASSERT(length_ >= offset + assigned_length,
                   "the current length of the GenericArray is not enough for copying content "
                   "of the assigned length from the assigned data");
    PELOTON_ASSERT(offset >= 0, "the offset must be greater than or equal to 0");
    PELOTON_MEMCPY(data_.get() + offset, assigned_data, assigned_length * sizeof(T));
  }

  /**
   * @brief Overloads + (plus) operator to concatenate two GenericArrays.
   *
   * @param tail the second GenericArray to be concatenated
   *
   * @return the concatenated GenericArray
   */
  GenericArray<T> operator+(const GenericArray<T> &tail) const {
    PELOTON_ASSERT(!IsNull(), "the data of the first GenericArray is null");
    PELOTON_ASSERT(!tail.IsNull(), "the data of the second GenericArray is null");
    GenericArray<T> concated(this->length_ + tail.length_);
    concated.Assign(this->data_.get(), 0, this->length_);
    concated.Assign(tail.data_.get(), this->length_, tail.length_);
    return concated;
  }

  /**
   * @brief Overloads [] operator to return a reference to the element at
   * position index
   *
   * @param index a position of an element
   *
   * @return the reference to the element at position index
   */
  const T &operator[](int index) const {
    PELOTON_ASSERT(!IsNull(), "the data of the GenericArray is null");
    PELOTON_ASSERT(length_ > index,
                   "the index is greater than or equal to the "
                   "length of the GenericArray");
    return data_.get()[index];
  }

  /**
   * @brief Overloads [] operator to return a reference to the element at
   * position index
   *
   * @param index a position of an element
   *
   * @return the reference to the element at position index
   */
  T &operator[](int index) {
    PELOTON_ASSERT(!IsNull(), "the data of the GenericArray is null");
    PELOTON_ASSERT(length_ > index,
                   "the index is greater than or equal to the "
                   "length of the GenericArray");
    return data_.get()[index];
  }

 private:
  std::shared_ptr<T> data_;
  int length_;
};

using ByteArray = GenericArray<std::byte>;
}  // namespace terrier
