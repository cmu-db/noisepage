#pragma once

#include <string_view>

#include "execution/util/region.h"

namespace noisepage::execution::util {

/**
 * Container for small-ish strings whose lifetimes are all the same.
 * Used during execution for intermediate strings, and in string vectors.
 * Actually a thin wrapper around a memory region.
 */
class StringHeap {
 public:
  /**
   * Construct a new empty string heap.
   */
  StringHeap();

  /**
   * Move constructor.
   */
  StringHeap(StringHeap &&) = default;

  /**
   * This class cannot be copied.
   */
  DISALLOW_COPY(StringHeap);

  /**
   * Move assignment.
   * @return This string heap.
   */
  StringHeap &operator=(StringHeap &&) = default;

  /**
   * Return the number of string currently in this heap.
   * @return The number of strings in the heap.
   */
  [[nodiscard]] uint32_t GetNumStrings() const { return num_strings_; }

  /**
   * Allocate a string of the given length from the heap.
   * @param string_len The length of the string.
   * @return A pointer to the allocate string.
   */
  [[nodiscard]] char *Allocate(std::size_t string_len);

  /**
   * Copy the given string into the heap, returning a pointer to its heap address.
   * @param str The string whose contents will be copied into the newly allocated string.
   * @return The pointer to the string in this heap whose contents are equal to the provided string.
   */
  [[nodiscard]] char *AddString(std::string_view str);

  /**
   * Deallocate all memory in this heap. This will invalidate any pointers that have been returned
   * through allocation. Callers must ensure that all pointers into this heap are no longer in use.
   */
  void Destroy();

 private:
  /** Where the strings live. */
  util::Region region_;
  /** Number of strings. */
  uint32_t num_strings_{0};
};

// ---------------------------------------------------------
// Implementation
// ---------------------------------------------------------

inline StringHeap::StringHeap() : region_("strings") {}

inline char *StringHeap::Allocate(std::size_t string_len) {
  num_strings_++;

  // Allocate string-length bytes + 1 for the NULL terminator
  const std::size_t num_bytes = string_len + 1;
  return reinterpret_cast<char *>(region_.Allocate(num_bytes));
}

inline char *StringHeap::AddString(std::string_view str) {
  num_strings_++;

  // Allocate string-length bytes + 1 for the NULL terminator
  const std::size_t num_bytes = str.length() + 1;
  void *ptr = region_.Allocate(num_bytes, alignof(char *));
  std::memcpy(ptr, str.data(), num_bytes);

  return reinterpret_cast<char *>(ptr);
}

inline void StringHeap::Destroy() { region_.FreeAll(); }

}  // namespace noisepage::execution::util
