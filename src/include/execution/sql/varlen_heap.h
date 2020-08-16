#pragma once

#include <string>

#include "execution/util/execution_common.h"
#include "execution/util/string_heap.h"
#include "storage/varlen_entry.h"

namespace terrier::execution::sql {

//===----------------------------------------------------------------------===//
//
// Variable-length values
//
//===----------------------------------------------------------------------===//

/**
 * A container for varlens.
 */
class EXPORT VarlenHeap {
 public:
  /**
   * Allocate memory from the heap whose contents will be filled in by the user BEFORE creating a varlen.
   * @param len The length of the varlen to allocate.
   * @return The character byte array.
   */
  char *PreAllocate(std::size_t len) { return heap_.Allocate(len); }

  /**
   * Allocate a varlen from this heap whose contents are the same as the input string.
   * @param str The string to copy into the heap.
   * @param len The length of the input string.
   * @return A varlen.
   */
  storage::VarlenEntry AddVarlen(const char *str, std::size_t len) {
    auto *content = heap_.AddString(std::string_view(str, len));
    return storage::VarlenEntry::Create(reinterpret_cast<byte *>(content), len, false);
  }

  /**
   * Allocate and return a varlen from this heap whose contents as the same as the input string.
   * @param string The string to copy into the heap.
   * @return A varlen.
   */
  storage::VarlenEntry AddVarlen(const std::string &string) { return AddVarlen(string.c_str(), string.length()); }

  /**
   * Add a copy of the given varlen into this heap.
   * @param other The varlen to copy into this heap.
   * @return A new varlen entry.
   */
  storage::VarlenEntry AddVarlen(const storage::VarlenEntry &other) {
    return AddVarlen(reinterpret_cast<const char *>(other.Content()), other.Size());
  }

  /**
   * Destroy all heap-allocated varlens.
   */
  void Destroy() { heap_.Destroy(); }

 private:
  // Internal heap of strings
  util::StringHeap heap_;
};
}  // namespace terrier::execution::sql
