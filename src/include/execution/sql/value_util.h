#pragma once

#include <memory>
#include <string>
#include <utility>

#include "common/strong_typedef.h"

namespace noisepage::execution::sql {

struct StringVal;

/**
 * Utility class for constructing values, currently just StringVals that may need separate allocation.
 */
class ValueUtil {
 public:
  ValueUtil() = delete;

  /**
   * Construct a StringVal, optionally allocating a byte buffer if the input can't be inlined.
   * @param string input
   * @param length input length
   * @return a pair of owned pointers, first to the StringVal and second to an optionally-allocated buffer
   */
  static std::pair<StringVal, std::unique_ptr<byte[]>> CreateStringVal(common::ManagedPointer<const char> string,
                                                                       uint32_t length);
  /**
   * Construct a StringVal, optionally allocating a byte buffer if the input can't be inlined.
   * @param string input
   * @return a pair of owned pointers, first to the StringVal and second to an optionally-allocated buffer
   */
  static std::pair<StringVal, std::unique_ptr<byte[]>> CreateStringVal(const std::string &string);
  /**
   * Construct a StringVal, optionally allocating a byte buffer if the input can't be inlined.
   * @param string input
   * @return a pair of owned pointers, first to the StringVal and second to an optionally-allocated buffer
   */
  static std::pair<StringVal, std::unique_ptr<byte[]>> CreateStringVal(std::string_view string);
  /**
   * Construct a StringVal, optionally allocating a byte buffer if the input can't be inlined.
   * @param string input
   * @return a pair of owned pointers, first to the StringVal and second to an optionally-allocated buffer
   */
  static std::pair<StringVal, std::unique_ptr<byte[]>> CreateStringVal(StringVal string);
};

}  // namespace noisepage::execution::sql
