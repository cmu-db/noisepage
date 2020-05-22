#pragma once

#include <memory>
#include <string>
#include <utility>

#include "common/strong_typedef.h"

namespace terrier::execution::sql {

struct StringVal;

class ValueUtil {
 public:
  ValueUtil() = delete;

  static std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> CreateStringVal(
      common::ManagedPointer<const char> string, uint32_t len);
  static std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> CreateStringVal(const std::string &string);
  static std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> CreateStringVal(std::string_view string);
  static std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> CreateStringVal(
      common::ManagedPointer<StringVal> string);
};

}  // namespace terrier::execution::sql
