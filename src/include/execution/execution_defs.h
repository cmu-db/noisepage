#pragma once
#include "common/macros.h"

namespace terrier::execution {
// All builtin operators we currently support
// TODO(Tianyu): convert to snake case all caps like other enums in the codebase?
enum class OperatorId : uint32_t {
  Negation = 0,
  Abs,
  Add,
  Sub,
  Mul,
  Div,
  Mod,
  LogicalAnd,
  LogicalOr,
  Ascii,
  Chr,
  Concat,
  Substr,
  CharLength,
  OctetLength,
  Length,
  Repeat,
  Replace,
  LTrim,
  RTrim,
  BTrim,
  Trim,
  Sqrt,
  Ceil,
  Round,
  DatePart,
  Floor,
  DateTrunc,
  Like,
  Now,

  // Add more operators here, before the last "Invalid" entry
  Invalid
};

enum class OnError : uint32_t { RETURN_NULL, THROW_EXCEPTION };
}  // namespace terrier::execution