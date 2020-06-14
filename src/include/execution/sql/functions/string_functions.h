#pragma once

#include <limits>
#include <string>

#include "common/all_static.h"
#include "execution/sql/value.h"

namespace terrier::execution::exec {
class ExecutionContext;
}

namespace terrier::execution::sql {

/**
 * Utility class to handle SQL string manipulations.
 */
class StringFunctions : public common::AllStatic {
 public:
  static void CharLength(Integer *result, exec::ExecutionContext *ctx, const StringVal &str) {
    Length(result, ctx, str);
  }

  static void Concat(StringVal *result, exec::ExecutionContext *ctx, const StringVal &left, const StringVal &right);

  static void Left(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n);

  static void Length(Integer *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void Lower(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void Lpad(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &len,
                   const StringVal &pad);

  static void Ltrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &chars);

  static void Ltrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void Repeat(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n);

  static void Reverse(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void Right(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n);

  static void Rpad(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n,
                   const StringVal &pad);

  static void Rtrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &chars);

  static void Rtrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void SplitPart(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &delim,
                        const Integer &field);

  static void Substring(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &pos,
                        const Integer &len);

  static void Substring(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &pos) {
    Substring(result, ctx, str, pos, Integer(std::numeric_limits<int64_t>::max()));
  }

  static void Trim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &chars);

  static void Trim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void Upper(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  static void Like(BoolVal *result, exec::ExecutionContext *ctx, const StringVal &string, const StringVal &pattern);
};

}  // namespace terrier::execution::sql
