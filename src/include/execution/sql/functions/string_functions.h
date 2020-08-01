#pragma once

#include <limits>
#include <string>

#include "execution/sql/value.h"

namespace terrier::execution::exec {
class ExecutionContext;
}

namespace terrier::execution::sql {

/**
 * Utility class to handle SQL string manipulations.
 */
class StringFunctions {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(StringFunctions);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(StringFunctions);

  /** Compute LENGTH(str). */
  static void CharLength(Integer *result, exec::ExecutionContext *ctx, const StringVal &str) {
    Length(result, ctx, str);
  }

  /** Compute CONCAT(left, right). */
  static void Concat(StringVal *result, exec::ExecutionContext *ctx, const StringVal &left, const StringVal &right);

  /** Compute LEFT(str, n). */
  static void Left(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n);

  /** Compute LENGTH(str). */
  static void Length(Integer *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute LOWER(str). */
  static void Lower(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute LPAD(str, len, pad). */
  static void Lpad(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &len,
                   const StringVal &pad);

  /** Compute LTRIM(str, chars). */
  static void Ltrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &chars);

  /** Compute LTRIM(str). */
  static void Ltrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute REPEAT(str, n). */
  static void Repeat(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n);

  /** Compute REVERSE(str). */
  static void Reverse(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute RIGHT(str, n). */
  static void Right(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n);

  /** Compute RPAD(str, n). */
  static void Rpad(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n,
                   const StringVal &pad);

  /** Compute RTRIM(str, chars). */
  static void Rtrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &chars);

  /** Compute RTRIM(str). */
  static void Rtrim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute SPLITPART(str, delim, field). */
  static void SplitPart(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &delim,
                        const Integer &field);

  /** Compute SUBSTRING(str, pos, len). */
  static void Substring(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &pos,
                        const Integer &len);

  /** Compute SUBSTRING(str, pos). */
  static void Substring(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &pos) {
    Substring(result, ctx, str, pos, Integer(std::numeric_limits<int64_t>::max()));
  }

  /** Compute TRIM(str, chars). */
  static void Trim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const StringVal &chars);

  /** Compute TRIM(str). */
  static void Trim(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute UPPER(str). */
  static void Upper(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str);

  /** Compute LIKE(string, pattern). */
  static void Like(BoolVal *result, exec::ExecutionContext *ctx, const StringVal &string, const StringVal &pattern);
};

}  // namespace terrier::execution::sql
