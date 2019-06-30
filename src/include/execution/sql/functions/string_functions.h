#pragma once

#include <limits>

#include "execution/sql/value.h"

namespace tpl::exec {
class ExecutionContext;
}

namespace tpl::sql {

/**
 * Utility class to handle SQL string manipulations.
 */
class StringFunctions {
 public:
  /**
   * Delete to force only static functions
   */
  StringFunctions() = delete;

  /**
   * Return the string length
   */
  static void CharLength(exec::ExecutionContext *ctx, Integer *result, const StringVal &str) {
    Length(ctx, result, str);
  }

  /**
   * First min(length, n) characters
   */
  static void Left(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &n);

  /**
   * Return the length of the string
   */
  static void Length(exec::ExecutionContext *ctx, Integer *result, const StringVal &str);

  /**
   * Set the string to lower case
   */
  static void Lower(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str);

  /**
   * Pads the left side of the string with min(pad_length, len) characters
   */
  static void Lpad(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &len,
                   const StringVal &pad);

  /**
   * Perform left trim of the given chars
   */
  static void Ltrim(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const StringVal &chars);

  /**
   * Perform left trim of blank chars
   */
  static void Ltrim(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str);

  /**
   * Repeat a string n times
   */
  static void Repeat(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &n);

  /**
   * Reverse a string
   */
  static void Reverse(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str);

  /**
   * Last min(length, n) characters of the string
   */
  static void Right(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &n);

  /**
   * Pads the right side of the string with min(pad_length, len) characters
   */
  static void Rpad(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &n,
                   const StringVal &pad);

  /**
   * Perform right trim of the given chars
   */
  static void Rtrim(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const StringVal &chars);

  /**
   * Perform right trim of blank chars
   */
  static void Rtrim(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str);

  /**
   * Split the string and return the split at the given index
   */
  static void SplitPart(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const StringVal &delim,
                        const Integer &field);

  /**
   * Return the substring starting at pos of length len
   */
  static void Substring(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &pos,
                        const Integer &len);

  /**
   * Return the suffix starting at pos
   */
  static void Substring(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &pos) {
    Substring(ctx, result, str, pos, Integer(std::numeric_limits<i64>::max()));
  }

  /**
   * Perform a trim of the given characters on both sides
   */
  static void Trim(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const StringVal &chars);

  /**
   * Perform a trim of blank spaces on both sides
   */
  static void Trim(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str);

  /**
   * Converts the string to upper case
   */
  static void Upper(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str);
};

}  // namespace tpl::sql
