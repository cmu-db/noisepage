#include "execution/sql/functions/string_functions.h"

#include <algorithm>
#include <bitset>
#include <string>

#include "execution/exec/execution_context.h"
#include "execution/sql/operators/like_operators.h"

namespace terrier::execution::sql {

void StringFunctions::Concat(StringVal *result, exec::ExecutionContext *ctx, const StringVal &left,
                             const StringVal &right) {
  if (left.is_null_ || right.is_null_) {
    *result = StringVal::Null();
    return;
  }

  const std::size_t length = left.GetLength() + right.GetLength();
  char *const ptr = ctx->GetStringAllocator()->PreAllocate(length);

  // Copy contents into result.
  std::memcpy(ptr, left.GetContent(), left.GetLength());
  std::memcpy(ptr + left.GetLength(), right.GetContent(), right.GetLength());
  *result = StringVal(ptr, length);
}

void StringFunctions::Substring(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                                const Integer &pos, const Integer &len) {
  if (str.is_null_ || pos.is_null_ || len.is_null_) {
    *result = StringVal::Null();
    return;
  }

  const auto start = std::max(pos.val_, int64_t{1});
  const auto end = pos.val_ + std::min(static_cast<int64_t>(str.GetLength()), len.val_);

  // The end can be before the start only if the length was negative. This is an
  // error.
  if (end < pos.val_) {
    *result = StringVal::Null();
    return;
  }

  // If start is negative, return empty string
  if (end < 1) {
    *result = StringVal("");
    return;
  }

  // All good
  *result = StringVal(str.GetContent() + start - 1, end - start);
}

namespace {

const char *SearchSubstring(const char *haystack, const std::size_t hay_len, const char *needle,
                            const std::size_t needle_len) {
  TERRIER_ASSERT(needle != nullptr, "No search string provided");
  TERRIER_ASSERT(needle_len > 0, "No search string provided");
  for (uint32_t i = 0; i < hay_len + needle_len; i++) {
    const auto pos = haystack + i;
    if (strncmp(pos, needle, needle_len) == 0) {
      return pos;
    }
  }
  return nullptr;
}

}  // namespace

void StringFunctions::SplitPart(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                                const StringVal &delim, const Integer &field) {
  if (str.is_null_ || delim.is_null_ || field.is_null_) {
    *result = StringVal::Null();
    return;
  }

  if (field.val_ < 0) {
    // ERROR
    *result = StringVal::Null();
    return;
  }

  if (delim.GetLength() == 0) {
    *result = str;
    return;
  }

  // Pointers to the start of the current part, the end of the input string, and
  // the delimiter string
  auto curr = str.GetContent();
  auto const end = curr + str.GetLength();
  auto const delimiter = delim.GetContent();

  for (uint32_t index = 1;; index++) {
    const auto remaining_len = end - curr;
    const auto next_delim = SearchSubstring(curr, remaining_len, delimiter, delim.GetLength());
    if (next_delim == nullptr) {
      if (index == field.val_) {
        *result = StringVal(curr, remaining_len);
      } else {
        *result = StringVal("");
      }
      return;
    }
    // Are we at the correct field?
    if (index == field.val_) {
      *result = StringVal(curr, next_delim - curr);
      return;
    }
    // We haven't reached the field yet, move along
    curr = next_delim + delim.GetLength();
  }
}

void StringFunctions::Repeat(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &n) {
  if (str.is_null_ || n.is_null_) {
    *result = StringVal::Null();
    return;
  }

  if (str.GetLength() == 0 || n.val_ <= 0) {
    *result = StringVal("");
    return;
  }

  // Allocate
  const std::size_t result_len = str.GetLength() * n.val_;
  char *target = ctx->GetStringAllocator()->PreAllocate(result_len);

  // Repeat
  char *ptr = target;
  for (uint32_t i = 0; i < n.val_; i++) {
    std::memcpy(ptr, str.GetContent(), str.GetLength());
    ptr += str.GetLength();
  }

  // Set result
  *result = StringVal(target, result_len);
}

void StringFunctions::Lpad(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &len,
                           const StringVal &pad) {
  if (str.is_null_ || len.is_null_ || pad.is_null_ || len.val_ < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (static_cast<std::size_t>(len.val_) == str.GetLength()) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (static_cast<std::size_t>(len.val_) < str.GetLength()) {
    *result = StringVal(str.GetContent(), len.val_);
    return;
  }

  // Allocate some memory
  char *target = ctx->GetStringAllocator()->PreAllocate(len.val_);

  // Pad
  char *ptr = target;
  for (std::size_t bytes_left = len.val_ - str.GetLength(); bytes_left > 0;) {
    auto copy_len = std::min(pad.GetLength(), bytes_left);
    std::memcpy(ptr, pad.GetContent(), copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }

  // Copy main string into target
  std::memcpy(ptr, str.GetContent(), str.GetLength());

  // Set result
  *result = StringVal(target, len.val_);
}

void StringFunctions::Rpad(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str, const Integer &len,
                           const StringVal &pad) {
  if (str.is_null_ || len.is_null_ || pad.is_null_ || len.val_ < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (static_cast<std::size_t>(len.val_) == str.GetLength()) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (static_cast<std::size_t>(len.val_) < str.GetLength()) {
    *result = StringVal(str.GetContent(), len.val_);
    return;
  }

  // Allocate output
  char *target = ctx->GetStringAllocator()->PreAllocate(len.val_);
  char *ptr = target;

  // Copy input string first
  std::memcpy(ptr, str.GetContent(), str.GetLength());
  ptr += str.GetLength();

  // Then padding
  for (std::size_t bytes_left = len.val_ - str.GetLength(); bytes_left > 0;) {
    auto copy_len = std::min(pad.GetLength(), bytes_left);
    std::memcpy(ptr, pad.GetContent(), copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }

  // Set result
  *result = StringVal(target, len.val_);
}

void StringFunctions::Length(Integer *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str) {
  result->is_null_ = str.is_null_;
  result->val_ = str.GetLength();
}

void StringFunctions::Lower(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str) {
  if (str.is_null_) {
    *result = StringVal::Null();
    return;
  }

  char *target = ctx->GetStringAllocator()->PreAllocate(str.GetLength());
  std::transform(str.GetContent(), str.GetContent() + str.GetLength(), target, ::tolower);
  *result = StringVal(target, str.GetLength());
}

void StringFunctions::Upper(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str) {
  if (str.is_null_) {
    *result = StringVal::Null();
    return;
  }

  char *target = ctx->GetStringAllocator()->PreAllocate(str.GetLength());
  std::transform(str.GetContent(), str.GetContent() + str.GetLength(), target, ::toupper);
  *result = StringVal(target, str.GetLength());
}

void StringFunctions::Reverse(StringVal *result, exec::ExecutionContext *ctx, const StringVal &str) {
  if (str.is_null_) {
    *result = StringVal::Null();
    return;
  }

  if (str.GetLength() == 0) {
    *result = str;
    return;
  }

  char *target = ctx->GetStringAllocator()->PreAllocate(str.GetLength());
  std::reverse_copy(str.GetContent(), str.GetContent() + str.GetLength(), target);
  *result = StringVal(target, str.GetLength());
}

namespace {

// TODO(pmenon): The bitset we use can be prepared once before all function invocations if the
//               characters list is a constant value, and not a column value
//               (i.e., SELECT ltrim(col, 'abc') FROM ...). This should be populated in the
//               execution context once apriori rather that initializing it each invocation.
// TODO(pmenon): What about non-ASCII strings?
// Templatized from Postgres
template <bool TrimLeft, bool TrimRight>
void DoTrim(StringVal *result, const StringVal &str, const StringVal &chars) {
  if (str.is_null_ || chars.is_null_) {
    *result = StringVal::Null();
    return;
  }

  if (str.GetLength() == 0) {
    *result = str;
    return;
  }

  std::bitset<256> bitset;
  for (uint32_t i = 0; i < chars.GetLength(); i++) {
    bitset.set(chars.GetContent()[i]);
  }

  // The valid range
  int32_t begin = 0, end = str.GetLength() - 1;

  if constexpr (TrimLeft) {  // NOLINT
    while (begin < static_cast<int32_t>(str.GetLength()) && bitset.test(str.GetContent()[begin])) {
      begin++;
    }
  }

  if constexpr (TrimRight) {  // NOLINT
    while (begin <= end && bitset.test(str.GetContent()[end])) {
      end--;
    }
  }

  *result = StringVal(str.GetContent() + begin, end - begin + 1);
}

}  // namespace

void StringFunctions::Trim(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str) {
  DoTrim<true, true>(result, str, StringVal(" "));
}

void StringFunctions::Trim(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                           const StringVal &chars) {
  DoTrim<true, true>(result, str, chars);
}

void StringFunctions::Ltrim(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str) {
  DoTrim<true, false>(result, str, StringVal(" "));
}

void StringFunctions::Ltrim(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                            const StringVal &chars) {
  DoTrim<true, false>(result, str, chars);
}

void StringFunctions::Rtrim(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str) {
  DoTrim<false, true>(result, str, StringVal(" "));
}

void StringFunctions::Rtrim(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                            const StringVal &chars) {
  DoTrim<false, true>(result, str, chars);
}

void StringFunctions::Left(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                           const Integer &n) {
  if (str.is_null_ || n.is_null_) {
    *result = StringVal::Null();
    return;
  }

  const auto len = n.val_ < 0 ? std::max(int64_t(0), static_cast<int64_t>(str.GetLength()) + n.val_)
                              : std::min(str.GetLength(), static_cast<std::size_t>(n.val_));
  *result = StringVal(str.GetContent(), len);
}

void StringFunctions::Right(StringVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &str,
                            const Integer &n) {
  if (str.is_null_ || n.is_null_) {
    *result = StringVal::Null();
    return;
  }

  const auto len = std::min(str.GetLength(), static_cast<std::size_t>(std::abs(n.val_)));
  if (n.val_ > 0) {
    *result = StringVal(str.GetContent() + (str.GetLength() - len), len);
  } else {
    *result = StringVal(str.GetContent() + len, str.GetLength() - len);
  }
}

void StringFunctions::Like(BoolVal *result, UNUSED_ATTRIBUTE exec::ExecutionContext *ctx, const StringVal &string,
                           const StringVal &pattern) {
  if (string.is_null_ || pattern.is_null_) {
    *result = BoolVal::Null();
    return;
  }

  result->is_null_ = false;
  result->val_ = sql::Like{}(string.val_, pattern.val_);  // NOLINT
}

void StringFunctions::Position(exec::ExecutionContext *ctx, Integer *pos, const StringVal &search_str,
                               const StringVal &search_sub_str) {
  if (search_str.is_null_ || search_sub_str.is_null_) {
    *pos = Integer::Null();
    return;
  }

  auto search_str_view = search_str.StringView();
  auto search_sub_str_view = search_sub_str.StringView();

  // Postgres performs a case insensitive search for Position()
  auto it =
      std::search(search_str_view.begin(), search_str_view.end(), search_sub_str_view.begin(),
                  search_sub_str_view.end(), [](char ch1, char ch2) { return std::toupper(ch1) == std::toupper(ch2); });
  auto found = (it - search_str_view.begin());

  if (static_cast<size_t>(found) == search_str_view.length()) {
    *pos = Integer(0);
  } else {
    *pos = Integer(found + 1);
  }
}

void StringFunctions::ASCII(exec::ExecutionContext *ctx, Integer *result, const StringVal &str) {
  if (str.is_null_) {
    *result = Integer::Null();
    return;
  }

  if (str.GetLength() == 0) {
    *result = Integer(0);
    return;
  }

  auto str_view = str.StringView();
  *result = Integer(static_cast<int>(str_view.front()));
}

void StringFunctions::Chr(exec::ExecutionContext *ctx, StringVal *result, const Integer &code) {
  if (code >= 0 && code <= 127) {
    *result = StringVal(&static_cast<char>(code.val_), 1);
  } else {
    *result = StringVal::Null();
  }
}

}  // namespace terrier::execution::sql
