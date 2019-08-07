#include "execution/sql/functions/string_functions.h"

#include <algorithm>

#include "execution/exec/execution_context.h"
#include "execution/util/bit_util.h"

namespace terrier::execution::sql {

void StringFunctions::Substring(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                                const Integer &pos, const Integer &len) {
  if (str.is_null || pos.is_null || len.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto start = std::max(pos.val, 1l);
  const auto end = pos.val + std::min(static_cast<i64>(str.len), len.val);

  // The end can be before the start only if the length was negative. This is an
  // error.
  if (end < pos.val) {
    *result = StringVal::Null();
    return;
  }

  // If start is negative, return empty string
  if (end < 1) {
    *result = StringVal("");
    return;
  }

  // All good
  *result = StringVal(str.Content() + start - 1, u32(end - start));
}

namespace {

const char *SearchSubstring(const char *haystack, const std::size_t hay_len, const char *needle,
                            const std::size_t needle_len) {
  TPL_ASSERT(needle != nullptr, "No search string provided");
  TPL_ASSERT(needle_len > 0, "No search string provided");
  for (u32 i = 0; i < hay_len + needle_len; i++) {
    const auto pos = haystack + i;
    if (strncmp(pos, needle, needle_len) == 0) {
      return pos;
    }
  }
  return nullptr;
}

}  // namespace

void StringFunctions::SplitPart(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                                const StringVal &delim, const Integer &field) {
  if (str.is_null || delim.is_null || field.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (field.val < 0) {
    // ERROR
    *result = StringVal::Null();
    return;
  }

  if (delim.len == 0) {
    *result = str;
    return;
  }

  // Pointers to the start of the current part, the end of the input string, and
  // the delimiter string
  auto curr = str.Content();
  auto const end = curr + str.len;
  auto const delimiter = delim.Content();

  for (u32 index = 1;; index++) {
    const auto remaining_len = end - curr;
    const auto next_delim = SearchSubstring(curr, remaining_len, delimiter, delim.len);
    if (next_delim == nullptr) {
      if (index == field.val) {
        *result = StringVal(curr, u32(remaining_len));
      } else {
        *result = StringVal("");
      }
      return;
    }
    // Are we at the correct field?
    if (index == field.val) {
      *result = StringVal(curr, u32(next_delim - curr));
      return;
    }
    // We haven't reached the field yet, move along
    curr = next_delim + delim.len;
  }
}

void StringFunctions::Repeat(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.len == 0 || n.val <= 0) {
    *result = StringVal("");
    return;
  }

  char *ptr = StringVal::PreAllocate(result, ctx->GetStringAllocator(), static_cast<u32>(str.len * n.val));
  if (TPL_UNLIKELY(ptr == nullptr)) {
    // Allocation failed
    return;
  }

  // Repeat
  auto *src = str.Content();
  for (u32 i = 0; i < n.val; i++) {
    std::memcpy(ptr, src, str.len);
    ptr += str.len;
  }
}

void StringFunctions::Lpad(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &len,
                           const StringVal &pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (len.val == str.len) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (len.val < str.len) {
    *result = StringVal(str.Content(), u32(len.val));
    return;
  }

  char *ptr = StringVal::PreAllocate(result, ctx->GetStringAllocator(), static_cast<u32>(len.val));
  if (TPL_UNLIKELY(ptr == nullptr)) {
    // Allocation failed
    return;
  }

  auto *pad_src = pad.Content();
  for (auto bytes_left = u32(len.val - str.len); bytes_left > 0;) {
    auto copy_len = std::min(pad.len, bytes_left);
    std::memcpy(ptr, pad_src, copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }

  std::memcpy(ptr, str.Content(), str.len);
}

void StringFunctions::Rpad(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &len,
                           const StringVal &pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (len.val == str.len) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (len.val < str.len) {
    *result = StringVal(str.Content(), u32(len.val));
    return;
  }

  char *ptr = StringVal::PreAllocate(result, ctx->GetStringAllocator(), static_cast<u32>(len.val));
  if (TPL_UNLIKELY(ptr == nullptr)) {
    // Allocation failed
    return;
  }

  // Copy input string first
  std::memcpy(ptr, str.Content(), str.len);
  ptr += str.len;

  // Then padding
  auto *pad_src = pad.Content();
  for (auto bytes_left = u32(len.val - str.len); bytes_left > 0;) {
    auto copy_len = std::min(pad.len, bytes_left);
    std::memcpy(ptr, pad_src, copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }
}

void StringFunctions::Length(UNUSED exec::ExecutionContext *ctx, Integer *result, const StringVal &str) {
  result->is_null = str.is_null;
  result->val = str.len;
}

void StringFunctions::Lower(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  char *ptr = StringVal::PreAllocate(result, ctx->GetStringAllocator(), str.len);
  if (TPL_UNLIKELY(ptr == nullptr)) {
    // Allocation failed
    return;
  }

  auto *src = str.Content();
  for (u32 i = 0; i < str.len; i++) {
    ptr[i] = static_cast<char>(std::tolower(src[i]));
  }
}

void StringFunctions::Upper(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  char *ptr = StringVal::PreAllocate(result, ctx->GetStringAllocator(), str.len);
  if (TPL_UNLIKELY(ptr == nullptr)) {
    // Allocation failed
    return;
  }

  auto *src = str.Content();
  for (u32 i = 0; i < str.len; i++) {
    ptr[i] = static_cast<char>(std::toupper(src[i]));
  }
}

void StringFunctions::Reverse(exec::ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.len == 0) {
    *result = str;
    return;
  }

  char *ptr = StringVal::PreAllocate(result, ctx->GetStringAllocator(), str.len);
  if (TPL_UNLIKELY(ptr == nullptr)) {
    // Allocation failed
    return;
  }

  auto *src = str.Content();
  std::reverse_copy(src, src + str.len, ptr);
}

namespace {

// TODO(pmenon): The bitset we use can be prepared once before all function
//               invocations if the characters list is a constant value, and not
//               a column value (i.e., SELECT ltrim(col, 'abc') FROM ...). This
//               should be populated in the execution context once apriori
//               rather that initializing it each invocation.
// TODO(pmenon): What about non-ASCII strings?
// Templatized from Postgres
template <bool TrimLeft, bool TrimRight>
void DoTrim(StringVal *result, const StringVal &str, const StringVal &chars) {
  if (str.is_null || chars.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.len == 0) {
    *result = str;
    return;
  }

  util::InlinedBitVector<256> bitset;
  // Store this variable to avoid reexecuting if statements.
  auto *chars_content = chars.Content();
  for (u32 i = 0; i < chars.len; i++) {
    bitset.Set(u32(chars_content[i]));
  }

  // The valid range
  i32 begin = 0, end = str.len - 1;

  auto *src = str.Content();
  if constexpr (TrimLeft) {
    while (begin < static_cast<i32>(str.len) && bitset.Test(u32(src[begin]))) {
      begin++;
    }
  }

  if constexpr (TrimRight) {
    while (begin <= end && bitset.Test(u32(src[end]))) {
      end--;
    }
  }

  *result = StringVal(src + begin, u32(end - begin + 1));
}

}  // namespace

void StringFunctions::Trim(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  DoTrim<true, true>(result, str, StringVal(" "));
}

void StringFunctions::Trim(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                           const StringVal &chars) {
  DoTrim<true, true>(result, str, chars);
}

void StringFunctions::Ltrim(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  DoTrim<true, false>(result, str, StringVal(" "));
}

void StringFunctions::Ltrim(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                            const StringVal &chars) {
  DoTrim<true, false>(result, str, chars);
}

void StringFunctions::Rtrim(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  DoTrim<false, true>(result, str, StringVal(" "));
}

void StringFunctions::Rtrim(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                            const StringVal &chars) {
  DoTrim<false, true>(result, str, chars);
}

void StringFunctions::Left(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                           const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto len = n.val < 0 ? std::max(0l, str.len + n.val) : std::min(str.len, static_cast<u32>(n.val));
  *result = StringVal(str.Content(), u32(len));
}

void StringFunctions::Right(UNUSED exec::ExecutionContext *ctx, StringVal *result, const StringVal &str,
                            const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto len = std::min(str.len, static_cast<u32>(std::abs(n.val)));
  if (n.val > 0) {
    *result = StringVal(str.Content() + (str.len - len), len);
  } else {
    *result = StringVal(str.Content() + len, str.len - len);
  }
}

}  // namespace terrier::execution::sql
