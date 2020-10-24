#include "execution/sql/operators/like_operators.h"

#include "common/macros.h"

namespace noisepage::execution::sql {

#define NextByte(p, plen) ((p)++, (plen)--)

// Inspired by Postgres
bool Like::Impl(const char *str, size_t str_len, const char *pattern, size_t pattern_len, char escape) {
  NOISEPAGE_ASSERT(str != nullptr, "Input string cannot be NULL");
  NOISEPAGE_ASSERT(pattern != nullptr, "Pattern cannot be NULL");

  const char *s = str, *p = pattern;
  std::size_t slen = str_len, plen = pattern_len;

  for (; plen > 0 && slen > 0; NextByte(p, plen)) {
    if (*p == escape) {
      // Next pattern character must match exactly, whatever it is
      NextByte(p, plen);

      if (plen == 0 || *p != *s) {
        return false;
      }

      NextByte(s, slen);
    } else if (*p == '%') {
      // Any sequence of '%' wildcards can essentially be replaced by one '%'. Similarly, any
      // sequence of N '_'s will blindly consume N characters from the input string. Process the
      // pattern until we reach a non-wildcard character.
      NextByte(p, plen);
      while (plen > 0) {
        if (*p == '%') {
          NextByte(p, plen);
        } else if (*p == '_') {
          if (slen == 0) {
            return false;
          }
          NextByte(s, slen);
          NextByte(p, plen);
        } else {
          break;
        }
      }

      // If we've reached the end of the pattern, the tail of the input string is accepted.
      if (plen == 0) {
        return true;
      }

      if (*p == escape) {
        NextByte(p, plen);
        NOISEPAGE_ASSERT(*p != 0, "LIKE pattern must not end with an escape character");
        if (plen == 0) {
          return false;
        }
      }

      while (slen > 0) {
        if (Like::Impl(s, slen, p, plen, escape)) {
          return true;
        }
        NextByte(s, slen);
      }
      // No match
      return false;
    } else if (*p == '_') {
      // '_' wildcard matches a single character in the input
      NextByte(s, slen);
    } else if (*p == *s) {
      // Exact character match
      NextByte(s, slen);
    } else {
      // Unmatched!
      return false;
    }
  }
  while (plen > 0 && *p == '%') {
    NextByte(p, plen);
  }
  return slen == 0 && plen == 0;
}

}  // namespace noisepage::execution::sql
