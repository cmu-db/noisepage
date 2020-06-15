#include "execution/parsing/token.h"

namespace terrier::execution::parsing {

#define T(name, str, precedence) #name,
const char *Token::TOKEN_NAMES[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) str,
const char *Token::TOKEN_STRINGS[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) precedence,
const uint32_t Token::TOKEN_PRECEDENCES[] = {TOKENS(T, T)};
#undef T

}  // namespace terrier::execution::parsing
