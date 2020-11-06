#include "execution/parsing/token.h"

namespace noisepage::execution::parsing {

#define T(name, str, precedence) #name,
const char *Token::token_names[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) str,
const char *Token::token_strings[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) precedence,
const uint32_t Token::TOKEN_PRECEDENCES[] = {TOKENS(T, T)};
#undef T

}  // namespace noisepage::execution::parsing
