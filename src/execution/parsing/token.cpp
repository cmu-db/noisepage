#include "execution/parsing/token.h"

namespace terrier::execution::parsing {

#define T(name, str, precedence) #name,
const char *Token::k_token_names[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) str,
const char *Token::k_token_strings[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) precedence,
const uint32_t Token::K_TOKEN_PRECEDENCE[] = {TOKENS(T, T)};
#undef T

}  // namespace terrier::execution::parsing
