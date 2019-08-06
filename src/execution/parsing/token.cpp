#include "execution/parsing/token.h"

namespace terrier::parsing {

#define T(name, str, precedence) #name,
const char *Token::kTokenNames[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) str,
const char *Token::kTokenStrings[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) precedence,
const u32 Token::kTokenPrecedence[] = {TOKENS(T, T)};
#undef T

}  // namespace terrier::parsing
