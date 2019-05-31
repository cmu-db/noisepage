#include "execution/parsing/scanner.h"

#include <cassert>
#include <stdexcept>
#include <string>

namespace tpl::parsing {

Scanner::Scanner(const std::string &source) : Scanner(source.data(), source.length()) {}

Scanner::Scanner(const char *source, u64 source_len) : source_(source), source_len_(source_len), offset_(0) {
  // Setup current token information
  curr_.type = Token::Type::UNINIITIALIZED;
  curr_.offset = 0;
  curr_.pos.line = 0;
  curr_.pos.column = 0;

  next_.type = Token::Type::UNINIITIALIZED;
  next_.offset = 0;
  next_.pos.line = 0;
  next_.pos.column = 0;

  // Advance character iterator to the first slot
  c0_pos_.line = 1;
  c0_pos_.column = 0;
  Advance();

  // Find the first token
  Scan();
}

Token::Type Scanner::Next() {
  curr_ = next_;
  Scan();
  return curr_.type;
}

void Scanner::Scan() {
  // Re-init the next token
  next_.literal.clear();

  // The token
  Token::Type type;

  do {
    // Setup current token positions
    next_.pos = c0_pos_;
    next_.offset = offset_;

    switch (c0_) {
      case '@': {
        Advance();
        if (IsIdentifierChar(c0_)) {
          ScanIdentifierOrKeyword();
          type = Token::Type::BUILTIN_IDENTIFIER;
        } else {
          type = Token::Type::AT;
        }
        break;
      }
      case '{': {
        Advance();
        type = Token::Type::LEFT_BRACE;
        break;
      }
      case '}': {
        Advance();
        type = Token::Type::RIGHT_BRACE;
        break;
      }
      case '(': {
        Advance();
        type = Token::Type::LEFT_PAREN;
        break;
      }
      case ')': {
        Advance();
        type = Token::Type::RIGHT_PAREN;
        break;
      }
      case '[': {
        Advance();
        type = Token::Type::LEFT_BRACKET;
        break;
      }
      case ']': {
        Advance();
        type = Token::Type::RIGHT_BRACKET;
        break;
      }
      case '&': {
        Advance();
        type = Token::Type::AMPERSAND;
        break;
      }
      case '|': {
        Advance();
        type = Token::Type::BIT_OR;
        break;
      }
      case '^': {
        Advance();
        type = Token::Type::BIT_XOR;
        break;
      }
      case '!': {
        Advance();
        if (Matches('=')) {
          type = Token::Type::BANG_EQUAL;
        } else {
          type = Token::Type::BANG;
        }
        break;
      }
      case '~': {
        Advance();
        type = Token::Type::BIT_NOT;
        break;
      }
      case ':': {
        Advance();
        type = Token::Type::COLON;
        break;
      }
      case ',': {
        Advance();
        type = Token::Type::COMMA;
        break;
      }
      case '.': {
        Advance();
        type = Token::Type::DOT;
        break;
      }
      case ';': {
        Advance();
        type = Token::Type::SEMI;
        break;
      }
      case '=': {
        Advance();
        if (Matches('=')) {
          type = Token::Type::EQUAL_EQUAL;
        } else {
          type = Token::Type::EQUAL;
        }
        break;
      }
      case '>': {
        Advance();
        if (Matches('=')) {
          type = Token::Type::GREATER_EQUAL;
        } else {
          type = Token::Type::GREATER;
        }
        break;
      }
      case '<': {
        Advance();
        if (Matches('=')) {
          type = Token::Type::LESS_EQUAL;
        } else {
          type = Token::Type::LESS;
        }
        break;
      }
      case '-': {
        Advance();
        if (Matches('>')) {
          type = Token::Type::ARROW;
        } else {
          type = Token::Type::MINUS;
        }
        break;
      }
      case '%': {
        Advance();
        type = Token::Type::PERCENT;
        break;
      }
      case '+': {
        Advance();
        type = Token::Type::PLUS;
        break;
      }
      case '/': {
        Advance();
        if (Matches('/')) {
          SkipLineComment();
          type = Token::Type::WHITESPACE;
        } else if (Matches('*')) {
          SkipBlockComment();
          type = Token::Type::WHITESPACE;
        } else {
          type = Token::Type::SLASH;
        }
        break;
      }
      case '*': {
        Advance();
        type = Token::Type::STAR;
        break;
      }
      case '"': {
        Advance();
        type = ScanString();
        break;
      }
      default: {
        if (IsDigit(c0_)) {
          type = ScanNumber();
        } else if (IsIdentifierChar(c0_)) {
          type = ScanIdentifierOrKeyword();
        } else if (c0_ == kEndOfInput) {
          type = Token::Type::EOS;
        } else {
          SkipWhiteSpace();
          type = Token::Type::WHITESPACE;
        }
      }
    }
  } while (type == Token::Type::WHITESPACE);

  next_.type = type;
}

void Scanner::SkipWhiteSpace() {
  while (true) {
    switch (c0_) {
      case ' ':
      case '\r':
      case '\t': {
        Advance();
        break;
      }
      case '\n': {
        c0_pos_.line++;
        c0_pos_.column = 0;
        Advance();
        break;
      }
      default: { return; }
    }
  }
}

void Scanner::SkipLineComment() {
  while (c0_ != '\n' && c0_ != kEndOfInput) {
    Advance();
  }
}

void Scanner::SkipBlockComment() {
  i32 c;
  do {
    c = c0_;
    Advance();
  } while (c != '*' && c0_ != '/' && c0_ != kEndOfInput);

  // Skip the '/' if we found. If we're are the end, Advance() will be a no-op
  // anyways, so we're safe.
  Advance();
}

Token::Type Scanner::ScanIdentifierOrKeyword() {
  // First collect identifier
  int32_t identifier_char0 = c0_;
  while (IsIdentifierChar(c0_) && c0_ != kEndOfInput) {
    next_.literal += static_cast<char>(c0_);
    Advance();
  }

  if (identifier_char0 == '_' || IsInRange(identifier_char0, 'A', 'Z')) {
    // Definitely not keyword
    return Token::Type::IDENTIFIER;
  }

  const auto *identifier = next_.literal.data();
  auto identifier_len = static_cast<u32>(next_.literal.length());

  return CheckIdentifierOrKeyword(identifier, identifier_len);
}

#define KEYWORDS()                          \
  GROUP_START('a')                          \
  GROUP_ELEM("and", Token::Type::AND)       \
  GROUP_START('e')                          \
  GROUP_ELEM("else", Token::Type::ELSE)     \
  GROUP_START('f')                          \
  GROUP_ELEM("false", Token::Type::FALSE)   \
  GROUP_ELEM("for", Token::Type::FOR)       \
  GROUP_ELEM("fun", Token::Type::FUN)       \
  GROUP_START('i')                          \
  GROUP_ELEM("if", Token::Type::IF)         \
  GROUP_ELEM("in", Token::Type::IN)         \
  GROUP_START('m')                          \
  GROUP_ELEM("map", Token::Type::MAP)       \
  GROUP_START('n')                          \
  GROUP_ELEM("nil", Token::Type::NIL)       \
  GROUP_START('o')                          \
  GROUP_ELEM("or", Token::Type::OR)         \
  GROUP_START('r')                          \
  GROUP_ELEM("return", Token::Type::RETURN) \
  GROUP_START('s')                          \
  GROUP_ELEM("struct", Token::Type::STRUCT) \
  GROUP_START('t')                          \
  GROUP_ELEM("true", Token::Type::TRUE)     \
  GROUP_START('v')                          \
  GROUP_ELEM("var", Token::Type::VAR)

Token::Type Scanner::CheckIdentifierOrKeyword(const char *input, u32 input_len) {
  static constexpr u32 kMinKeywordLen = 2;
  static constexpr u32 kMaxKeywordLen = 6;

  if (input_len < kMinKeywordLen || input_len > kMaxKeywordLen) {
    return Token::Type::IDENTIFIER;
  }

#define GROUP_START(c) \
  break;               \
  case c:

#define GROUP_ELEM(str, typ)                                                                             \
  {                                                                                                      \
    const u64 keyword_len = sizeof(str) - 1;                                                             \
    if (keyword_len == input_len && (str)[1] == input[1] && (keyword_len < 3 || (str)[2] == input[2]) && \
        (keyword_len < 4 || (str)[3] == input[3]) && (keyword_len < 5 || (str)[4] == input[4]) &&        \
        (keyword_len < 6 || (str)[5] == input[5])) {                                                     \
      return typ;                                                                                        \
    }                                                                                                    \
  }

  // The main switch statement that outlines all keywords
  switch (input[0]) {
    default:
      KEYWORDS()
  }

  // The input isn't a keyword, it must be an identifier
  return Token::Type::IDENTIFIER;
}

// hygiene
#undef GROUP_ELEM
#undef GROUP_START
#undef KEYWORDS

Token::Type Scanner::ScanNumber() {
  Token::Type type = Token::Type::INTEGER;

  while (IsDigit(c0_)) {
    next_.literal += static_cast<char>(c0_);
    Advance();
  }

  if (c0_ == '.') {
    type = Token::Type::FLOAT;

    next_.literal.append(".");

    Advance();

    while (IsDigit(c0_)) {
      next_.literal += static_cast<char>(c0_);
      Advance();
    }
  }

  return type;
}

Token::Type Scanner::ScanString() {
  // Single-line string. The lookahead character points to the start of the
  // string literal
  while (true) {
    if (c0_ == kEndOfInput) {
      next_.literal.clear();
      next_.literal = "Unterminated string";
      return Token::Type::ERROR;
    }

    // Is this character an escape?
    bool escape = (c0_ == '\\');

    // Add the character to the current string literal
    next_.literal += static_cast<char>(c0_);

    Advance();

    // If we see an enclosing quote and it hasn't been escaped, we're done
    if (c0_ == '"' && !escape) {
      Advance();
      return Token::Type::STRING;
    }
  }
}

}  // namespace tpl::parsing
