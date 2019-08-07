#pragma once

#include <cstdint>
#include <string>

#include "execution/parsing/token.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace terrier::execution::parsing {

/**
 * Text Scanner
 */
class Scanner {
  static constexpr i32 kEndOfInput = -1;

 public:
  /**
   * Constructor
   * @param source source code
   * @param source_len length of the source code
   */
  Scanner(const char *source, u64 source_len);

  /**
   * Constructor
   * @param source code
   */
  explicit Scanner(const std::string &source);

  /**
   * Prevent copy and move
   */
  DISALLOW_COPY_AND_MOVE(Scanner);

  /**
   * Returns the current token type and advances the scanner
   * @return the current token type
   */
  Token::Type Next();

  /**
   * @return the next token type.
   */
  Token::Type peek() const { return next_.type; }

  /**
   * @return the current token type
   */
  Token::Type current_token() const { return curr_.type; }

  /**
   * @return the current string literal
   */
  const std::string &current_literal() const { return curr_.literal; }

  /**
   * @return the current source position
   */
  const SourcePosition &current_position() const { return curr_.pos; }

 private:
  // Scan the next token
  void Scan();

  // Advance a single character into the source input stream
  void Advance() {
    bool at_end = (offset_ == source_len_);
    if (TPL_UNLIKELY(at_end)) {
      c0_ = kEndOfInput;
      return;
    }

    // Not at end, bump
    c0_ = source_[offset_++];
    c0_pos_.column++;
  }

  // Does the current character match the expected? If so, advance the scanner
  bool Matches(i32 expected) {
    if (c0_ != expected) {
      return false;
    }

    Advance();

    return true;
  }

  // Skip whitespace from the current token to the next valid token
  void SkipWhiteSpace();

  // Skip a line comment
  void SkipLineComment();

  // Skip a block comment
  void SkipBlockComment();

  // Scan an identifier token
  Token::Type ScanIdentifierOrKeyword();

  // Check if the given input is a keyword or an identifier
  Token::Type CheckIdentifierOrKeyword(const char *input, u32 input_len);

  // Scan a number literal
  Token::Type ScanNumber();

  // Scan a string literal
  Token::Type ScanString();

  /*
   * This struct describes information about a single token, including its type,
   * its position in the origin source, and its literal value if it should have
   * one.
   */
  struct TokenDesc {
    Token::Type type;
    SourcePosition pos;
    u64 offset;
    std::string literal;
  };

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Static utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  // Is the current character a character?
  static bool IsInRange(i32 c, i32 lower, i32 upper) { return (c >= lower && c <= upper); }

  // Is the provided character an alphabetic character
  static bool IsAlpha(i32 c) { return IsInRange(c, 'a', 'z') || IsInRange(c, 'A', 'Z'); }

  // Is the current character a digit?
  static bool IsDigit(i32 c) { return IsInRange(c, '0', '9'); }

  // Is this character allowed in an identifier?
  static bool IsIdentifierChar(i32 c) { return IsAlpha(c) || IsDigit(c) || c == '_'; }

 private:
  // The source input and its length
  const char *source_;
  u64 source_len_;

  // The offset/position in the source where the next character is read from
  u64 offset_;

  // The lookahead character and its position in the source
  i32 c0_;
  SourcePosition c0_pos_;

  // Information about the current and next token in the input stream
  TokenDesc curr_;
  TokenDesc next_;
};

}  // namespace terrier::execution::parsing
