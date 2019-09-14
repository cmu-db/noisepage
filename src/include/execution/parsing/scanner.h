#pragma once

#include <cstdint>
#include <string>

#include "common/macros.h"
#include "execution/parsing/token.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::parsing {

/**
 * Text Scanner
 */
class Scanner {
  static constexpr int32_t K_END_OF_INPUT = -1;

 public:
  /**
   * Constructor
   * @param source source code
   * @param source_len length of the source code
   */
  Scanner(const char *source, uint64_t source_len);

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
  Token::Type Peek() const { return next_.type_; }

  /**
   * @return the current token type
   */
  Token::Type CurrentToken() const { return curr_.type_; }

  /**
   * @return the current string literal
   */
  const std::string &CurrentLiteral() const { return curr_.literal_; }

  /**
   * @return the current source position
   */
  const SourcePosition &CurrentPosition() const { return curr_.pos_; }

 private:
  // Scan the next token
  void Scan();

  // Advance a single character into the source input stream
  void Advance() {
    bool at_end = (offset_ == source_len_);
    if (UNLIKELY(at_end)) {
      c0_ = K_END_OF_INPUT;
      return;
    }

    // Not at end, bump
    c0_ = source_[offset_++];
    c0_pos_.column_++;
  }

  // Does the current character match the expected? If so, advance the scanner
  bool Matches(int32_t expected) {
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
  Token::Type CheckIdentifierOrKeyword(const char *input, uint32_t input_len);

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
    Token::Type type_;
    SourcePosition pos_;
    uint64_t offset_;
    std::string literal_;
  };

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Static utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  // Is the current character a character?
  static bool IsInRange(int32_t c, int32_t lower, int32_t upper) { return (c >= lower && c <= upper); }

  // Is the provided character an alphabetic character
  static bool IsAlpha(int32_t c) { return IsInRange(c, 'a', 'z') || IsInRange(c, 'A', 'Z'); }

  // Is the current character a digit?
  static bool IsDigit(int32_t c) { return IsInRange(c, '0', '9'); }

  // Is this character allowed in an identifier?
  static bool IsIdentifierChar(int32_t c) { return IsAlpha(c) || IsDigit(c) || c == '_'; }

 private:
  // The source input and its length
  const char *source_;
  uint64_t source_len_;

  // The offset/position in the source where the next character is read from
  uint64_t offset_;

  // The lookahead character and its position in the source
  int32_t c0_;
  SourcePosition c0_pos_;

  // Information about the current and next token in the input stream
  TokenDesc curr_;
  TokenDesc next_;
};

}  // namespace terrier::execution::parsing
