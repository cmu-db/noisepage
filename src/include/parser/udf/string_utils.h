#pragma once

#include <string>

namespace noisepage::parser::udf {

/**
 * StringUtils is a static class that implements some basic
 * string-processing utilities. Eventually, we might want to
 * move functionality like this to our own internal algo library.
 */
class StringUtils {
 public:
  /**
   * Convert a non-owned string to lowercase.
   * @param string The input string
   * @return The lowercased string
   */
  static std::string Lower(const std::string &string);

  /**
   * Strip whitespace from the start and end of a non-owned string.
   * @param string The input string
   * @return The stripped string
   */
  static std::string Strip(const std::string &string);
};

}  // namespace noisepage::parser::udf
