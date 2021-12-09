#include "parser/udf/string_utils.h"

#include <algorithm>

namespace noisepage::parser::udf {

std::string StringUtils::Lower(const std::string &string) {
  std::string result{};
  std::transform(string.cbegin(), string.cend(), std::back_inserter(result),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

std::string StringUtils::Strip(const std::string &string) {
  auto not_whitespace = [](unsigned char c) { return std::isspace(c) == 0; };

  // Find the first non-whitespace character
  auto begin = std::find_if(string.cbegin(), string.cend(), not_whitespace);
  if (begin == string.cend()) {
    return std::string{};
  }

  // Find the last non whitespace character
  auto end = std::find_if(string.rbegin(), string.rend(), not_whitespace);

  // Construct the result
  std::string result{};
  std::copy(begin, end.base(), std::back_inserter(result));
  return result;
}

}  // namespace noisepage::parser::udf
