#pragma once

#include <string>
#include <vector>

namespace terrier::util {

/**
 * String Utility Functions
 * Note that these are not the most efficient implementations (i.e., they copy
 * memory) and therefore they should only be used for debug messages and other
 * such things.
 */
class StringUtil {
 public:
  /**
   * Returns true if the needle string exists in the haystack
   * @param haystack
   * @param needle
   * @return
   */
  static bool Contains(const std::string &haystack, const std::string &needle);

  /**
   * Returns true if the target string starts with the given prefix
   * @param str
   * @param prefix
   * @return
   */
  static bool StartsWith(const std::string &str, const std::string &prefix);

  /**
   * Returns true if the target string <b>ends</b> with the given suffix.
   * http://stackoverflow.com/a/2072890
   * @param str
   * @param suffix
   * @return
   */
  static bool EndsWith(const std::string &str, const std::string &suffix);

  /**
   * Repeat a string multiple times
   * @param str
   * @param n
   * @return
   */
  static std::string Repeat(const std::string &str, std::size_t n);

  /**
   * Split the input string based on newline char
   * @param str
   * @param delimiter
   * @return
   */
  static std::vector<std::string> Split(const std::string &str, char delimiter);

  /**
   * Join multiple strings into one string. Components are concatenated by the
   * given separator
   * @param input
   * @param separator
   * @return
   */
  static std::string Join(const std::vector<std::string> &input, const std::string &separator);

  /**
   * Append the prefix to the beginning of each line in str
   * @param str
   * @param prefix
   * @return
   */
  static std::string Prefix(const std::string &str, const std::string &prefix);

  /**
   * Return a string that formats the give number of bytes into the appropriate
   * kilobyte, megabyte, gigabyte representation.
   * http://ubuntuforums.org/showpost.php?p=10215516&postcount=5
   * @param bytes
   * @return
   */
  static std::string FormatSize(int64_t bytes);

  /**
   * Wrap the given string with the control characters
   * to make the text appear bold in the console
   * @param str
   * @return
   */
  static std::string Bold(const std::string &str);

  /**
   * Convert a string to its uppercase form
   * @param str
   * @return
   */
  static std::string Upper(const std::string &str);

  /**
   * Convert a string to its uppercase form
   * @param str
   * @return
   */
  static std::string Lower(const std::string &str);

  /**
   * Format a string using printf semantics
   * http://stackoverflow.com/a/8098080
   * @param fmt_str
   * @param ...
   * @return
   */
  static std::string Format(const std::string fmt_str, ...);  // NOLINT

  /**
   * Split the input string into a vector of strings based on
   * the split string given us
   * @param input
   * @param split
   * @return
   */
  static std::vector<std::string> Split(const std::string &input, const std::string &split);

  /**
   * Remove the whitespace char in the right end of the string
   * @param str
   */
  static std::string RTrim(const std::string &str);

  /**
   * Return a new string that has stripped all occurrences of the provided
   * character from the provided string.
   *
   * NOTE: This function copies the input string into a new string, which is
   * wasteful. Don't use this for performance critical code, please!
   *
   * @param str The input string
   * @param c The character we want to remove
   * @return A new string with no occurrences of the provided character
   */
  static std::string Strip(const std::string &str, char c);
};

}  // namespace terrier::util
