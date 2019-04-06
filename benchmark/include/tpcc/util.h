#pragma once

#include <chrono>  // NOLINT
#include <random>

namespace terrier::tpcc {
struct RandomUtil {
  RandomUtil() = delete;

  // 2.1.6
  template <class Random>
  static int32_t NURand(const int32_t A, const int32_t x, const int32_t y, Random *const generator) {
    TERRIER_ASSERT(
        (A == 255 && x == 0 && y == 999) || (A == 1023 && x == 1 && y == 3000) || (A == 8191 && x == 1 && y == 100000),
        "Invalid inputs to NURand().");

    static const auto C_c_last = RandomWithin<int32_t>(0, 255, 0, generator);
    static const auto C_c_id = RandomWithin<int32_t>(0, 1023, 0, generator);
    static const auto C_ol_i_id = RandomWithin<int32_t>(0, 8191, 0, generator);

    int32_t C;

    if (A == 255) {
      C = C_c_last;
    } else if (A == 1023) {
      C = C_c_id;
    } else {
      C = C_ol_i_id;
    }

    return (((RandomWithin<int32_t>(0, A, 0, generator) | RandomWithin<int32_t>(x, y, 0, generator)) + C) %
            (y - x - +1)) +
           x;
  }

  // 4.3.2.2
  template <class Random>
  static storage::VarlenEntry RandomAlphaNumericVarlenEntry(const uint32_t x, const uint32_t y, const bool numeric_only,
                                                            Random *const generator) {
    TERRIER_ASSERT(x <= y, "Minimum cannot be greater than the maximum length.");
    const auto astring = RandomAlphaNumericString(x, y, numeric_only, generator);
    if (astring.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(astring.data()), astring.length());
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(astring.length());
    std::memcpy(varlen, astring.data(), astring.length());
    return storage::VarlenEntry::Create(varlen, astring.length(), true);
  }

  // 4.3.2.3
  static storage::VarlenEntry RandomLastNameVarlenEntry(const uint16_t numbers) {
    TERRIER_ASSERT(numbers >= 0 && numbers <= 999, "Invalid input generating C_LAST.");
    static const char *const syllables[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};

    const uint8_t syllable1 = numbers / 100;
    const uint8_t syllable2 = (numbers / 10 % 10);
    const uint8_t syllable3 = numbers % 10;

    std::string last_name(syllables[syllable1]);
    last_name.append(syllables[syllable2]);
    last_name.append(syllables[syllable3]);

    if (last_name.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(last_name.data()), last_name.length());
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(last_name.length());
    std::memcpy(varlen, last_name.data(), last_name.length());
    return storage::VarlenEntry::Create(varlen, last_name.length(), true);
  }

  // 4.3.2.5
  template <class T, class Random>
  static T RandomWithin(uint32_t x, uint32_t y, uint32_t p, Random *const generator) {
    return std::uniform_int_distribution(x, y)(*generator) / static_cast<T>(std::pow(10, p));
  }

  // 4.3.2.7
  template <class Random>
  static storage::VarlenEntry RandomZipVarlenEntry(Random *const generator) {
    auto string = RandomAlphaNumericString(4, 4, true, generator);
    string.append("11111");
    TERRIER_ASSERT(string.length() == 9, "Wrong ZIP code length.");
    return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(string.data()), string.length());
  }

  // 4.3.3.1
  template <class Random>
  static storage::VarlenEntry RandomOriginalVarlenEntry(const uint32_t x, const uint32_t y, Random *const generator) {
    TERRIER_ASSERT(x <= y, "Minimum cannot be greater than the maximum length.");
    auto astring = RandomAlphaNumericString(x, y, false, generator);
    TERRIER_ASSERT(astring.length() >= 8, "Needs enough room for ORIGINAL.");

    const uint32_t original_index = std::uniform_int_distribution(
        static_cast<uint32_t>(0), static_cast<uint32_t>(astring.length() - 8))(*generator);

    astring.replace(original_index, 8, "ORIGINAL");

    auto *const varlen = common::AllocationUtil::AllocateAligned(astring.length());
    std::memcpy(varlen, astring.data(), astring.length());
    return storage::VarlenEntry::Create(varlen, astring.length(), true);
  }

 private:
  template <class Random>
  static char RandomAlphaNumericChar(const bool numeric_only, Random *const generator) {
    static constexpr char alpha_num[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const size_t length = numeric_only ? 9 : 61;
    return alpha_num[std::uniform_int_distribution(static_cast<size_t>(0), length)(*generator)];
  }

  template <class Random>
  static std::string RandomAlphaNumericString(const uint32_t x, const uint32_t y, const bool numeric_only,
                                              Random *const generator) {
    const uint32_t length = std::uniform_int_distribution(x, y)(*generator);
    std::string astring(length, 'a');
    for (uint32_t i = 0; i < length; i++) {
      astring[i] = RandomAlphaNumericChar(numeric_only, generator);
    }
    return astring;
  }
};
}