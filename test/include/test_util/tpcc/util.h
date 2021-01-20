#pragma once

#include <chrono>  // NOLINT
#include <cstring>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "storage/garbage_collector.h"
#include "storage/projected_row.h"
#include "test_util/catalog_test_util.h"

namespace noisepage::tpcc {

struct Util {
  Util() = delete;

  static std::vector<catalog::col_oid_t> AllColOidsForSchema(const catalog::Schema &schema) {
    const auto &cols = schema.GetColumns();
    std::vector<catalog::col_oid_t> col_oids;
    col_oids.reserve(cols.size());
    for (const auto &col : cols) {
      col_oids.emplace_back(col.Oid());
    }
    return col_oids;
  }

  template <typename T>
  static void SetTupleAttribute(const catalog::Schema &schema, const uint32_t col_offset,
                                const storage::ProjectionMap &projection_map, storage::ProjectedRow *const pr,
                                T value) {
    NOISEPAGE_ASSERT(storage::AttrSizeBytes(schema.GetColumn(col_offset).AttributeLength()) == sizeof(T),
                     "Invalid attribute size.");
    const auto col_oid = schema.GetColumn(col_offset).Oid();
    const auto attr_offset = projection_map.at(col_oid);
    auto *const attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<T *>(attr) = value;
  }

  template <typename T>
  static void SetKeyAttribute(const catalog::IndexSchema &schema, const uint32_t col_offset,
                              const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &projection_map,
                              storage::ProjectedRow *const pr, T value) {
    const auto &key_cols = schema.GetColumns();
    NOISEPAGE_ASSERT((type::TypeUtil::GetTypeSize(key_cols.at(col_offset).Type()) & INT16_MAX) == sizeof(T),
                     "Invalid attribute size.");
    const auto col_oid = key_cols.at(col_offset).Oid();
    const auto attr_offset = static_cast<uint16_t>(projection_map.at(col_oid));
    auto *const attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<T *>(attr) = value;
  }

  static uint64_t Timestamp() {
    auto time_since_epoch = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(time_since_epoch).count());
  }

  // 2.1.6
  template <class Random>
  static uint32_t NURand(const uint32_t A, const uint32_t x, const uint32_t y, Random *const generator) {
    NOISEPAGE_ASSERT((A == 255 && x == 0 && y == 999)              // C_LAST
                         || (A == 1023 && x == 1 && y == 3000)     // C_ID
                         || (A == 8191 && x == 1 && y == 100000),  // OL_I_ID
                     "Invalid inputs to NURand().");

    static const auto c_c_last = RandomWithin<uint32_t>(0, 255, 0, generator);
    static const auto c_c_id = RandomWithin<uint32_t>(0, 1023, 0, generator);
    static const auto c_ol_i_id = RandomWithin<uint32_t>(0, 8191, 0, generator);

    uint32_t c;

    if (A == 255) {
      c = c_c_last;
    } else if (A == 1023) {
      c = c_c_id;
    } else {
      c = c_ol_i_id;
    }

    const auto rand0_a = RandomWithin<uint32_t>(0, A, 0, generator);
    const auto randxy = RandomWithin<uint32_t>(x, y, 0, generator);

    return (((rand0_a | randxy) + c) % (y - x + 1)) + x;
  }

  // 4.3.2.2
  template <class Random>
  static storage::VarlenEntry AlphaNumericVarlenEntry(const uint32_t x, const uint32_t y, const bool numeric_only,
                                                      Random *const generator) {
    NOISEPAGE_ASSERT(x <= y, "Minimum cannot be greater than the maximum length.");
    const auto astring = AlphaNumericString(x, y, numeric_only, generator);
    if (astring.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(astring.data()),
                                                static_cast<uint32_t>(astring.length()));
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(astring.length());
    std::memcpy(varlen, astring.data(), astring.length());
    return storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(astring.length()), true);
  }

  // 4.3.2.3
  static storage::VarlenEntry LastNameVarlenEntry(const uint16_t numbers) {
    NOISEPAGE_ASSERT(numbers >= 0 && numbers <= 999, "Invalid input generating C_LAST.");
    static const char *const syllables[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};

    const auto syllable1 = static_cast<const uint8_t>(numbers / 100);
    const auto syllable2 = static_cast<const uint8_t>(numbers / 10 % 10);
    const auto syllable3 = static_cast<const uint8_t>(numbers % 10);

    std::string last_name(syllables[syllable1]);
    last_name.append(syllables[syllable2]);
    last_name.append(syllables[syllable3]);

    if (last_name.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(last_name.data()),
                                                static_cast<uint32_t>(last_name.length()));
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(last_name.length());
    std::memcpy(varlen, last_name.data(), last_name.length());
    return storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(last_name.length()), true);
  }

  // 4.3.2.5
  template <class T, class Random>
  static T RandomWithin(const uint32_t x, const uint32_t y, const uint32_t p, Random *const generator) {
    return static_cast<T>(std::uniform_int_distribution(x, y)(*generator) / static_cast<T>(std::pow(10, p)));
  }

  // 4.3.2.7
  template <class Random>
  static storage::VarlenEntry ZipVarlenEntry(Random *const generator) {
    auto string = AlphaNumericString(4, 4, true, generator);
    string.append("11111");
    NOISEPAGE_ASSERT(string.length() == 9, "Wrong ZIP code length.");
    return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(string.data()),
                                              static_cast<uint32_t>(string.length()));
  }

  // 4.3.3.1
  template <class Random>
  static storage::VarlenEntry OriginalVarlenEntry(const uint32_t x, const uint32_t y, Random *const generator) {
    NOISEPAGE_ASSERT(x <= y, "Minimum cannot be greater than the maximum length.");
    auto astring = AlphaNumericString(x, y, false, generator);
    NOISEPAGE_ASSERT(astring.length() >= 8, "Needs enough room for ORIGINAL.");

    const uint32_t original_index = std::uniform_int_distribution(
        static_cast<uint32_t>(0), static_cast<uint32_t>(astring.length() - 8))(*generator);

    astring.replace(original_index, 8, "ORIGINAL");

    auto *const varlen = common::AllocationUtil::AllocateAligned(astring.length());
    std::memcpy(varlen, astring.data(), astring.length());
    return storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(astring.length()), true);
  }

 private:
  template <class Random>
  static char AlphaNumericChar(const bool numeric_only, Random *const generator) {
    static constexpr char alpha_num[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const size_t length = numeric_only ? 9 : 61;
    return alpha_num[std::uniform_int_distribution(static_cast<size_t>(0), length)(*generator)];
  }

  template <class Random>
  static std::string AlphaNumericString(const uint32_t x, const uint32_t y, const bool numeric_only,
                                        Random *const generator) {
    const uint32_t length = std::uniform_int_distribution(x, y)(*generator);
    std::string astring(length, 'a');
    for (uint32_t i = 0; i < length; i++) {
      astring[i] = AlphaNumericChar(numeric_only, generator);
    }
    return astring;
  }
};
}  // namespace noisepage::tpcc
