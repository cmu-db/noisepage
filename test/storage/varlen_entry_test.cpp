#include <random>
#include <string>
#include <string_view>  // NOLINT
#include <vector>

#include "common/allocator.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage {
// This is a simple test for the behavior of varlen entry with various creation parameters, particularly
// that the various flags are stored correctly and that inlining and prefixes are handled correctly.
// NOLINTNEXTLINE
TEST(VarlenEntryTests, Basic) {
  std::default_random_engine generator;
  const uint32_t large_size = 40;
  byte *large_buffer = common::AllocationUtil::AllocateAligned(large_size);
  StorageTestUtil::FillWithRandomBytes(large_size, large_buffer, &generator);
  storage::VarlenEntry entry = storage::VarlenEntry::Create(large_buffer, large_size, true);
  EXPECT_TRUE(entry.NeedReclaim());
  EXPECT_FALSE(entry.IsInlined());
  EXPECT_EQ(std::memcmp(entry.Prefix(), large_buffer, storage::VarlenEntry::PrefixSize()), 0);
  EXPECT_EQ(entry.Content(), large_buffer);

  entry = storage::VarlenEntry::Create(large_buffer, large_size, false);
  EXPECT_FALSE(entry.NeedReclaim());
  EXPECT_FALSE(entry.IsInlined());
  EXPECT_EQ(std::memcmp(entry.Prefix(), large_buffer, storage::VarlenEntry::PrefixSize()), 0);
  EXPECT_EQ(entry.Content(), large_buffer);

  delete[] large_buffer;

  const uint32_t inlined_size = 10;
  byte inlined[inlined_size];
  StorageTestUtil::FillWithRandomBytes(inlined_size, inlined, &generator);
  entry = storage::VarlenEntry::CreateInline(inlined, inlined_size);
  EXPECT_FALSE(entry.NeedReclaim());
  EXPECT_TRUE(entry.IsInlined());
  EXPECT_EQ(std::memcmp(entry.Prefix(), inlined, storage::VarlenEntry::PrefixSize()), 0);
  EXPECT_EQ(std::memcmp(entry.Content(), inlined, inlined_size), 0);
  EXPECT_NE(entry.Content(), inlined);
}

/**
 * Simple test to demonstrate std::string_view functionalty.
 */
// NOLINTNEXTLINE
TEST(VarlenEntryTests, StringView) {
  std::string hello_world = "hello world";
  const auto inlined_entry = storage::VarlenEntry::CreateInline(reinterpret_cast<byte *const>(hello_world.data()),
                                                                static_cast<uint32_t>(hello_world.length()));
  auto inlined_string_view = inlined_entry.StringView();
  EXPECT_EQ(inlined_string_view, hello_world);

  std::string not_hello_world = "this is not a hello world string at all.";
  const uint32_t large_size = 40;
  byte *large_buffer = common::AllocationUtil::AllocateAligned(large_size);
  std::memcpy(large_buffer, not_hello_world.data(), not_hello_world.length());
  storage::VarlenEntry non_inlined_entry = storage::VarlenEntry::Create(large_buffer, large_size, true);
  auto non_inlined_string_view = non_inlined_entry.StringView();
  EXPECT_EQ(non_inlined_string_view, not_hello_world);
  delete[] large_buffer;
}

/**
 * Simple test to demonstrate array serialization and deserialization.
 */
// NOLINTNEXTLINE
TEST(VarlenEntryTests, Array) {
  {
    std::vector<int32_t> test_data{2, 5, 6, 7, 2};
    const auto varlen_entry = storage::StorageUtil::CreateVarlen(test_data);
    const std::vector<int32_t> test_view = varlen_entry.DeserializeArray<int32_t>();
    EXPECT_EQ(test_data, test_view);
    delete[] varlen_entry.Content();
  }

  // test inline
  {
    std::vector<int32_t> test_data{2};
    const auto varlen_entry = storage::StorageUtil::CreateVarlen(test_data);
    const std::vector<int32_t> test_view = varlen_entry.DeserializeArray<int32_t>();
    EXPECT_EQ(test_data, test_view);
  }

  // test strings
  {
    std::vector<std::string> test_data{"hello", "world", "i am", "a test"};
    std::vector<std::string_view> sv_test_data;
    sv_test_data.reserve(test_data.size());
    for (std::string &s : test_data) {
      sv_test_data.push_back(s);
    }
    const auto varlen_entry = storage::StorageUtil::CreateVarlen(test_data);
    std::vector<std::string_view> test_view = varlen_entry.DeserializeArrayVarlen();
    EXPECT_EQ(sv_test_data, test_view);
    delete[] varlen_entry.Content();
  }
}

/**
 * Test equality operator for inline
 */
// NOLINTNEXTLINE
TEST(VarlenEntryTests, InlineEquality) {
  constexpr std::string_view wan = "wan";
  constexpr std::string_view matt = "matt";
  constexpr std::string_view john = "john";
  constexpr std::string_view johnny = "johnny";
  constexpr std::string_view johnie = "johnie";
  EXPECT_EQ(storage::VarlenEntry::Create(wan),
            storage::VarlenEntry::Create(wan));  // equal in prefix, doesn't have content
  EXPECT_EQ(storage::VarlenEntry::Create(john), storage::VarlenEntry::Create(john));      // equal thru prefix
  EXPECT_EQ(storage::VarlenEntry::Create(johnny), storage::VarlenEntry::Create(johnny));  // equal thru content
  EXPECT_NE(storage::VarlenEntry::Create(john),
            storage::VarlenEntry::Create(matt));  //  same length, different prefix
  EXPECT_NE(storage::VarlenEntry::Create(matt),
            storage::VarlenEntry::Create(johnny));  // different prefix, different length
  EXPECT_NE(storage::VarlenEntry::Create(john),
            storage::VarlenEntry::Create(johnny));  // equal thru prefix, different length
  EXPECT_NE(storage::VarlenEntry::Create(johnny),
            storage::VarlenEntry::Create(johnie));  // equal thru prefix, equal in length, different in content
}

/**
 * Test equality operator for non-inline
 */
// NOLINTNEXTLINE
TEST(VarlenEntryTests, NonInlineEquality) {
  constexpr std::string_view matthew = "matthew";
  constexpr std::string_view matthew_was_here = "matthew_was_here";
  constexpr std::string_view matthew_was_gone = "matthew_was_gone";
  constexpr std::string_view matthew_was_not_here = "matthew_was_not_here";

  EXPECT_EQ(storage::VarlenEntry::Create(matthew_was_here),
            storage::VarlenEntry::Create(matthew_was_here));  // equal thru content
  EXPECT_NE(storage::VarlenEntry::Create(matthew),
            storage::VarlenEntry::Create(matthew_was_here));  // equal thru prefix, different length (in-line)
  EXPECT_NE(storage::VarlenEntry::Create(matthew_was_gone),
            storage::VarlenEntry::Create(
                matthew_was_here));  // equal thru prefix, same length, different in content (non-in-line)
  EXPECT_NE(storage::VarlenEntry::Create(matthew_was_gone),
            storage::VarlenEntry::Create(matthew_was_not_here));  // equal thru prefix, different length (non-in-line)
}

/**
 * Test that ownership doesn't change whether VarlenEntry content compares equal
 */
// NOLINTNEXTLINE
TEST(VarlenEntryTests, OwnershipEquality) {
  constexpr std::string_view matthew_was_here = "matthew_was_here";  // non-in-line

  EXPECT_EQ(storage::VarlenEntry::Create(reinterpret_cast<const byte *const>(matthew_was_here.data()),
                                         matthew_was_here.length(), false),
            storage::VarlenEntry::Create(reinterpret_cast<const byte *const>(matthew_was_here.data()),
                                         matthew_was_here.length(), false));

  EXPECT_EQ(storage::VarlenEntry::Create(reinterpret_cast<const byte *const>(matthew_was_here.data()),
                                         matthew_was_here.length(), false),
            storage::VarlenEntry::Create(reinterpret_cast<const byte *const>(matthew_was_here.data()),
                                         matthew_was_here.length(), true));

  EXPECT_EQ(storage::VarlenEntry::Create(reinterpret_cast<const byte *const>(matthew_was_here.data()),
                                         matthew_was_here.length(), true),
            storage::VarlenEntry::Create(reinterpret_cast<const byte *const>(matthew_was_here.data()),
                                         matthew_was_here.length(), true));
}

// NOLINTNEXTLINE
TEST(VarlenEntryTests, JsonTest) {
  std::string hello_world = "hello world";
  const auto entry = storage::VarlenEntry::Create(hello_world);

  nlohmann::json json;
  nlohmann::adl_serializer<storage::VarlenEntry>::to_json(json, entry);
  EXPECT_EQ(json.at("string_content").get<std::string>(), hello_world);

  auto deserialized_entry = nlohmann::adl_serializer<storage::VarlenEntry>::from_json(json);
  EXPECT_EQ(deserialized_entry, entry);
}

}  // namespace noisepage
