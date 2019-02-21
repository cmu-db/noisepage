#include <random>
#include "storage/storage_defs.h"
#include "util/storage_test_util.h"
#include "common/allocator.h"
#include "util/test_harness.h"

namespace terrier {
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
  EXPECT_TRUE(std::memcmp(entry.Prefix(), large_buffer, storage::VarlenEntry::PrefixSize()) == 0);
  EXPECT_EQ(entry.Content(), large_buffer);

  entry = storage::VarlenEntry::Create(large_buffer, large_size, false);
  printf("%d\n", entry.size_);
  EXPECT_FALSE(entry.NeedReclaim());
  EXPECT_FALSE(entry.IsInlined());
  EXPECT_TRUE(std::memcmp(entry.Prefix(), large_buffer, storage::VarlenEntry::PrefixSize()) == 0);
  EXPECT_EQ(entry.Content(), large_buffer);

  delete[] large_buffer;

  const uint32_t inlined_size = 10;
  byte inlined[inlined_size];
  StorageTestUtil::FillWithRandomBytes(inlined_size, inlined, &generator);
  entry = storage::VarlenEntry::CreateInline(inlined, inlined_size);
  EXPECT_FALSE(entry.NeedReclaim());
  EXPECT_TRUE(entry.IsInlined());
  EXPECT_TRUE(std::memcmp(entry.Prefix(), inlined, storage::VarlenEntry::PrefixSize()) == 0);
  EXPECT_TRUE(std::memcmp(entry.Content(), inlined, inlined_size) == 0);
  EXPECT_NE(entry.Content(), inlined);
}
}
