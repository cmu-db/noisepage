#include <algorithm>
#include <limits>
#include <memory>
#include <random>

#include "execution/sql/runtime_types.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class VarlenEntryTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VarlenEntryTest, Basic) {
  std::default_random_engine gen(std::random_device{}());  // NOLINT
  std::uniform_int_distribution<uint8_t> dist(0, std::numeric_limits<uint8_t>::max());

  const uint32_t large_size = 40;
  auto large_buf_ptr = std::make_unique<byte[]>(large_size);
  byte *large_buf = large_buf_ptr.get();
  std::generate(large_buf, large_buf + large_size, [&]() { return static_cast<byte>(dist(gen)); });

  storage::VarlenEntry entry = storage::VarlenEntry::Create(large_buf, large_size, false);
  EXPECT_FALSE(entry.IsInlined());
  EXPECT_EQ(0u, std::memcmp(entry.Prefix(), large_buf, storage::VarlenEntry::PrefixSize()));
  EXPECT_EQ(large_buf, entry.Content());

  const uint32_t small_size = 10;
  byte small_buf[small_size];
  std::generate(small_buf, small_buf + small_size, [&]() { return static_cast<byte>(dist(gen)); });

  entry = storage::VarlenEntry::Create(small_buf, small_size, false);
  EXPECT_TRUE(entry.IsInlined());
  EXPECT_EQ(0u, std::memcmp(entry.Prefix(), small_buf, storage::VarlenEntry::PrefixSize()));
  EXPECT_EQ(0u, std::memcmp(entry.Content(), small_buf, small_size));
  EXPECT_NE(small_buf, entry.Content());
}

// NOLINTNEXTLINE
TEST_F(VarlenEntryTest, Comparison) {
  // Small/Small
  {
    auto e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("helo"), 4, false);
    auto e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("fuck"), 4, false);
    EXPECT_NE(e1, e2);
    EXPECT_GT(e1, e2);

    e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("he"), 2, false);
    e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hell"), 4, false);
    EXPECT_NE(e1, e2);

    e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hi"), 2, false);
    e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hi"), 2, false);
    EXPECT_EQ(e1, e2);
  }

  // Small/Medium
  {
    auto e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("helo"), 4, false);
    auto e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("fuckyou"), 7, false);
    EXPECT_NE(e1, e2);

    e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("he"), 2, false);
    e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hellothere"), 10, false);
    EXPECT_NE(e1, e2);

    e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hi"), 2, false);
    e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hi"), 2, false);
    EXPECT_EQ(e1, e2);
  }

  // Medium/Medium
  {
    auto e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hello"), 5, false);
    auto e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hellothere"), 10, false);
    EXPECT_NE(e1, e2);
    EXPECT_LT(e1, e2);

    e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hello"), 5, false);
    e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hiyathere"), 9, false);
    EXPECT_NE(e1, e2);
    EXPECT_LT(e1, e2);
  }

  // Large/Large
  {
    auto e1 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("nottodayson"), 11, false);
    auto e2 = storage::VarlenEntry::Create(reinterpret_cast<const byte *>("hellotherebro"), 13, false);
    EXPECT_NE(e1, e2);
    EXPECT_GT(e1, e2);
  }
}

}  // namespace noisepage::execution::sql::test
