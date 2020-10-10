#include "storage/block_access_controller.h"

#include <thread>  // NOLINT

#include "test_util/test_harness.h"
namespace terrier {
// Some hacked together infrastructure to reason about the progress of test threads.
#define DECLARE_PROGRAM_POINT(name) volatile bool name = false;
#define PROGRAM_POINT(name) name = true;
#define WAIT_UNTIL(expr) \
  while (!(expr)) std::this_thread::yield();
#define REACHED(name) (name)

// Tests that a single reader behaves as expected
// NOLINTNEXTLINE
TEST(BlockAccessControllerTest, SingleReader) {
  storage::BlockAccessController tested;
  tested.Initialize();
  EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
  EXPECT_FALSE(tested.TryAcquireInPlaceRead());
  tested.GetBlockState()->store(storage::BlockState::COOLING);
  EXPECT_FALSE(tested.TryAcquireInPlaceRead());
  tested.GetBlockState()->store(storage::BlockState::FREEZING);
  EXPECT_FALSE(tested.TryAcquireInPlaceRead());
  tested.GetBlockState()->store(storage::BlockState::FROZEN);
  EXPECT_TRUE(tested.TryAcquireInPlaceRead());
}

// Tests that a writer is able to preempt the compactor
// NOLINTNEXTLINE
TEST(BlockAccessControllerTest, WriterPreemption) {
  storage::BlockAccessController tested;
  tested.Initialize();
  tested.GetBlockState()->store(storage::BlockState::COOLING);
  tested.WaitUntilHot();
  EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
}

// Tests that the freezing flag blocks both readers and writers
// NOLINTNEXTLINE
TEST(BlockAccessControllerTest, FreezingBlocksBoth) {
  const uint32_t iteration = 10000;
  for (uint32_t i = 0; i < iteration; i++) {
    storage::BlockAccessController tested;
    tested.Initialize();
    tested.GetBlockState()->store(storage::BlockState::FREEZING);
    DECLARE_PROGRAM_POINT(flag_flipped)
    std::thread writer([&] {
      tested.WaitUntilHot();
      EXPECT_TRUE(REACHED(flag_flipped));
      EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
    });

    EXPECT_FALSE(tested.TryAcquireInPlaceRead());
    // This is technically not atomic, but I am not worried about splitting two instructions here...
    PROGRAM_POINT(flag_flipped)
    tested.GetBlockState()->store(storage::BlockState::FROZEN);
    writer.join();
  }
}

// Tests that writer blocks future readers but waits for reader exit
// NOLINTNEXTLINE
TEST(BlockAccessControllerTest, ReaderWriter) {
  const uint32_t iteration = 10000;
  for (uint32_t i = 0; i < iteration; i++) {
    storage::BlockAccessController tested;
    tested.Initialize();
    tested.GetBlockState()->store(storage::BlockState::FROZEN);
    DECLARE_PROGRAM_POINT(read_acquired)
    DECLARE_PROGRAM_POINT(read_releasing)
    std::thread reader([&] {
      EXPECT_TRUE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(read_acquired)
      // Block the reader until the write has executed
      WAIT_UNTIL(tested.GetBlockState()->load() == storage::BlockState::HOT)
      EXPECT_FALSE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(read_releasing)
      tested.ReleaseInPlaceRead();
    });

    WAIT_UNTIL(read_acquired)
    tested.WaitUntilHot();
    // The thread should only be unblocked when the reader has released the lock
    EXPECT_TRUE(read_releasing);
    EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
    reader.join();
  }
}

// Tests that writer waits for all readers to exit
// NOLINTNEXTLINE
TEST(BlockAccessControllerTest, MultipleReaders) {
  const uint32_t iteration = 10000;
  for (uint32_t i = 0; i < iteration; i++) {
    storage::BlockAccessController tested;
    tested.Initialize();
    tested.GetBlockState()->store(storage::BlockState::FROZEN);
    DECLARE_PROGRAM_POINT(reader1_acquired)
    DECLARE_PROGRAM_POINT(reader1_releasing)
    DECLARE_PROGRAM_POINT(reader2_acquired)
    DECLARE_PROGRAM_POINT(reader2_releasing)
    std::thread reader1([&] {
      EXPECT_TRUE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(reader1_acquired)
      PROGRAM_POINT(reader1_releasing)
      tested.ReleaseInPlaceRead();
    });

    std::thread reader2([&] {
      EXPECT_TRUE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(reader2_acquired)
      PROGRAM_POINT(reader2_releasing)
      tested.ReleaseInPlaceRead();
    });
    WAIT_UNTIL(reader1_acquired && reader2_acquired)
    tested.WaitUntilHot();
    EXPECT_TRUE(REACHED(reader1_releasing) && REACHED(reader2_releasing));
    EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
    reader1.join();
    reader2.join();
  }
}
}  // namespace terrier
