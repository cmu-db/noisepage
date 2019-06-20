#include "storage/block_access_controller.h"
#include <thread>  // NOLINT
#include "util/test_harness.h"
namespace terrier {
// Some hacked together infrastructure to reason about the progress of test threads.
#define DECLARE_PROGRAM_POINT(name) volatile bool name = false;
#define PROGRAM_POINT(name) name = true;
#define WAIT_UNTIL(expr) \
  while (!(expr)) std::this_thread::yield();
#define REACHED(name) (name)

// Tests that a single reader behaves as expected
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
TEST(BlockAccessControllerTest, WriterPreemption) {
  storage::BlockAccessController tested;
  tested.Initialize();
  tested.GetBlockState()->store(storage::BlockState::COOLING);
  tested.WaitUntilHot();
  EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
}

// Tests that the freezing flag blocks both readers and writers
TEST(BlockAccessControllerTest, FreezingBlocksBoth) {
  const uint32_t iteration = 100;
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
TEST(BlockAccessControllerTest, ReaderWriter) {
  const uint32_t iteration = 100;
  for (uint32_t i = 0; i < iteration; i++) {
    storage::BlockAccessController tested;
    tested.Initialize();
    tested.GetBlockState()->store(storage::BlockState::FROZEN);
    DECLARE_PROGRAM_POINT(read_acquired)
    DECLARE_PROGRAM_POINT(read_released)
    std::thread reader([&] {
      EXPECT_TRUE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(read_acquired)
      // Block the reader until the write has executed
      WAIT_UNTIL(tested.GetBlockState()->load() == storage::BlockState::HOT)
      EXPECT_FALSE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(read_released)
      tested.ReleaseInPlaceRead();
    });

    WAIT_UNTIL(read_acquired)
    tested.WaitUntilHot();
    // The thread should only be unblocked when the reader has released the lock
    EXPECT_TRUE(read_released);
    EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
    reader.join();
  }
}

// Tests that writer waits for all readers to exit
TEST(BlockAccessControllerTest, MultipleReaders) {
  const uint32_t iteration = 100;
  for (uint32_t i = 0; i < iteration; i++) {
    storage::BlockAccessController tested;
    tested.Initialize();
    tested.GetBlockState()->store(storage::BlockState::FROZEN);
    DECLARE_PROGRAM_POINT(reader1_acquired)
    DECLARE_PROGRAM_POINT(reader1_released)
    DECLARE_PROGRAM_POINT(reader2_acquired)
    DECLARE_PROGRAM_POINT(reader2_released)
    std::thread reader1([&] {
      EXPECT_TRUE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(reader1_acquired)
      tested.ReleaseInPlaceRead();
      PROGRAM_POINT(reader1_released)
    });

    std::thread reader2([&] {
      EXPECT_TRUE(tested.TryAcquireInPlaceRead());
      PROGRAM_POINT(reader2_acquired)
      tested.ReleaseInPlaceRead();
      PROGRAM_POINT(reader2_released)
    });
    WAIT_UNTIL(reader1_acquired && reader2_acquired)
    tested.WaitUntilHot();
    EXPECT_TRUE(REACHED(reader1_released) && REACHED(reader1_released));
    EXPECT_EQ(tested.GetBlockState()->load(), storage::BlockState::HOT);
    reader1.join();
    reader2.join();
  }
}
}  // namespace terrier
