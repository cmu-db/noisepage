#include "storage/varlen_pool.h"
#include "common/test_util.h"
#include "gtest/gtest.h"

namespace terrier {

// Allocate and free once
TEST(VarlenPoolTests, AllocateOnceTest) {
  VarlenPool pool;
  const uint32_t size = 40;

  auto *p = pool.Allocate(size);
  EXPECT_EQ(size, p->size_);

  pool.Free(p);
}

VarlenEntry *TailOf(VarlenEntry *a) {
  return testutil::IncrementByBytes(a, sizeof(uint32_t) + a->size_ - 1);
}

void CheckNotOverlapping(VarlenEntry *a, VarlenEntry *b) {
  testutil::CheckNotInBounds(a, b, testutil::IncrementByBytes(TailOf(b), 1));
  testutil::CheckNotInBounds(TailOf(a), b, testutil::IncrementByBytes(TailOf(b), 1));
  testutil::CheckNotInBounds(b, a, testutil::IncrementByBytes(TailOf(a), 1));
  testutil::CheckNotInBounds(TailOf(b), a, testutil::IncrementByBytes(TailOf(a), 1));
}

// This test generates random workload of both new and delete.
// It checks that all varlen entries allocated are well formed (size field has
// expected value, and no overlapping)
TEST(VarlenPoolTests, ConcurrentCorrectnessTest) {
  const uint32_t repeat = 100, num_threads = 8;
  for (uint32_t i = 0; i < repeat; i++) {
    VarlenPool pool;
    std::vector<std::vector<VarlenEntry *>> entries(num_threads);
    std::vector<std::vector<uint32_t>> sizes(num_threads);

    auto workload = [&](uint32_t thread_id) {
      // Randomly generate a sequence of use-free
      std::default_random_engine generator;
      // Store the pointers we use.
      auto allocate = [&] {
        uint8_t size = std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator);
        sizes[thread_id].push_back(size);
        entries[thread_id].push_back(pool.Allocate(size));
      };

      auto free = [&] {
        if (!entries[thread_id].empty()) {
          auto pos = testutil::UniformRandomElement(entries[thread_id], generator);
          // Check size field as expected
          EXPECT_EQ(sizes[thread_id][pos - entries[thread_id].begin()], (*pos)->size_);
          // clean up
          pool.Free(*pos);
          sizes[thread_id].erase(sizes[thread_id].begin() + (pos - entries[thread_id].begin()));
          entries[thread_id].erase(pos);
        }
      };

      testutil::InvokeWorkloadWithDistribution({free, allocate},
                                               {0.2, 0.8},
                                               generator,
                                               100);
    };

    testutil::RunThreadsUntilFinish(num_threads, workload);

    // Concat all the entries we have
    std::vector<VarlenEntry *> all_entries;
    for (auto &thread_entries : entries)
      for (auto *entry : thread_entries)
        all_entries.push_back(entry);

    std::vector<uint32_t> all_sizes;
    for (auto &thread_sizes : sizes)
      for (auto &size : thread_sizes)
        all_sizes.push_back(size);

    // Check size field as expected, and no overlapping memory regions
    for (uint32_t j = 0; j < all_entries.size(); j++) {
      EXPECT_EQ(all_sizes[j], all_entries[j]->size_);
      for (auto *entry : all_entries)
        if (entry != all_entries[j])
          CheckNotOverlapping(entry, all_entries[j]);
    }

    for (auto *entry : all_entries)
      pool.Free(entry);
  }

}

}  // namespace terrier
