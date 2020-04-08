#include "common/concurrent_vector.h"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace terrier {

// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, BasicInsertLookUpTest) {
  const uint64_t num_inserts = 100000;
  const uint64_t num_iterations = 100;
  for (uint64_t _ = 0; _ < num_iterations; _++) {
    common::ConcurrentVector<uint64_t> v;
    uint64_t location_array[num_inserts];
    for (uint64_t i = 0; i < num_inserts; i++) {
      uint64_t inserted_index UNUSED_ATTRIBUTE = v.Insert(&location_array[i]);
      EXPECT_EQ(i, inserted_index);
    }
    EXPECT_EQ(v.size(), num_inserts);

    for (uint64_t i = 0; i < num_inserts; i++) {
      EXPECT_EQ(v.LookUp(i), &location_array[i]);
    }
  }
}

// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, ConcurrentInsert) {
  const uint64_t num_inserts = 100000;
  const uint64_t num_iterations = 100;
  const uint64_t num_threads = 8;
  for (uint64_t _ = 0; _ < num_iterations; _++) {
    std::thread threads[num_threads];
    std::vector<std::vector<uint64_t>> indexes(num_threads);
    uint64_t location_array[num_inserts];
    common::ConcurrentVector<uint64_t> v;
    for (uint64_t i = 0; i < num_threads; i++) { // NOLINT
      threads[i] = std::thread([&, i] {
        for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
          indexes[i].emplace_back(v.Insert(&location_array[index * num_threads + i]));
        }
      });
    }

    for (uint64_t i = 0; i < num_threads; i++) { // NOLINT
      threads[i].join();
    }

    for (uint64_t i = 0; i < num_threads; i++) {
      for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
        EXPECT_EQ(v.LookUp(indexes[i][index]), &location_array[index * num_threads + i]);
      }
    }

  }
}


// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, ConcurrentInsertAndLookup) {
  const uint64_t num_inserts = 100000;
  const uint64_t num_iterations = 100;
  const uint64_t num_threads = 8;
  for (uint64_t _ = 0; _ < num_iterations; _++) {
    std::thread threads[num_threads];
    std::vector<std::vector<uint64_t>> indexes(num_threads);
    uint64_t location_array[num_inserts];
    common::ConcurrentVector<uint64_t> v;
    for (uint64_t i = 0; i < num_threads; i++) { // NOLINT
      threads[i] = std::thread([&, i] {
        for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
          indexes[i].emplace_back(v.Insert(&location_array[index * num_threads + i]));
          EXPECT_EQ(v.LookUp(indexes[i][index]), &location_array[index * num_threads + i]);
        }
      });
    }

    for (uint64_t i = 0; i < num_threads; i++) { // NOLINT
      threads[i].join();
    }

    for (uint64_t i = 0; i < num_threads; i++) {
      for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
        EXPECT_EQ(v.LookUp(indexes[i][index]), &location_array[index * num_threads + i]);
      }
    }

  }
}




}  // namespace terrier
