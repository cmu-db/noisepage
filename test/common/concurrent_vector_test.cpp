#include <thread>  // NOLINT
#include <vector>

#include "common/concurrent_vector.h"
#include "gtest/gtest.h"

namespace terrier {

// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, BasicInsertLookUpTest) {
  const uint64_t num_inserts = 100000;
  const uint64_t num_iterations = 100;
  for (uint64_t _ = 0; _ < num_iterations; _++) {
    common::ConcurrentVector<uint64_t *> v;
    uint64_t location_array[num_inserts];
    for (uint64_t i = 0; i < num_inserts; i++) {
      v.PushBack(&location_array[i]);
    }
    EXPECT_EQ(v.size(), num_inserts);

    for (uint64_t i = 0; i < num_inserts; i++) {
      EXPECT_EQ(v[i], &location_array[i]);
    }
  }
}

// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, ConcurrentInsert) {
  const uint64_t num_inserts = 1000;
  const uint64_t num_iterations = 100;
  const uint64_t num_threads = std::thread::hardware_concurrency();
  for (uint64_t _ = 0; _ < num_iterations; _++) {  // NOLINT
    std::thread threads[num_threads];              // NOLINT
    std::vector<std::vector<uint64_t>> indexes(num_threads);
    std::vector<std::vector<common::ConcurrentVector<uint64_t*>::Iterator>> iterators(num_threads);
    uint64_t location_array[num_inserts];
    common::ConcurrentVector<uint64_t*> v;
    for (uint64_t i = 0; i < num_threads; i++) {  // NOLINT
      threads[i] = std::thread([&, i] {
        for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
          iterators[i].emplace_back(v.PushBack(&location_array[index * num_threads + i]));
        }
      });
    }

    for (uint64_t i = 0; i < num_threads; i++) {  // NOLINT
      threads[i].join();
    }

    for (uint64_t thread = 0; thread < num_threads; thread++) {
      for (auto &iterator : iterators[thread]) {
        uint64_t i = 0;
        for (auto it = v.begin(); it != v.end() && it != iterator; it++) i++;
        indexes[thread].emplace_back(i);
      }
    }

    for (uint64_t i = 0; i < num_threads; i++) {
      for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
        EXPECT_EQ(v[indexes[i][index]], &location_array[index * num_threads + i]);
      }
    }
  }
}

// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, ConcurrentInsertAndLookup) {
  const uint64_t num_inserts = 1000;
  const uint64_t num_iterations = 100;
  const uint64_t num_threads = std::thread::hardware_concurrency();
  for (uint64_t _ = 0; _ < num_iterations; _++) {  // NOLINT
    std::thread threads[num_threads];              // NOLINT
    std::vector<std::vector<uint64_t>> indexes(num_threads);
    std::vector<std::vector<common::ConcurrentVector<uint64_t*>::Iterator>> iterators(num_threads);
    uint64_t location_array[num_inserts];
    common::ConcurrentVector<uint64_t *> v;
    for (uint64_t i = 0; i < num_threads; i++) {  // NOLINT
      threads[i] = std::thread([&, i] {
        for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
          auto it = v.PushBack(&location_array[index * num_threads + i]);
          iterators[i].emplace_back(it);
          EXPECT_EQ(*it, &location_array[index * num_threads + i]);
        }
      });
    }

    for (uint64_t i = 0; i < num_threads; i++) {  // NOLINT
      threads[i].join();
    }

    for (uint64_t thread = 0; thread < num_threads; thread++) {
      for (auto &iterator : iterators[thread]) {
        uint64_t i = 0;
        for (auto it = v.begin(); it != v.end() && it != iterator; it++) i++;
        indexes[thread].emplace_back(i);
      }
    }

    for (uint64_t i = 0; i < num_threads; i++) {
      for (uint64_t index = 0; index < num_inserts / num_threads; index++) {
        EXPECT_EQ(v[indexes[i][index]], &location_array[index * num_threads + i]);
      }
    }
  }
}

// NOLINTNEXTLINE
TEST(ConcurrentVectorTest, ConcurrentInsertLookupAndIteration) {
  const uint64_t num_inserts = 10000;
  const uint64_t num_iterations = 100;
  const uint64_t num_threads = std::thread::hardware_concurrency();
  for (uint64_t _ = 0; _ < num_iterations; _++) {  // NOLINT
    std::thread threads[num_threads];              // NOLINT
    std::vector<std::vector<uint64_t>> indexes(num_threads);
    std::vector<std::vector<common::ConcurrentVector<uint64_t*>::Iterator>> iterators(num_threads);
    uint64_t location_array[num_inserts];
    common::ConcurrentVector<uint64_t *> v;
    std::atomic<uint64_t> num_inserted = 0;
    for (uint64_t i = 0; i < num_threads; i++) {  // NOLINT
      threads[i] = std::thread([&, i] {
        if (i % 2 != 0) {
          uint64_t num_to_find = num_inserted;
          uint64_t num_found = 0;
          for (auto it = v.begin(); it != v.end(); it++) {
            num_found++;
          }
          EXPECT_GE(num_found, num_to_find);
          return;
        }
        for (uint64_t index = 0; index < 2 * num_inserts / num_threads; index++) {
          auto it = v.PushBack(&location_array[index * num_threads + i]);
          iterators[i].emplace_back(it);
          EXPECT_EQ(*it, &location_array[index * num_threads + i]);
          num_inserted++;
        }
      });
    }

    for (uint64_t i = 0; i < num_threads; i++) {  // NOLINT
      threads[i].join();
    }

    for (uint64_t thread = 0; thread < num_threads; thread++) {
      for (auto &iterator : iterators[thread]) {
        uint64_t i = 0;
        for (auto it = v.begin(); it != v.end() && it != iterator; it++) i++;
        indexes[thread].emplace_back(i);
      }
    }

    for (uint64_t i = 0; i < num_threads; i++) {
      if (i % 2 != 0) continue;
      for (uint64_t index = 0; index < 2 * num_inserts / num_threads; index++) {
        EXPECT_EQ(v[indexes[i][index]], &location_array[index * num_threads + i]);
      }
    }
  }
}

}  // namespace terrier
