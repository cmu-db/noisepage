#include "common/performance_counter.h"
#include <functional>
#include <random>
#include <vector>
#include "common/json.h"
#include "gtest/gtest.h"
#include "util/random_test_util.h"

namespace terrier {

// clang-format off
/**
 * A simple dummy cache object with four differently typed attributes:
 *   uint64_t NumInsert
 *   uint32_t NumHit
 *   uint16_t NumFailure
 *   uint8_t NumUser
 */
#define CACHE_MEMBERS(f) \
  f(uint64_t, NumInsert) \
  f(uint32_t, NumHit) \
  f(uint16_t, NumFailure) \
  f(uint8_t, NumUser)
// clang-format on

DEFINE_PERFORMANCE_CLASS(CacheCounter, CACHE_MEMBERS)

/**
 * Helper class for testing the four attributes of a CacheCounter.
 */
class CacheCounterTestObject {
 private:
  std::atomic<uint64_t> num_insert{0};
  std::atomic<uint32_t> num_hit{0};
  std::atomic<uint16_t> num_failure{0};
  std::atomic<uint8_t> num_user{0};
  CacheCounter *cc;

  /**
   * Calls the gtest EXPECT_EQ function for all the relevant variables.
   */
  void Equal() const {
    EXPECT_EQ(cc->GetNumInsert(), num_insert);
    EXPECT_EQ(cc->GetNumHit(), num_hit);
    EXPECT_EQ(cc->GetNumFailure(), num_failure);
    EXPECT_EQ(cc->GetNumUser(), num_user);
  }

 public:
  /**
   * Instantiates the CacheCounterTestObject with the given CacheCounter.
   * It will check that it remains consistent with the CacheCounter;
   * if you modify the CacheCounter, make sure you tell it.
   */
  explicit CacheCounterTestObject(CacheCounter *cc) : cc(cc) {}

  /**
   * Zeroes the cache counter, automatically checking that all the state remains consistent.
   */
  void Zero() {
    num_insert.store(0);
    num_hit.store(0);
    num_failure.store(0);
    num_user.store(0);
    cc->ZeroCounters();
    Equal();
  }

  /**
   * Runs random operations, automatically checking that all the state remains consistent.
   * @param generator random number generator
   * @param num_operations number of operations to run
   */
  void RandomOperation(std::default_random_engine generator, uint32_t num_operations) {
    std::uniform_int_distribution<uint8_t> rng(0, 255);
    uint8_t num = rng(generator);

    std::vector<std::function<void()>> workloads = {
        [&] {
          cc->IncrementNumInsert(num);
          num_insert += num;
          Equal();
        },
        [&] {
          cc->IncrementNumHit(num);
          num_hit += num;
          Equal();
        },
        [&] {
          cc->IncrementNumFailure(num);
          num_failure += num;
          Equal();
        },
        [&] {
          cc->IncrementNumUser(num);
          num_user += num;
          Equal();
        },
        [&] {
          cc->DecrementNumInsert(num);
          num_insert -= num;
          Equal();
        },
        [&] {
          cc->DecrementNumHit(num);
          num_hit -= num;
          Equal();
        },
        [&] {
          cc->DecrementNumFailure(num);
          num_failure -= num;
          Equal();
        },
        [&] {
          cc->DecrementNumUser(num);
          num_user -= num;
          Equal();
        },
        [&] {
          cc->SetNumInsert(num);
          num_insert = num;
          Equal();
        },
        [&] {
          cc->SetNumHit(num);
          num_hit = num;
          Equal();
        },
        [&] {
          cc->SetNumFailure(num);
          num_failure = num;
          Equal();
        },
        [&] {
          cc->SetNumUser(num);
          num_user = num;
          Equal();
        },
    };
    std::vector<double> probs = std::vector<double>(workloads.size(), 1.0 / static_cast<double>(workloads.size()));

    RandomTestUtil::InvokeWorkloadWithDistribution(workloads, probs, &generator, num_operations);
  }
};

// Test simple increment/decrement/zero/name setting operations on a performance counter
// NOLINTNEXTLINE
TEST(PerformanceCounterTests, GTEST_DEBUG_ONLY(SimpleCorrectnessTest)) {
  std::default_random_engine generator;
  const uint32_t num_iterations = 1000;
  const uint32_t num_operations = 1000;

  CacheCounter cc;
  CacheCounterTestObject cc_test(&cc);

  // check we can change name, by default it is the class name
  EXPECT_EQ(cc.GetName(), "CacheCounter");
  cc.SetName("NewName");
  EXPECT_EQ(cc.GetName(), "NewName");
  cc.SetName("CacheCounter");
  EXPECT_EQ(cc.GetName(), "CacheCounter");

  cc_test.Zero();
  // test that our counter works for increment/decrement/get
  for (uint32_t i = 0; i < num_iterations; ++i) {
    cc_test.RandomOperation(generator, num_operations);
  }
}

// Test that we can serialize and deserialize our performance counters
// NOLINTNEXTLINE
TEST(PerformanceCounterTests, GTEST_DEBUG_ONLY(SerializationTest)) {
  std::default_random_engine generator;
  const uint32_t num_iterations = 1000;
  const uint32_t num_operations = 100;

  CacheCounter cc;
  CacheCounterTestObject cc_test(&cc);

  terrier::common::json json_old, json_new;

  for (uint32_t i = 0; i < num_iterations; ++i) {
    cc_test.Zero();

    // perform some random operations
    cc_test.RandomOperation(generator, num_operations);
    // save the current state
    json_old = cc.ToJson();
    EXPECT_EQ(json_old, cc.ToJson());
    // perform one more random operation
    cc_test.RandomOperation(generator, 1);
    // assert the state is not the same
    json_new = cc.ToJson();
    EXPECT_EQ(json_new, cc.ToJson());
    EXPECT_NE(json_old, json_new);
    // restore cc from the old state
    cc.FromJson(json_old);
    // assert the state is the same now
    EXPECT_EQ(json_old, cc.ToJson());
  }
}
}  // namespace terrier
