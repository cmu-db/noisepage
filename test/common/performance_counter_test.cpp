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
 *   uint64_t num_insert
 *   uint32_t num_hit
 *   uint16_t num_failure
 *   uint8_t num_user
 */
#define CACHE_MEMBERS(f) \
  f(uint64_t, NumInsert) \
  f(uint32_t, NumHit) \
  f(uint16_t, NumFailure) \
  f(uint8_t, NumUser)
// clang-format on

DEFINE_PERFORMANCE_CLASS(CacheCounter, CACHE_MEMBERS)

/**
 * A simple dummy network object
 *   uint64_t num_requests
 */
#define NETWORK_MEMBERS(f) f(uint64_t, num_requests)

DEFINE_PERFORMANCE_CLASS(NetworkCounter, NETWORK_MEMBERS)

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

  std::vector<double> work_probs = std::vector<double>(8, 0.125);
  std::vector<std::function<void()>> workloads = {
      [&] {
        cc->GetNumInsert()++;
        num_insert++;
        Equal();
      },
      [&] {
        cc->GetNumHit()++;
        num_hit++;
        Equal();
      },
      [&] {
        cc->GetNumFailure()++;
        num_failure++;
        Equal();
      },
      [&] {
        cc->GetNumUser()++;
        num_user++;
        Equal();
      },
      [&] {
        cc->GetNumInsert()--;
        num_insert--;
        Equal();
      },
      [&] {
        cc->GetNumHit()--;
        num_hit--;
        Equal();
      },
      [&] {
        cc->GetNumFailure()--;
        num_failure--;
        Equal();
      },
      [&] {
        cc->GetNumUser()--;
        num_user--;
        Equal();
      },
  };

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
    RandomTestUtil::InvokeWorkloadWithDistribution(workloads, work_probs, &generator, num_operations);
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
