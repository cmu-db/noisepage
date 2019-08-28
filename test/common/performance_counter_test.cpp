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
  std::atomic<uint64_t> num_insert_{0};
  std::atomic<uint32_t> num_hit_{0};
  std::atomic<uint16_t> num_failure_{0};
  std::atomic<uint8_t> num_user_{0};
  CacheCounter *cc_;
  std::uniform_int_distribution<uint8_t> rng_;

  /**
   * Calls the gtest EXPECT_EQ function for all the relevant variables.
   */
  void Equal() const {
    EXPECT_EQ(cc_->GetNumInsert(), num_insert_);
    EXPECT_EQ(cc_->GetNumHit(), num_hit_);
    EXPECT_EQ(cc_->GetNumFailure(), num_failure_);
    EXPECT_EQ(cc_->GetNumUser(), num_user_);
  }

 public:
  /**
   * Instantiates the CacheCounterTestObject with the given CacheCounter.
   * It will check that it remains consistent with the CacheCounter;
   * if you modify the CacheCounter, make sure you tell it.
   */
  explicit CacheCounterTestObject(CacheCounter *cc) : cc_(cc), rng_(1, 255) {}

  /**
   * Zeroes the cache counter, automatically checking that all the state remains consistent.
   */
  void Zero() {
    num_insert_.store(0);
    num_hit_.store(0);
    num_failure_.store(0);
    num_user_.store(0);
    cc_->ZeroCounters();
    Equal();
  }

  /**
   * Runs random operations, automatically checking that all the state remains consistent.
   * @param generator random number generator
   * @param num_operations number of operations to run
   */
  void RandomOperation(std::default_random_engine generator, uint32_t num_operations) {
    uint8_t num = rng_(generator);

    std::vector<std::function<void()>> workloads = {
        [&] {
          cc_->IncrementNumInsert(num);
          num_insert_ += num;
          Equal();
        },
        [&] {
          cc_->IncrementNumHit(num);
          num_hit_ += num;
          Equal();
        },
        [&] {
          cc_->IncrementNumFailure(num);
          num_failure_ += num;
          Equal();
        },
        [&] {
          cc_->IncrementNumUser(num);
          num_user_ += num;
          Equal();
        },
        [&] {
          cc_->DecrementNumInsert(num);
          num_insert_ -= num;
          Equal();
        },
        [&] {
          cc_->DecrementNumHit(num);
          num_hit_ -= num;
          Equal();
        },
        [&] {
          cc_->DecrementNumFailure(num);
          num_failure_ -= num;
          Equal();
        },
        [&] {
          cc_->DecrementNumUser(num);
          num_user_ -= num;
          Equal();
        },
        [&] {
          cc_->SetNumInsert(num);
          num_insert_ = num;
          Equal();
        },
        [&] {
          cc_->SetNumHit(num);
          num_hit_ = num;
          Equal();
        },
        [&] {
          cc_->SetNumFailure(num);
          num_failure_ = num;
          Equal();
        },
        [&] {
          cc_->SetNumUser(num);
          num_user_ = num;
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
    EXPECT_FALSE(json_old == json_new);
    // restore cc from the old state
    cc.FromJson(json_old);
    // assert the state is the same now
    EXPECT_EQ(json_old, cc.ToJson());
  }
}
}  // namespace terrier
