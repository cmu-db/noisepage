#include "common/stat_registry.h"
#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>
#include "common/json.h"
#include "common/macros.h"
#include "gtest/gtest.h"

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
 *   uint64_t num_request
 */
#define NETWORK_MEMBERS(f) f(uint64_t, NumRequest)

DEFINE_PERFORMANCE_CLASS(NetworkCounter, NETWORK_MEMBERS)

// Test being able to register/deregister a performance counter to the registry
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(SimpleCorrectnessTest)) {
  terrier::common::StatisticsRegistry reg;
  CacheCounter cc;
  NetworkCounter nc;
  std::vector<std::string> current;
  std::vector<std::string> expected;

  // register under root
  reg.Register({}, &cc, this);
  EXPECT_EQ(reg.GetRegistrant({}, "CacheCounter"), this);
  expected = {"CacheCounter"};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);
  // get our counter back
  terrier::common::PerformanceCounter *pc = reg.GetPerformanceCounter({}, "CacheCounter");
  EXPECT_EQ(pc, &cc);
  // delete from root
  reg.Deregister({}, "CacheCounter", false);
  expected = {};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);

  // register under cache submodule
  reg.Register({"Cache"}, &cc, this);
  expected = {"Cache"};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);
  // check that we exist under cache submodule
  expected = {"CacheCounter"};
  current = reg.GetRegistryListing({"Cache"});
  EXPECT_TRUE(current == expected);
  // add something else to cache submodule
  reg.Register({"Cache"}, &nc, this);
  expected = {"CacheCounter", "NetworkCounter"};
  current = reg.GetRegistryListing({"Cache"});
  std::sort(current.begin(), current.end());
  EXPECT_TRUE(current == expected);
  // try deleting one thing from cache submodule
  reg.Deregister({"Cache"}, "CacheCounter", false);
  expected = {"NetworkCounter"};
  current = reg.GetRegistryListing({"Cache"});
  EXPECT_TRUE(current == expected);
  // try deleting another thing from cache submodule
  // since that becomes empty, should delete cache submodule itself
  reg.Deregister({"Cache"}, "NetworkCounter", false);
  expected = {};
  current = reg.GetRegistryListing();
  EXPECT_TRUE(current == expected);

  reg.Shutdown(false);
}

// Test registering multiple performance counters with the same name
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(MultipleNameTest)) {
  terrier::common::StatisticsRegistry reg;
  CacheCounter cc;
  CacheCounter cc2;
  std::vector<std::string> current;
  std::vector<std::string> expected;

  reg.Register({}, &cc, this);
  reg.Register({}, &cc2, this);
  expected = {"CacheCounter", "CacheCounter1"};
  current = reg.GetRegistryListing();
  std::sort(current.begin(), current.end());
  EXPECT_TRUE(current == expected);
  reg.Deregister({}, "CacheCounter", false);
  expected = {"CacheCounter1"};
  current = reg.GetRegistryListing();
  std::sort(current.begin(), current.end());
  EXPECT_TRUE(current == expected);

  reg.Shutdown(false);
}

// Test dumping statistics
// NOLINTNEXTLINE
TEST(StatRegistryTest, GTEST_DEBUG_ONLY(DumpTest)) {
  terrier::common::StatisticsRegistry reg;
  CacheCounter cc;
  CacheCounter cc2;
  std::vector<std::string> current;
  std::vector<std::string> expected;

  reg.Register({}, &cc, this);
  reg.Register({"Cache"}, &cc2, this);

  cc.IncNumFailure();
  cc2.IncNumHit();

  terrier::common::json json = terrier::common::json::parse(reg.DumpStats());
  EXPECT_EQ(json["CacheCounter"]["Counters"]["NumFailure"], 1);
  EXPECT_EQ(json["Cache"]["CacheCounter"]["Counters"]["NumHit"], 1);

  reg.Shutdown(false);
}
}  // namespace terrier
