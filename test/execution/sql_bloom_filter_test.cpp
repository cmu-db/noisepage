#include <random>
#include <unordered_set>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/bloom_filter.h"
#include "execution/tpl_test.h"

namespace terrier::execution::sql::test {

class BloomFilterTest : public TplTest {
 public:
  BloomFilterTest() : memory_(nullptr) {}

  MemoryPool *Memory() { return &memory_; }

 protected:
  template <typename T>
  auto Hash(const T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return common::HashUtil::Hash(val);
  }

 private:
  MemoryPool memory_;
};

// NOLINTNEXTLINE
TEST_F(BloomFilterTest, Simple) {
  BloomFilter bf(Memory(), 10);

  bf.Add(Hash(10));
  EXPECT_TRUE(bf.Contains(Hash(10)));
  EXPECT_EQ(1u, bf.GetNumAdditions());

  bf.Add(20);
  EXPECT_TRUE(bf.Contains(Hash(10)));
  EXPECT_EQ(2u, bf.GetNumAdditions());
}

void GenerateRandom32(std::vector<uint32_t> &vals, uint32_t n) {  // NOLINT
  vals.resize(n);
  std::random_device rd;
  std::generate(vals.begin(), vals.end(), [&]() { return rd(); });
}

// Mix in elements from source into the target vector with probability p
template <typename T>
void Mix(std::vector<T> &target, const std::vector<T> &source, double p) {  // NOLINT
  TERRIER_ASSERT(target.size() > source.size(), "Bad sizes!");
  std::random_device random;
  std::mt19937 g(random());

  for (uint32_t i = 0; i < (p * target.size()); i++) {
    target[i] = source[g() % source.size()];
  }

  std::shuffle(target.begin(), target.end(), g);
}

// NOLINTNEXTLINE
TEST_F(BloomFilterTest, Comprehensive) {
  const uint32_t num_filter_elems = 10000;
  const uint32_t lookup_scale_factor = 100;

  // Create a vector of data to insert into the filter
  std::vector<uint32_t> insertions;
  GenerateRandom32(insertions, num_filter_elems);

  // The validation set. We use this to check false negatives.
  std::unordered_set<uint32_t> check(insertions.begin(), insertions.end());

  MemoryPool memory(nullptr);
  BloomFilter filter(&memory, num_filter_elems);

  // Insert everything
  for (const auto elem : insertions) {
    filter.Add(Hash(elem));
  }

  EXECUTION_LOG_TRACE("{}", filter.DebugString());

  for (auto prob_success : {0.00, 0.25, 0.50, 0.75, 1.00}) {
    std::vector<uint32_t> lookups;
    GenerateRandom32(lookups, num_filter_elems * lookup_scale_factor);
    Mix(lookups, insertions, prob_success);

    auto expected_found = static_cast<uint32_t>(prob_success * lookups.size());

    util::Timer<std::milli> timer;
    timer.Start();

    uint32_t actual_found = 0;
    for (const auto elem : lookups) {
      auto exists = filter.Contains(Hash(elem));

      if (!exists) {
        EXPECT_EQ(0u, check.count(elem));
      }

      actual_found += static_cast<uint32_t>(exists);
    }

    timer.Stop();

    UNUSED_ATTRIBUTE double fpr = (actual_found - expected_found) / static_cast<double>(lookups.size());
    UNUSED_ATTRIBUTE double probes_per_sec =
        static_cast<double>(lookups.size()) / timer.GetElapsed() * 1000.0 / 1000000.0;
    EXECUTION_LOG_TRACE("p: {:.2f}, {} M probes/sec, FPR: {:2.4f}, (expected: {}, actual: {})", prob_success,
                        probes_per_sec, fpr, expected_found, actual_found);
  }
}

}  // namespace terrier::execution::sql::test
