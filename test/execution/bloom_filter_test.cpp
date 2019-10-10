#include <random>
#include <unordered_set>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/sql/bloom_filter.h"
#include "execution/util/hash.h"

namespace terrier::execution::sql::test {

class BloomFilterTest : public TplTest {};

template <typename F>
void GenerateRandom32(std::vector<uint32_t> *vals, uint32_t n, const F &f) {
  vals->resize(n);
  std::random_device random;
  auto genrand = [&random, &f]() {
    while (true) {
      auto r = random();
      if (f(r)) {
        return r;
      }
    }
  };
  std::generate(vals->begin(), vals->end(), genrand);
}

void GenerateRandom32(std::vector<uint32_t> *vals, uint32_t n) {
  GenerateRandom32(vals, n, [](auto r) { return true; });
}

// Mix in elements from source into the target vector with probability p
template <typename T>
void Mix(std::vector<T> *target, const std::vector<T> &source, double p) {
  TERRIER_ASSERT(target->size() > source.size(), "Bad sizes_!");
  std::random_device random;
  std::mt19937 g(random());

  for (uint32_t i = 0; i < (p * static_cast<double>(target->size())); i++) {
    (*target)[i] = source[g() % source.size()];
  }

  std::shuffle(target->begin(), target->end(), g);
}

// NOLINTNEXTLINE
TEST_F(BloomFilterTest, ComprehensiveTest) {
  const uint32_t num_filter_elems = 10000;
  const uint32_t lookup_scale_factor = 100;

  // Create a vector of data to insert into the filter
  std::vector<uint32_t> insertions;
  GenerateRandom32(&insertions, num_filter_elems);

  // The validation set. We use this to check false negatives.
  std::unordered_set<uint32_t> check(insertions.begin(), insertions.end());

  MemoryPool memory(nullptr);
  BloomFilter filter(&memory, num_filter_elems);
  for (const auto elem : insertions) {
    filter.Add(util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&elem), sizeof(elem)));
  }

  // All inserted elements **must** be present in filter
  for (const auto elem : insertions) {
    filter.Add(util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&elem), sizeof(elem)));
  }

  auto bits_per_elem = static_cast<double>(filter.GetSizeInBits()) / num_filter_elems;
  auto bit_set_prob = static_cast<double>(filter.GetTotalBitsSet()) / static_cast<double>(filter.GetSizeInBits());
  EXECUTION_LOG_INFO("Filter: {} elements, {} bits, {} bits/element, {} bits set (p={:.2f})", num_filter_elems,
                     filter.GetSizeInBits(), bits_per_elem, filter.GetTotalBitsSet(), bit_set_prob);

  for (auto prob_success : {0.00, 0.25, 0.50, 0.75, 1.00}) {
    std::vector<uint32_t> lookups;
    GenerateRandom32(&lookups, num_filter_elems * lookup_scale_factor);
    Mix(&lookups, insertions, prob_success);

    auto expected_found = static_cast<uint32_t>(prob_success * static_cast<double>(lookups.size()));

    util::Timer<std::milli> timer;
    timer.Start();

    uint32_t actual_found = 0;
    for (const auto elem : lookups) {
      auto exists = filter.Contains(util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&elem), sizeof(elem)));

      if (!exists) {
        EXPECT_EQ(0u, check.count(elem));
      }

      actual_found += static_cast<uint32_t>(exists);
    }

    timer.Stop();

    double fpr = (actual_found - expected_found) / static_cast<double>(lookups.size());
    double probes_per_sec = static_cast<double>(lookups.size()) / timer.Elapsed() * 1000.0 / 1000000.0;
    EXECUTION_LOG_INFO("p: {:.2f}, {} M probes/sec, FPR: {:2.4f}, (expected: {}, actual: {})", prob_success,
                       probes_per_sec, fpr, expected_found, actual_found);
  }
}

}  // namespace terrier::execution::sql::test
