#pragma once

#include <cmath>
#include <string>

#include "common/macros.h"
#include "count/hll.h"
#include "loggers/optimizer_logger.h"
#include "xxHash/xxh3.h"

namespace noisepage::optimizer {

/**
 * HyperLogLog (HLL) is an approximate data structure that generates cardinality estimates.
 * You give it a bunch of keys and then it estimates the number of unique keys
 * that it has seen over time.
 * The underlying implementation of the HLL is libcount.
 *
 * @see https://github.com/dialtr/libcount
 * @tparam KeyType the type of keys that this HLL will track.
 */
template <typename KeyType>
class HyperLogLog {
 public:
  /**
   * Constructor
   * Using a larger precision means that the estimated cardinalities will be more accurate
   * in exchange for higher computational and storage overhead.
   * The default precision in libcount is 9. That's probably good enough.
   * @param precision what precision level the HLL should record
   */
  explicit HyperLogLog(const int precision) : precision_{precision} {
    hll_ = libcount::HLL::Create(precision_).release();
  }

  /**
   * Deconstructor.
   */
  ~HyperLogLog() { delete hll_; }

  /**
   * Update the existence of the given key in the HLL. Note that we only
   * need to keep track that we saw it and not the number of times that we saw it.
   * @param key the key to apply to the HLL.
   */
  void Update(const KeyType &key) { Update(reinterpret_cast<const void *>(&key), sizeof(key)); }

  /**
   * Update the existence of the given key in the HLL. Note that we only
   * need to keep track that we saw it and not the number of times that we saw it.
   * @param key a pointer to the underlying storage of the key
   * @param length the length of the key.
   */
  void Update(const void *key, size_t length) { hll_->Update(XXH3_64bits(key, length)); }

  /**
   * Compute the bias-corrected estimate using the HyperLogLog++ algorithm.
   * @return
   */
  uint64_t EstimateCardinality() const { return hll_->Estimate(); }

  /**
   * Estimate relative error for HLL.
   * @return
   */
  double RelativeError() const {
    // This comes from the original HLL++ algorithm.
    auto register_count = 1 << precision_;
    NOISEPAGE_ASSERT(register_count > 0, "Invalid register count");
    return 1.04 / std::sqrt(register_count);
  }

 private:
  /**
   * The provided precision of the HLL instance.
   */
  const int precision_;

  /**
   * Libcount's HyperLogLog implementation
   */
  libcount::HLL *hll_;
};

}  // namespace noisepage::optimizer
