#pragma once

#include <common/hash_util.h>
#include <inttypes.h>
#include <libcount/hll.h>
#include <murmur3/MurmurHash3.h>
#include <stdint.h>
#include <cmath>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"

namespace terrier::optimizer {

/**
 *
 * @tparam KeyType
 */
template <typename KeyType>
class HyperLogLog {
 public:
  /**
   *
   * @param precision
   */
  HyperLogLog(const int precision) : precision_{precision}, register_count_{1 << precision} {
    hll_ = libcount::HLL::Create(precision_);
  }

  /**
   * Deconstructor.
   */
  ~HyperLogLog() { delete hll_; }

  /**
   *
   * @param key
   */
  void Update(const KeyType &key) {
    // Throw the given key at murmur3 and get back a 128-bit hash.
    // We then update the HLL using the first 64-bits of the hash.
    // Andy tried using the second 64-bits and found that it produced
    // slightly less accurate estimations. He did not perform
    // a rigorous test of this though...
    uint64_t hash[2];
    murmur3::MurmurHash3_x64_128(reinterpret_cast<const void *>(&key),
                                 sizeof(key),
                                 0,
                                 reinterpret_cast<void *>(&hash));
    hll_->Update(hash[0]);
  }

  /**
   *
   * @return
   */
  uint64_t EstimateCardinality() const { return hll_->Estimate(); }

  /**
   * Estimate relative error for HLL.
   * @return
   */
  double RelativeError() const {
    TERRIER_ASSERT(register_count_ > 0, "Invalid register count");
    return 1.04 / std::sqrt(register_count_);
  }

 private:
  /**
   *
   */
  const int precision_;

  /**
   *
   */
  const int register_count_;

  /**
   * Libcount's HyperLogLog implementation
   */
  libcount::HLL *hll_;
};

}  // namespace terrier::optimizer