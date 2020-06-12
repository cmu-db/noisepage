// Copyright 2015 The libcount Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License. See the AUTHORS file for names of
// contributors.

#ifndef INCLUDE_COUNT_HLL_H_
#define INCLUDE_COUNT_HLL_H_

#include <assert.h>
#include <memory>
#include <stdint.h>
#include <algorithm>
#include <vector>
#include "hll_limits.h"
#include "utility.h"

namespace libcount {

class HLL {
 public:
  ~HLL();

  // Create an instance of a HyperLogLog++ cardinality estimator. Valid values
  // for precision are [4..18] inclusive, and govern the precision of the
  // estimate. Returns NULL on failure. In the event of failure, the caller
  // may provide a pointer to an integer to learn the reason.
  static std::unique_ptr<HLL> Create(int precision, int* error = 0);

  // Update the instance to record the observation of an element. It is
  // assumed that the caller uses a high-quality 64-bit hash function that
  // is free of bias. Empirically, using a subset of bits from a well-known
  // cryptographic hash function such as SHA1, is a good choice.
  void Update(uint64_t hash);

  // Update the instance to record the observation of multiple elements.
  void UpdateMany(const uint64_t *hashes, int num_hashes);
  void UpdateMany(const std::vector<uint64_t> &hashes);

  // Merge count tracking information from another instance into the object.
  // The object being merged in must have been instantiated with the same
  // precision. Returns 0 on success, EINVAL otherwise.
  int Merge(const HLL* other);

  // Compute the bias-corrected estimate using the HyperLogLog++ algorithm.
  uint64_t Estimate() const;

  // Reset the estimator
  void Reset();

 private:
  // No copying allowed
  HLL(const HLL& no_copy);
  HLL& operator=(const HLL& no_assign);

  // Constructor is private: we validate the precision in the Create function.
  explicit HLL(int precision);

  // Compute the raw estimate based on the HyperLogLog algorithm.
  double RawEstimate() const;

  // Return the number of registers equal to zero; used in LinearCounting.
  int RegistersEqualToZero() const;

  // Helper to calculate the index into the table of registers from the hash
  static int RegisterIndexOf(uint64_t hash, int precision) {
    return (hash >> (64 - precision));
  }

  // Helper to count the leading zeros (less the bits used for the reg. index)
  static uint8_t ZeroCountOf(uint64_t hash, int precision) {
    // Make a mask for isolating the leading bits used for the register index.
    const uint64_t ONE = 1;
    const uint64_t mask = ~(((ONE << precision) - ONE) << (64 - precision));

    // Count zeroes, less the index bits we're masking off.
    return (CountLeadingZeroes(hash & mask) - static_cast<uint8_t>(precision));
  }

 private:
  int precision_;
  int register_count_;
  uint8_t* registers_;
};

// Inlined here for performance
inline void HLL::Update(const uint64_t hash) {
  // Which register will potentially receive the zero count of this hash?
  const int index = RegisterIndexOf(hash, precision_);
  assert(index < register_count_);

  // Count the zeroes for the hash, and add one, per the algorithm spec.
  const uint8_t count = ZeroCountOf(hash, precision_) + 1;
  assert(count <= 64);

  // Update the appropriate register if the new count is greater than current.
  registers_[index] = std::max(registers_[index], count);
}

}  // namespace libcount

#endif  // INCLUDE_COUNT_HLL_H_
