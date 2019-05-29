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

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "count/hll.h"
#include "count/hll_limits.h"

using std::cerr;
using std::cout;
using std::endl;

using libcount::HLL;
using libcount::HLL_MIN_PRECISION;
using libcount::HLL_MAX_PRECISION;

// Hash function that hashes an integer to uint64_t, using 64 bits of SHA-1.
uint64_t hash(int i) {
  // Structure that is 160 bits wide used to extract 64 bits from a SHA-1.
  struct hashval {
    uint64_t high64;
    char low96[12];
  } hash;

  // Calculate the SHA-1 hash of the integer.
  SHA_CTX ctx;
  SHA1_Init(&ctx);
  SHA1_Update(&ctx, (unsigned char*)&i, sizeof(i));
  SHA1_Final((unsigned char*)&hash, &ctx);

  // Return 64 bits of the hash.
  return hash.high64;
}

struct TestResults {
  int precision;
  uint64_t size;
  uint64_t cardinality;
  uint64_t estimate;
  double percent_error;
};

std::ostream& operator<<(std::ostream& os, const TestResults& results) {
  char buffer[1000];
  snprintf(buffer, sizeof(buffer),
           "Precision:%2d  Size:%8lu  Actual:%8lu  Estimate:%8lu,  Error: "
           "%7.2lf %%",
           results.precision, results.size, results.cardinality,
           results.estimate, results.percent_error);
  os << buffer;
  return os;
}

int certify(int precision, uint64_t size, uint64_t cardinality,
            TestResults* results) {
  assert(results != NULL);
  // The precision must be 4..18 inclusive.
  if ((precision < HLL_MIN_PRECISION) || (precision > HLL_MAX_PRECISION)) {
    return EINVAL;
  }

  // The cardinality must be 1..size inclusive.
  if ((cardinality == 0) || (cardinality > size)) {
    return EINVAL;
  }

  // Initialize the results structure.
  results->precision = precision;
  results->size = size;
  results->cardinality = cardinality;
  results->estimate = 0;
  results->percent_error = 0.0;

  HLL* hll = HLL::Create(precision);
  if (!hll) {
    return EINVAL;
  }

  // Push 'size' elements through the counter. We are guaranteed exactly
  // 'cardinality' unique hash values.
  for (uint64_t i = 0; i < size; ++i) {
    hll->Update(hash(i % cardinality));
  }

  // Calculate the estimate
  results->estimate = hll->Estimate();

  // Calculate percentage difference.
  const double actual = static_cast<double>(results->cardinality);
  const double estimated = static_cast<double>(results->estimate);
  const double percent = ((estimated - actual) / actual) * 100.0;

  // Report percentage error.
  results->percent_error = percent;

  // Cleanup.
  delete hll;

  return 0;
}

int main(int argc, char* argv[]) {
  const int kMaxCardinality = 1000000;
  const int kMaxSize = kMaxCardinality * 10;
  size_t tests = 0;
  // For every precision level...
  for (int p = HLL_MIN_PRECISION; p <= HLL_MAX_PRECISION; ++p) {
    for (int c = 1; c <= kMaxCardinality; c *= 10) {
      for (int s = c; s <= kMaxSize; s *= 10) {
        TestResults results;
        int status = certify(p, s, c, &results);
        if (status < 0) {
          cerr << "certify: the certify() function returned an error" << endl;
          exit(EXIT_FAILURE);
        }
        cout << results << endl;
        ++tests;
      }
    }
  }
  return EXIT_SUCCESS;
}
