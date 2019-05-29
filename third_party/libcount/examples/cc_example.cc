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

#include <inttypes.h>
#include <openssl/sha.h>
#include <iostream>
#include "count/hll.h"

using libcount::HLL;
using std::cout;
using std::endl;

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

int main(int argc, char* argv[]) {
  const int kPrecision = 9;

  // Create an HLL object to track set cardinality.
  HLL* hll = HLL::Create(kPrecision);

  // Count 'kIterations' elements with 'kTrueCardinality' cardinality.
  const uint64_t kIterations = 1000000;
  const uint64_t kTrueCardinality = 100;
  for (uint64_t i = 0; i < kIterations; ++i) {
    hll->Update(hash(i % kTrueCardinality));
  }

  // Obtain the cardinality estimate.
  const uint64_t estimate = hll->Estimate();

  // Display results.
  cout << "actual cardinality:    " << kTrueCardinality << endl;
  cout << "estimated cardinality: " << estimate << endl;

  // Delete object.
  delete hll;

  return 0;
}
