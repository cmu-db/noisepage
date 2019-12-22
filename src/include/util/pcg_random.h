// *Really* minimal PCG32 code / (c) 2014 M.E. O'Neill / pcg-random.org
// Licensed under Apache License 2.0 (NO WARRANTY, etc. see website)

#pragma once

namespace terrier::util {

class PCGRandomGenerator {
 public:
  // the state struct  for the generator
  typedef struct {
    uint64_t state;
    uint64_t inc;
  } pcg32_random_t;

  /**
   * Generate a random number
   * @param rng the random number generation state struct
   * @return a random number
   */
  static uint32_t pcg32_random_r(pcg32_random_t *rng) {
    uint64_t oldstate = rng->state;
    // Advance internal state
    rng->state = oldstate * 6364136223846793005ULL + (rng->inc | 1);
    // Calculate output function (XSH RR), uses old state for max ILP
    uint32_t xorshifted = ((oldstate >> 18u) ^ oldstate) >> 27u;
    uint32_t rot = oldstate >> 59u;
    return (xorshifted >> rot) | (xorshifted << ((-rot) & 31));
  }
};

}  // namespace terrier::util
