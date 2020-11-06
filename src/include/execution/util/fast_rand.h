#pragma once

#include <random>

namespace noisepage::execution::util {

/**
 * Fast-random number generator based on Lehmer's technique.
 *
 * D. H. Lehmer.
 * Mathematical methods in large-scale computing units.
 * Proceedings of a Second Symposium on Large Scale Digital Calculating Machinery;
 * Annals of the Computation Laboratory, Harvard Univ. 26 (1951), pp. 141-146.
 *
 * P L'Ecuyer.
 * Tables of linear congruential generators of different sizes and good lattice structure.
 * Mathematics of Computation of the American Mathematical
 * Society 68.225 (1999): 249-260.
 */
class FastRand {
 public:
  /**
   * Create a new fast random number generator.
   */
  FastRand() {
    // Seed using a number generated from a slow PRNG.
    std::random_device rd;
    std::mt19937_64 mt(rd());
    std::uniform_int_distribution<uint64_t> dist;
    state_ = dist(mt);
  }

  /**
   * @return The next 64-bit psuedo-random number from this generator.
   */
  uint64_t Next() {
    state_ *= 0xda942042e4dd58b5ull;
    return state_ >> 64u;
  }

  /**
   * @return The next 64-bit psuedo-random number from this generator. Used by the STL random number
   *         engines.
   */
  uint64_t operator()() { return Next(); }

 private:
  uint128_t state_;
};

}  // namespace noisepage::execution::util
