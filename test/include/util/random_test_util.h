#pragma once

#include <random>
#include <vector>

namespace terrier {
/**
 * Utility class for random element selection
 */
class RandomTestUtil {
 public:
  /**
   * Selects an element from the supplied vector uniformly at random, using the
   * given random generator.
   *
   * @tparam T type of elements in the vector
   * @tparam Random type of random generator to use
   * @param elems vector of elements to draw from
   * @param generator source of randomness to use
   * @return iterator to a randomly selected element
   */
  template <typename T, typename Random>
  static typename std::vector<T>::iterator UniformRandomElement(std::vector<T> *elems, Random *const generator) {
    return elems->begin() + std::uniform_int_distribution(0, static_cast<int>(elems->size() - 1))(*generator);
  }

  /**
   * Selects an element from the supplied constant vector uniformly at random, using the
   * given random generator.
   *
   * @tparam T type of elements in the vector
   * @tparam Random type of random generator to use
   * @param elems vector of elements to draw from
   * @param generator source of randomness to use
   * @return const iterator to a randomly selected element
   */
  template <class T, class Random>
  static typename std::vector<T>::const_iterator UniformRandomElement(const std::vector<T> &elems,
                                                                      Random *const generator) {
    return elems.cbegin() + std::uniform_int_distribution(0, static_cast<int>(elems.size() - 1))(*generator);
  }
};

}  // namespace terrier
