#pragma once

#include <functional>
#include <random>
#include <vector>

#include "common/macros.h"

namespace terrier {
/**
 * Utility class for random element selection
 */
struct RandomTestUtil {
  RandomTestUtil() = delete;
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

  /**
   * Given a list of workloads and probabilities (must be of the same size), select
   * a random workload according to the probability to be run. This can be done
   * repeatedly if desired.
   *
   * @tparam Random type of random generator to use
   * @param workloads list of workloads to draw from
   * @param probabilities list of probabilities for each workload to be selected
   *    (if they don't sum up to one, they would be treated as weights
   *     i.e. suppose we have {w1, w2, ...} pr_n = w_n / sum(ws))
   * @param generator source of randomness to use
   * @param repeat the number of times this should be done.
   */
  template <typename Random>
  static void InvokeWorkloadWithDistribution(const std::vector<std::function<void()>> &workloads,
                                             std::vector<double> probabilities, Random *generator,
                                             uint32_t repeat = 1) {
    TERRIER_ASSERT(probabilities.size() == workloads.size(), "Probabilities and workloads must have the same size.");
    std::discrete_distribution dist(probabilities.begin(), probabilities.end());
    for (uint32_t i = 0; i < repeat; i++) workloads[dist(*generator)]();
  }
};

}  // namespace terrier
