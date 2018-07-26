#pragma once
#include <random>
#include <functional>
#include <thread>
#include "gtest/gtest.h"
#include "common/object_pool.h"
namespace terrier {
namespace testutil {

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
template<typename T, typename Random>
typename std::vector<T>::iterator UniformRandomElement(std::vector<T> &elems,
                                                       Random &generator) {
  return elems.begin()
      + std::uniform_int_distribution(0, (int) elems.size() - 1)(generator);
};

/**
 * Spawn up the specified number of threads with the workload and join them before
 * returning. This can be done repeatedly if desired.
 *
 * @param num_threads number of threads to spawn up
 * @param workload the task the thread should run
 * @param repeat the number of times this should be done.
 */
void RunThreadsUntilFinish(uint32_t num_threads,
                           const std::function<void(uint32_t)> &workload,
                           uint32_t repeat = 1) {
  for (uint32_t i = 0; i < repeat; i++) {
    std::vector<std::thread> threads;
    for (uint32_t j = 0; j < num_threads; j++)
      threads.emplace_back([j, &workload] { workload(j); });
    for (auto &thread : threads)
      thread.join();
  }
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
template<typename Random>
void InvokeWorkloadWithDistribution(std::vector<std::function<void()>> workloads,
                                    std::vector<double> probabilities,
                                    Random &generator,
                                    uint32_t repeat = 1) {
  PELOTON_ASSERT(probabilities.size() == workloads.size());
  std::discrete_distribution dist(probabilities.begin(), probabilities.end());
  for (uint32_t i = 0; i < repeat; i++)
    workloads[dist(generator)]();
}

#define TO_INT(p) reinterpret_cast<uintptr_t>(p)
/**
 * Check if memory address represented by val in [lower, upper)
 * @tparam A type of ptr
 * @tparam B type of ptr
 * @tparam C type of ptr
 * @param val value to check
 * @param lower lower bound
 * @param upper upper bound
 */
template<typename A, typename B, typename C>
void CheckInBounds(A *val, B *lower, C *upper) {
  EXPECT_GE(TO_INT(val), TO_INT(lower));
  EXPECT_LT(TO_INT(val), TO_INT(upper));
};

/**
 * Check if memory address represented by val not in [lower, upper)
 * @tparam A type of ptr
 * @tparam B type of ptr
 * @tparam C type of ptr
 * @param val value to check
 * @param lower lower bound
 * @param upper upper bound
 */
template<typename A, typename B, typename C>
void CheckNotInBounds(A *val, B *lower, C *upper) {
  EXPECT_TRUE(TO_INT(val) < TO_INT(lower) || TO_INT(val) >= TO_INT(upper));
};

/**
 * @tparam A type of ptr
 * @param ptr ptr to start from
 * @param bytes bytes to advance
 * @return  pointer that is the specified amount of bytes ahead of the given
 */
template<typename A>
A *IncrementByBytes(A *ptr, uint64_t bytes) {
  return reinterpret_cast<A *>(reinterpret_cast<byte *>(ptr) + bytes);
}
}
}