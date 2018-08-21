#pragma once
#include <condition_variable>  // NOLINT
#include <functional>
#include <mutex>  // NOLINT
#include <queue>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>
#include "common/container/concurrent_vector.h"
#include "common/object_pool.h"
#include "gtest/gtest.h"
#include "tbb/task_group.h"
#include "tbb/task_scheduler_init.h"

namespace terrier {
/**
 * Static utility class for common code for multi-threaded tests.
 */
class MultiThreadedTestUtil {
 public:
  ~MultiThreadedTestUtil() {
    std::unique_lock<std::mutex> lock(work_lock_);    // grab the lock
    shutdown_ = true;                                 // signal all the threads to shutdown
    work_cv_.notify_all();                            // wake up all the threads
    lock.unlock();                                    // free the lock
    for (auto &thread : thread_pool_) thread.join();  // wait for all the threads to terminate
  }

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
  static typename std::vector<T>::iterator UniformRandomElement(std::vector<T> *elems, Random *generator) {
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
  static typename std::vector<T>::const_iterator UniformRandomElement(const std::vector<T> &elems, Random *generator) {
    return elems.cbegin() + std::uniform_int_distribution(0, static_cast<int>(elems.size() - 1))(*generator);
  }

  /**
   * Execute the workload with the specified number of threads and wait for them to finish before
   * returning. This can be done repeatedly if desired. Threads will be reused.
   *
   * @param num_threads number of threads to use for execution
   * @param workload the task the thread should run
   * @param repeat the number of times this should be done.
   */
  void RunThreadsUntilFinish(uint32_t num_threads, const std::function<void(uint32_t)> &workload, uint32_t repeat = 1) {
    // ensure our thread pool has enough threads
    while (thread_pool_.size() < num_threads) AddThread();

    for (uint32_t i = 0; i < repeat; i++) {
      std::unique_lock<std::mutex> lock(work_lock_);  // grab the lock
      // add the jobs to the queue
      for (uint32_t j = 0; j < num_threads; j++) {
        work_pool_.emplace([j, &workload] { workload(j); });
        work_cv_.notify_one();
      }
      // wait for all the threads to finish
      finished_cv_.wait(lock, [this] { return busy_threads_ == 0 && work_pool_.empty(); });
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
  template <typename Random>
  static void InvokeWorkloadWithDistribution(std::vector<std::function<void()>> workloads,
                                             std::vector<double> probabilities, Random *generator,
                                             uint32_t repeat = 1) {
    PELOTON_ASSERT(probabilities.size() == workloads.size(), "Probabilities and workloads must have the same size.");
    std::discrete_distribution dist(probabilities.begin(), probabilities.end());
    for (uint32_t i = 0; i < repeat; i++) workloads[dist(*generator)]();
  }

 private:
  std::vector<std::thread> thread_pool_;
  std::queue<std::function<void()>> work_pool_;
  std::mutex work_lock_;
  std::condition_variable work_cv_;
  std::condition_variable finished_cv_;
  uint32_t busy_threads_ = 0;
  bool shutdown_ = false;

  void AddThread() {
    thread_pool_.emplace_back([this] {
      // keep the thread alive
      while (true) {
        // grab the lock
        std::unique_lock<std::mutex> lock(work_lock_);
        // try to get work
        work_cv_.wait(lock, [this] { return shutdown_ || !work_pool_.empty(); });
        // woke up! time to work or time to die?
        if (shutdown_) {
          break;
        }
        // grab the work
        ++busy_threads_;
        auto work = std::move(work_pool_.front());
        work_pool_.pop();
        // release the lock while we work
        lock.unlock();
        work();
        // we lock again to notify that we're done
        lock.lock();
        --busy_threads_;
        finished_cv_.notify_one();
      }
    });
  }
};

}  // namespace terrier
