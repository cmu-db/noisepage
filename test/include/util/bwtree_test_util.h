
/*
 * test_suite.cpp
 *
 * This files includes basic testing infrastructure and function declarations
 *
 * by Ziqi Wang
 */
#pragma once

#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "bwtree/art/art.h"
#include "bwtree/bwtree.h"
#include "bwtree/libcuckoo/cuckoohash_map.hh"
#include "bwtree/stx_btree/btree_multimap.h"

/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
 public:
  inline bool operator()(const int64_t k1, const int64_t k2) const { return k1 < k2; }

  explicit KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  // KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const int64_t k1, const int64_t k2) const { return k1 == k2; }

  explicit KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  // KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

using TreeType = wangziqi2013::bwtree::BwTree<int64_t, int64_t, KeyComparator, KeyEqualityChecker>;

using BTreeType = stx::btree_multimap<int64_t, int64_t, KeyComparator>;
using ARTType = art_tree;

using LeafRemoveNode = typename TreeType::LeafRemoveNode;
using LeafInsertNode = typename TreeType::LeafInsertNode;
using LeafDeleteNode = typename TreeType::LeafDeleteNode;
using LeafSplitNode = typename TreeType::LeafSplitNode;
using LeafMergeNode = typename TreeType::LeafMergeNode;
using LeafNode = typename TreeType::LeafNode;

using InnerRemoveNode = typename TreeType::InnerRemoveNode;
using InnerInsertNode = typename TreeType::InnerInsertNode;
using InnerDeleteNode = typename TreeType::InnerDeleteNode;
using InnerSplitNode = typename TreeType::InnerSplitNode;
using InnerMergeNode = typename TreeType::InnerMergeNode;
using InnerNode = typename TreeType::InnerNode;

using DeltaNode = typename TreeType::DeltaNode;

using NodeType = typename TreeType::NodeType;
using ValueSet = typename TreeType::ValueSet;
using NodeSnapshot = typename TreeType::NodeSnapshot;
using BaseNode = typename TreeType::BaseNode;

using Context = typename TreeType::Context;

/*
 * Common Infrastructure
 */

#define END_TEST       \
  do {                 \
    print_flag = true; \
    delete t1;         \
                       \
    return 0;          \
  } while (0);

/*
 * LaunchParallelTestID() - Starts threads on a common procedure
 *
 * This function is coded to be accepting variable arguments
 *
 * NOTE: Template function could only be defined in the header
 *
 * tree_p is used to allocate thread local array for doing GC. In the meanwhile
 * if it is nullptr then we know we are not using BwTree, so just ignore this
 * argument
 */
template <typename Fn, typename... Args>
void LaunchParallelTestID(TreeType *tree_p, uint64_t num_threads, Fn &&fn, Args &&... args) {
  std::vector<std::thread> thread_group;

  if (tree_p != nullptr) {
    // Update the GC array
    tree_p->UpdateThreadLocal(num_threads);
  }

  auto fn2 = [tree_p, &fn](uint64_t thread_id, Args... args) {
    if (tree_p != nullptr) {
      tree_p->AssignGCID(static_cast<int>(thread_id));
    }

    fn(thread_id, args...);

    if (tree_p != nullptr) {
      // Make sure it does not stand on the way of other threads
      tree_p->UnregisterThread(static_cast<int>(thread_id));
    }

    return;
  };

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread{fn2, thread_itr, std::ref(args...)});
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Restore to single thread mode after all threads have finished
  if (tree_p != nullptr) {
    tree_p->UpdateThreadLocal(1);
  }

  return;
}

/*
 * class Random - A random number generator
 *
 * This generator is a template class letting users to choose the number
 *
 * Note that this object uses C++11 library generator which is slow, and super
 * non-scalable.
 *
 * NOTE 2: lower and upper are closed interval!!!!
 */
template <typename IntType>
class Random {
 private:
  std::random_device device;
  std::default_random_engine engine;
  std::uniform_int_distribution<IntType> dist;

 public:
  /*
   * Constructor - Initialize random seed and distribution object
   */
  Random(IntType lower, IntType upper) : device{}, engine{device()}, dist{lower, upper} {}

  /*
   * Get() - Get a random number of specified type
   */
  inline IntType Get() { return dist(engine); }

  /*
   * operator() - Grammar sugar
   */
  inline IntType operator()() { return Get(); }
};

/*
 * Initialize and destroy btree
 */
TreeType *GetEmptyTree(bool no_print = false);
void DestroyTree(TreeType *t, bool no_print = false);

/*
 * Btree
 */
BTreeType *GetEmptyBTree();
void DestroyBTree(BTreeType *t);

void PrintStat(TreeType *t);
void PinToCore(size_t core_id);

/*
 * Basic test suite
 */
void InsertTest1(uint64_t thread_id, TreeType *t);
void InsertTest2(uint64_t thread_id, TreeType *t);
void DeleteTest1(uint64_t thread_id, TreeType *t);
void DeleteTest2(uint64_t thread_id, TreeType *t);

void InsertGetValueTest(TreeType *t);
void DeleteGetValueTest(TreeType *t);

extern int basic_test_key_num;
extern int basic_test_thread_num;

/*
 * Mixed test suite
 */
void MixedTest1(uint64_t thread_id, TreeType *t);
void MixedGetValueTest(TreeType *t);

extern std::atomic<size_t> mixed_insert_success;
extern std::atomic<size_t> mixed_delete_success;
extern std::atomic<size_t> mixed_delete_attempt;

extern int mixed_thread_num;
extern int mixed_key_num;

/*
 * Stress test suite
 */
void StressTest(uint64_t thread_id, TreeType *t);

/*
 * Iterator test suite
 */
void ForwardIteratorTest(TreeType *t, int key_num);
void BackwardIteratorTest(TreeType *t, int key_num);

/*
 * Random test suite
 */
void RandomInsertTest(uint64_t thread_id, TreeType *t);
void RandomInsertVerify(TreeType *t);

/*
 * Misc test suite
 */
void TestEpochManager(TreeType *t);
