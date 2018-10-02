
/*
 * test_suite.cpp
 *
 * This files includes basic testing infrastructure and function declarations
 *
 * by Ziqi Wang
 */
#pragma once

#include <cstring>
#include <string>
#include <unordered_map>
#include <random>
#include <map>
#include <fstream>
#include <iostream>

#include <pthread.h>

#include "bwtree/bwtree.h"
#include "bwtree/stx_btree/btree_multimap.h"
#include "bwtree/libcuckoo/cuckoohash_map.hh"
#include "bwtree/art/art.h"

#ifdef BWTREE_PELOTON
using namespace peloton::index;
#else
using namespace wangziqi2013::bwtree;
#endif

using namespace stx;

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
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }

  KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
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
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

using TreeType = BwTree<long int,
                        long int,
                        KeyComparator,
                        KeyEqualityChecker>;

using BTreeType = btree_multimap<long, long, KeyComparator>;
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
 
#define END_TEST do{ \
                print_flag = true; \
                delete t1; \
                \
                return 0; \
               }while(0);

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
void LaunchParallelTestID(TreeType *tree_p, 
                          uint64_t num_threads, 
                          Fn &&fn, 
                          Args &&...args) {
  std::vector<std::thread> thread_group;

  if(tree_p != nullptr) {
    // Update the GC array
    tree_p->UpdateThreadLocal(num_threads);
  }
  
  auto fn2 = [tree_p, &fn](uint64_t thread_id, Args ...args) {
    if(tree_p != nullptr) {
      tree_p->AssignGCID(static_cast<int>(thread_id));
    }
    
    fn(thread_id, args...);
    
    if(tree_p != nullptr) {
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
  if(tree_p != nullptr) {
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
  Random(IntType lower, IntType upper) :
    device{},
    engine{device()},
    dist{lower, upper}
  {}
  
  /*
   * Get() - Get a random number of specified type
   */
  inline IntType Get() {
    return dist(engine);
  }
  
  /*
   * operator() - Grammar sugar
   */
  inline IntType operator()() {
    return Get(); 
  }
};

/*
 * class SimpleInt64Random - Simple paeudo-random number generator 
 *
 * This generator does not have any performance bottlenect even under
 * multithreaded environment, since it only uses local states. It hashes
 * a given integer into a value between 0 - UINT64T_MAX, and in order to derive
 * a number inside range [lower bound, upper bound) we should do a mod and 
 * addition
 *
 * This function's hash method takes a seed for generating a hashing value,
 * together with a salt which is used to distinguish different callers
 * (e.g. threads). Each thread has a thread ID passed in the inlined hash
 * method (so it does not pose any overhead since it is very likely to be 
 * optimized as a register resident variable). After hashing finishes we just
 * normalize the result which is evenly distributed between 0 and UINT64_T MAX
 * to make it inside the actual range inside template argument (since the range
 * is specified as template arguments, they could be unfold as constants during
 * compilation)
 *
 * Please note that here upper is not inclusive (i.e. it will not appear as the 
 * random number)
 */
template <uint64_t lower, uint64_t upper>
class SimpleInt64Random {
 public:
   
  /*
   * operator()() - Mimics function call
   *
   * Note that this function must be denoted as const since in STL all
   * hashers are stored as a constant object
   */
  inline uint64_t operator()(uint64_t value, uint64_t salt) const {
    //
    // The following code segment is copied from MurmurHash3, and is used
    // as an answer on the Internet:
    // http://stackoverflow.com/questions/5085915/what-is-the-best-hash-
    //   function-for-uint64-t-keys-ranging-from-0-to-its-max-value
    //
    // For small values this does not actually have any effect
    // since after ">> 33" all its bits are zeros
    //value ^= value >> 33;
    value += salt;
    value *= 0xff51afd7ed558ccd;
    value ^= value >> 33;
    value += salt;
    value *= 0xc4ceb9fe1a85ec53;
    value ^= value >> 33;

    return lower + value % (upper - lower);
  }
};

/*
 * class Timer - Measures time usage for testing purpose
 */
class Timer {
 private:
  std::chrono::time_point<std::chrono::system_clock> start;
  std::chrono::time_point<std::chrono::system_clock> end;
  
 public: 
 
  /* 
   * Constructor
   *
   * It takes an argument, which denotes whether the timer should start 
   * immediately. By default it is true
   */
  Timer(bool start = true) : 
    start{},
    end{} {
    if(start == true) {
      Start();
    }
    
    return;
  }
  
  /*
   * Start() - Starts timer until Stop() is called
   *
   * Calling this multiple times without stopping it first will reset and
   * restart
   */
  inline void Start() {
    start = std::chrono::system_clock::now();
    
    return;
  }
  
  /*
   * Stop() - Stops timer and returns the duration between the previous Start()
   *          and the current Stop()
   *
   * Return value is represented in double, and is seconds elapsed between
   * the last Start() and this Stop()
   */
  inline double Stop() {
    end = std::chrono::system_clock::now();
    
    return GetInterval();
  }
  
  /*
   * GetInterval() - Returns the length of the time interval between the latest
   *                 Start() and Stop()
   */
  inline double GetInterval() const {
    std::chrono::duration<double> elapsed_seconds = end - start;
    return elapsed_seconds.count();
  }
};

/*
 * class Envp() - Reads environmental variables 
 */
class Envp {
 public:
  /*
   * Get() - Returns a string representing the value of the given key
   *
   * If the key does not exist then just use empty string. Since the value of 
   * an environmental key could not be empty string
   */
  static std::string Get(const std::string &key) {
    char *ret = getenv(key.c_str());
    if(ret == nullptr) {
      return std::string{""}; 
    } 
    
    return std::string{ret};
  }
  
  /*
   * operator() - This is called with an instance rather than class name
   */
  std::string operator()(const std::string &key) const {
    return Envp::Get(key);
  }
  
  /*
   * GetValueAsUL() - Returns the value by argument as unsigned long
   *
   * If the env var is found and the value is parsed correctly then return true 
   * If the env var is not found then retrun true, and value_p is not modified
   * If the env var is found but value could not be parsed correctly then
   *   return false and value is not modified 
   */
  static bool GetValueAsUL(const std::string &key, 
                           unsigned long *value_p) {
    const std::string value = Envp::Get(key);
    
    // Probe first character - if is '\0' then we know length == 0
    if(value.c_str()[0] == '\0') {
      return true;
    }
    
    unsigned long result;
    
    try {
      result = std::stoul(value);
    } catch(...) {
      return false; 
    } 
    
    *value_p = result;
    
    return true;
  }
};

/*
 * class Zipfian - Generates zipfian random numbers
 *
 * This class is adapted from: 
 *   https://github.com/efficient/msls-eval/blob/master/zipf.h
 *   https://github.com/efficient/msls-eval/blob/master/util.h
 *
 * The license is Apache 2.0.
 *
 * Usage:
 *   theta = 0 gives a uniform distribution.
 *   0 < theta < 0.992 gives some Zipf dist (higher theta = more skew).
 * 
 * YCSB's default is 0.99.
 * It does not support theta > 0.992 because fast approximation used in
 * the code cannot handle that range.
  
 * As extensions,
 *   theta = -1 gives a monotonely increasing sequence with wraparounds at n.
 *   theta >= 40 returns a single key (key 0) only. 
 */
class Zipfian {
 private:
  // number of items (input)
  uint64_t n;    
  // skewness (input) in (0, 1); or, 0 = uniform, 1 = always zero
  double theta;  
  // only depends on theta
  double alpha;  
  // only depends on theta
  double thres;
  // last n used to calculate the following
  uint64_t last_n;  
  
  double dbl_n;
  double zetan;
  double eta;
  uint64_t rand_state; 
 
  /*
   * PowApprox() - Approximate power function
   *
   * This function is adapted from the above link, which was again adapted from
   *   http://martin.ankerl.com/2012/01/25/optimized-approximative-pow-in-c-and-cpp/
   */
  static double PowApprox(double a, double b) {
    // calculate approximation with fraction of the exponent
    int e = (int)b;
    union {
      double d;
      int x[2];
    } u = {a};
    u.x[1] = (int)((b - (double)e) * (double)(u.x[1] - 1072632447) + 1072632447.);
    u.x[0] = 0;
  
    // exponentiation by squaring with the exponent's integer part
    // double r = u.d makes everything much slower, not sure why
    // TODO: use popcount?
    double r = 1.;
    while (e) {
      if (e & 1) r *= a;
      a *= a;
      e >>= 1;
    }
  
    return r * u.d;
  }
  
  /*
   * Zeta() - Computes zeta function
   */
  static double Zeta(uint64_t last_n, double last_sum, uint64_t n, double theta) {
    if (last_n > n) {
      last_n = 0;
      last_sum = 0.;
    }
    
    while (last_n < n) {
      last_sum += 1. / PowApprox((double)last_n + 1., theta);
      last_n++;
    }
    
    return last_sum;
  }
  
  /*
   * FastRandD() - Fast randum number generator that returns double
   *
   * This is adapted from:
   *   https://github.com/efficient/msls-eval/blob/master/util.h
   */
  static double FastRandD(uint64_t *state) {
    *state = (*state * 0x5deece66dUL + 0xbUL) & ((1UL << 48) - 1);
    return (double)*state / (double)((1UL << 48) - 1);
  }
 
 public:

  /*
   * Constructor
   *
   * Note that since we copy this from C code, either memset() or the variable
   * n having the same name as a member is a problem brought about by the
   * transformation
   */
  Zipfian(uint64_t n, double theta, uint64_t rand_seed) {
    assert(n > 0);
    if (theta > 0.992 && theta < 1) {
      fprintf(stderr, "theta > 0.992 will be inaccurate due to approximation\n");
    } else if (theta >= 1. && theta < 40.) {
      fprintf(stderr, "theta in [1., 40.) is not supported\n");
      assert(false);
    }
    
    assert(theta == -1. || (theta >= 0. && theta < 1.) || theta >= 40.);
    assert(rand_seed < (1UL << 48));
    
    // This is ugly, but it is copied from C code, so let's preserve this
    memset(this, 0, sizeof(*this));
    
    this->n = n;
    this->theta = theta;
    
    if (theta == -1.) { 
      rand_seed = rand_seed % n;
    } else if (theta > 0. && theta < 1.) {
      this->alpha = 1. / (1. - theta);
      this->thres = 1. + PowApprox(0.5, theta);
    } else {
      this->alpha = 0.;  // unused
      this->thres = 0.;  // unused
    }
    
    this->last_n = 0;
    this->zetan = 0.;
    this->rand_state = rand_seed;
    
    return;
  }
  
  /*
   * ChangeN() - Changes the parameter n after initialization
   *
   * This is adapted from zipf_change_n()
   */
  void ChangeN(uint64_t n) {
    this->n = n;
    
    return;
  }
  
  /*
   * Get() - Return the next number in the Zipfian distribution
   */
  uint64_t Get() {
    if (this->last_n != this->n) {
      if (this->theta > 0. && this->theta < 1.) {
        this->zetan = Zeta(this->last_n, this->zetan, this->n, this->theta);
        this->eta = (1. - PowApprox(2. / (double)this->n, 1. - this->theta)) /
                     (1. - Zeta(0, 0., 2, this->theta) / this->zetan);
      }
      this->last_n = this->n;
      this->dbl_n = (double)this->n;
    }
  
    if (this->theta == -1.) {
      uint64_t v = this->rand_state;
      if (++this->rand_state >= this->n) this->rand_state = 0;
      return v;
    } else if (this->theta == 0.) {
      double u = FastRandD(&this->rand_state);
      return (uint64_t)(this->dbl_n * u);
    } else if (this->theta >= 40.) {
      return 0UL;
    } else {
      // from J. Gray et al. Quickly generating billion-record synthetic
      // databases. In SIGMOD, 1994.
  
      // double u = erand48(this->rand_state);
      double u = FastRandD(&this->rand_state);
      double uz = u * this->zetan;
      
      if(uz < 1.) {
        return 0UL;
      } else if(uz < this->thres) {
        return 1UL;
      } else {
        return (uint64_t)(this->dbl_n *
                          PowApprox(this->eta * (u - 1.) + 1., this->alpha));
      }
    }
    
    // Should not reach here
    assert(false);
    return 0UL;
  }
   
};

#ifdef NO_USE_PAPI

/*
 * class CacheMeter - Placeholder for systems without PAPI
 */
class CacheMeter {
 public:
  CacheMeter() {};
  CacheMeter(bool) {};
  ~CacheMeter() {}
  void Start() {};
  void Stop() {};
  void PrintL3CacheUtilization() {};
  void PrintL1CacheUtilization() {};
  void GetL3CacheUtilization() {};
  void GetL1CacheUtilization() {};
};

#else

// This requires adding PAPI library during compilation
// The linking flag of PAPI is:
//   -lpapi
// To install PAPI under ubuntu please use the following command:
//   sudo apt-get install libpapi-dev
#include <papi.h>

/*
 * class CacheMeter - Measures cache usage using PAPI library
 *
 * This class is a high level encapsulation of the PAPI library designed for
 * more comprehensive profiling purposes, only using a small feaction of its
 * functionalities available. Also, the applicability of this library is highly
 * platform dependent, so please check whether the platform is supported before
 * using
 */
class CacheMeter {
 private:
  // This is a list of events that we care about
  int event_list[6] = {
      PAPI_LD_INS,       // Load instructions
      PAPI_L1_LDM,       // L1 load misses

      PAPI_SR_INS,       // Store instructions
      PAPI_L1_STM,       // L1 store misses

      PAPI_L3_TCA,       // L3 total cache access
      PAPI_L3_TCM,       // L3 total cache misses
  };

  // Use the length of the event_list to compute number of events we
  // are counting
  static constexpr int EVENT_COUNT = sizeof(event_list) / sizeof(int);

  // A list of results collected from the hardware performance counter
  long long counter_list[EVENT_COUNT];

  // Use this to print out event names
  const char *event_name_list[EVENT_COUNT] = {
      "PAPI_LD_INS",
      "PAPI_L1_LDM",
      "PAPI_SR_INS",
      "PAPI_L1_STM",
      "PAPI_L3_TCA",
      "PAPI_L3_TCM",
  };

  // The level of information we need to collect
  int level;

  /*
   * CheckEvent() - Checks whether the event exists in this platform
   *
   * This function wraps PAPI function in C++. Note that PAPI events are
   * declared using anonymous enum which is directly translated into int type
   */
  inline bool CheckEvent(int event) {
    int ret = PAPI_query_event(event);
    return ret == PAPI_OK;
  }

  /*
   * CheckAllEvents() - Checks all events that this object is going to use
   *
   * If the checking fails we just exit with error message indicating which one
   * failed
   */
  void CheckAllEvents() {
    // If any of the required events do not exist we just exit
    for(int i = 0;i < level;i++) {
      if(CheckEvent(event_list[i]) == false) {
        fprintf(stderr,
                "ERROR: PAPI event %s is not supported\n",
                event_name_list[i]);
        exit(1);
      }
    }

    return;
  }

 public:

  /*
   * CacheMeter() - Initialize PAPI and events
   *
   * This function starts counting if the argument passed is true. By default
   * it is false
   */
  CacheMeter(bool start=false, int p_level=2) :
      level{p_level} {
    int ret = PAPI_library_init(PAPI_VER_CURRENT);

    if (ret != PAPI_VER_CURRENT) {
      fprintf(stderr, "ERROR: PAPI library failed to initialize\n");
      exit(1);
    }

    // Initialize pthread support
    ret = PAPI_thread_init(pthread_self);
    if(ret != PAPI_OK) {
      fprintf(stderr, "ERROR: PAPI library failed to initialize for pthread\n");
      exit(1);
    }

    // If this does not pass just exit
    CheckAllEvents();

    // If we want to start the counter immediately just test this flag
    if(start == true) {
      Start();
    }

    return;
  }

  /*
   * Destructor
   */
  ~CacheMeter() {
    PAPI_shutdown();

    return;
  }

  /*
   * Start() - Starts the counter until Stop() is called
   *
   * If counter could not be started we just fail
   */
  void Start() {
    int ret = PAPI_start_counters(event_list, level);
    // Start counters
    if (ret != PAPI_OK) {
      fprintf(stderr,
              "ERROR: Failed to start counters using"
              " PAPI_start_counters() (%d)\n",
              ret);
      exit(1);
    }

    return;
  }

  /*
   * Stop() - Stops all counters, and dump their values inside the local array
   *
   * This function will clear all counters after dumping them into the internal
   * array of this object
   */
  void Stop() {
    // Use counter list to hold counters
    if (PAPI_stop_counters(counter_list, level) != PAPI_OK) {
      fprintf(stderr,
              "ERROR: Failed to start counters using PAPI_stop_counters()\n");
      exit(1);
    }

    // Store zero to all unused counters
    for(int i = level;i < EVENT_COUNT;i++) {
      counter_list[i] = 0LL;
    }

    return;
  }

  /*
   * GetL3CacheUtilization() - Returns L3 total cache accesses and misses
   *
   * These two values are returned in a tuple, the first element of which being
   * total cache accesses and the second element being L3 cache misses
   */
  std::pair<long long, long long> GetL3CacheUtilization() {
    return std::make_pair(counter_list[4], counter_list[5]);
  }

  /*
   * GetL1CacheUtilization() - Returns L1 cache utilizations
   */
  std::pair<long long, long long> GetL1CacheUtilization() {
    return std::make_pair(counter_list[0] + counter_list[2],
                          counter_list[1] + counter_list[3]);
  }

  /*
   * PrintL3CacheUtilization() - Prints L3 cache utilization
   */
  void PrintL3CacheUtilization() {
    // Return L3 total accesses and cache misses
    auto l3_util = GetL3CacheUtilization();

    std::cout << "    L3 total = " << l3_util.first << "; miss = " \
              << l3_util.second << "; hit ratio = " \
              << static_cast<double>(l3_util.first - l3_util.second) / \
                 static_cast<double>(l3_util.first) \
              << std::endl;

    return;
  }

  /*
   * PrintL1CacheUtilization() - Prints L1 cache utilization
   */
  void PrintL1CacheUtilization() {
    // Return L3 total accesses and cache misses
    auto l1_util = GetL1CacheUtilization();

    std::cout << "    LOAD/STORE total = " << l1_util.first << "; miss = " \
              << l1_util.second << "; hit ratio = " \
              << static_cast<double>(l1_util.first - l1_util.second) / \
                 static_cast<double>(l1_util.first) \
              << std::endl;

    return;
  }
};

#endif


/*
 * class Permutation - Generates permutation of k numbers, ranging from 
 *                     0 to k - 1
 *
 * This is usually used to randomize insert() to a data structure such that
 *   (1) Each Insert() call could hit the data structure
 *   (2) There is no extra overhead for failed insertion because all keys are
 *       unique
 */
template <typename IntType> 
class Permutation {
 private:
  std::vector<IntType> data;
  
 public:
  
  /*
   * Generate() - Generates a permutation and store them inside data
   */
  void Generate(size_t count, IntType start=IntType{0}) {
    // Extend data vector to fill it with elements
    data.resize(count);  

    // This function fills the vector with IntType ranging from
    // start to start + count - 1
    std::iota(data.begin(), data.end(), start);
    
    // The two arguments define a closed interval, NOT open interval
    Random<IntType> rand{0, static_cast<IntType>(count) - 1};
    
    // Then swap all elements with a random position
    for(size_t i = 0;i < count;i++) {
      IntType random_key = rand();
      
      // Swap two numbers
      std::swap(data[i], data[random_key]);
    }
    
    return;
  }
   
  /*
   * Constructor
   */
  Permutation() {}
  
  /*
   * Constructor - Starts the generation process
   */
  Permutation(size_t count, IntType start=IntType{0}) {
    Generate(count, start);
    
    return;
  }
  
  /*
   * operator[] - Accesses random elements
   *
   * Note that return type is reference type, so element could be
   * modified using this method 
   */
  inline IntType &operator[](size_t index) {
    return data[index];
  }
  
  inline const IntType &operator[](size_t index) const {
    return data[index];
  }
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
 * Performance test suite
 */
void TestStdMapInsertReadPerformance(int key_size);
void TestStdUnorderedMapInsertReadPerformance(int key_size);
void TestBTreeInsertReadPerformance(int key_size);
void TestBTreeMultimapInsertReadPerformance(int key_size);
void TestCuckooHashTableInsertReadPerformance(int key_size);
void TestBwTreeInsertReadDeletePerformance(TreeType *t, int key_num);
void TestBwTreeInsertReadPerformance(TreeType *t, int key_num);

// Multithreaded benchmark
void BenchmarkBwTreeRandInsert(int key_num, int thread_num);
void BenchmarkBwTreeSeqInsert(TreeType *t, int key_num, int thread_num);
void BenchmarkBwTreeSeqRead(TreeType *t, int key_num, int thread_num);
void BenchmarkBwTreeRandRead(TreeType *t, int key_num, int thread_num);
void BenchmarkBwTreeZipfRead(TreeType *t, int key_num, int thread_num);

// Benchmark for stx::btree
void BenchmarkBTreeSeqInsert(BTreeType *t, 
                             int key_num, 
                             int num_thread);
void BenchmarkBTreeSeqRead(BTreeType *t, 
                           int key_num,
                           int num_thread);
void BenchmarkBTreeRandRead(BTreeType *t, 
                            int key_num,
                            int num_thread);
void BenchmarkBTreeRandLocklessRead(BTreeType *t, 
                                    int key_num,
                                    int num_thread);
void BenchmarkBTreeZipfRead(BTreeType *t, 
                            int key_num,
                            int num_thread);
void BenchmarkBTreeZipfLockLessRead(BTreeType *t, 
                                    int key_num,
                                    int num_thread);

// Benchmark for ART              
void BenchmarkARTSeqInsert(ARTType *t, 
                           int key_num, 
                           int num_thread,
                           long int *array);
void BenchmarkARTSeqRead(ARTType *t, 
                         int key_num,
                         int num_thread);
void BenchmarkARTRandRead(ARTType *t, 
                          int key_num,
                          int num_thread);
void BenchmarkARTZipfRead(ARTType *t, 
                          int key_num,
                          int num_thread);

void TestBwTreeEmailInsertPerformance(BwTree<std::string, long int> *t, std::string filename);

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
void RandomBtreeMultimapInsertSpeedTest(size_t key_num);
void RandomCuckooHashMapInsertSpeedTest(size_t key_num);
void RandomInsertSpeedTest(TreeType *t, size_t key_num);
void RandomInsertSeqReadSpeedTest(TreeType *t, size_t key_num);
void SeqInsertRandomReadSpeedTest(TreeType *t, size_t key_num);
void InfiniteRandomInsertTest(TreeType *t);
void RandomInsertTest(uint64_t thread_id, TreeType *t);
void RandomInsertVerify(TreeType *t);

/*
 * Misc test suite
 */
void TestEpochManager(TreeType *t);

