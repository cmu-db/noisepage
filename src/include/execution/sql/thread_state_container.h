#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "common/strong_typedef.h"
#include "execution/sql/memory_pool.h"

namespace terrier::execution::sql {

/**
 * This class serves as a container for thread-local data required during query
 * execution. Users create an instance of this class and call @em Reset() to
 * configure it to store thread-local structures of an opaque type with a given
 * size.
 *
 * During query execution, threads can access their thread-local state by
 * calling @em AccessThreadStateOfCurrentThread(). Thread-local state is
 * constructed lazily upon first access and destroyed on a subsequent call to
 * @em Reset() or when the container itself is destroyed.
 */
class EXPORT ThreadStateContainer {
 public:
  /**
   * Function used to initialize a thread's local state upon first use
   */
  using InitFn = void (*)(void *, void *);

  /**
   * Function used to destroy a thread's local state if the container is
   * destructed, or if the states are reset.
   */
  using DestroyFn = void (*)(void *, void *);

  /**
   * Function to iterate over all thread-local states in this container.
   */
  using IterateFn = void (*)(void *, void *);

  /**
   * Construct a container for all thread state using the given allocator
   * @param memory The memory allocator to use to allocate thread states
   */
  explicit ThreadStateContainer(MemoryPool *memory);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ThreadStateContainer);

  /**
   * Destructor
   */
  ~ThreadStateContainer();

  /**
   * Clear all thread local data
   */
  void Clear();

  /**
   * Reset this container to store thread-local state whose size in bytes is
   * @em state_size given size, and use the optional initialization and
   * destruction functions @em init_fn and @em destroy_fn, respectively, to
   * initialize the thread-state exactly once upon first access, and destroy
   * the state upon last access. Both functions will be passed in the given
   * opaque context object @em ctx as the first argument.
   * @param state_size The size in bytes of the state
   * @param init_fn The (optional) initialization function to call on first
   *                access. This is called in the thread that first accesses it.
   * @param destroy_fn The (optional) destruction function called to destroy the
   *                   state.
   * @param ctx The (optional) context object to pass to both initialization and
   *            destruction functions.
   */
  void Reset(std::size_t state_size, InitFn init_fn, DestroyFn destroy_fn, void *ctx);

  /**
   * Access the calling thread's thread-local state.
   */
  byte *AccessThreadStateOfCurrentThread();

  /**
   * Access the calling thread's thread-local state and interpret it as the
   * templated type.
   */
  template <typename T>
  T *AccessThreadStateOfCurrentThreadAs() {
    return reinterpret_cast<T *>(AccessThreadStateOfCurrentThread());
  }

  /**
   * Collect all thread-local states and store pointers in the output container
   * @em container.
   * @param container The output container to store the results.
   */
  void CollectThreadLocalStates(std::vector<byte *> *container) const;

  /**
   * Collect an element at offset @em element_offset from all thread-local
   * states in this container and store pointers in the output container.
   * @param[out] container The output container to store the results.
   * @param element_offset The offset of the element in the thread-local state
   */
  void CollectThreadLocalStateElements(std::vector<byte *> *container, std::size_t element_offset) const;

  /**
   * Collect an element at offset @em element_offset from all thread-local
   * states, interpret them as @em T, and store pointers in the output
   * container.
   * NOTE: This is a little inefficient because it will perform two copies: one
   *       into a temporary vector, and a final copy into the output container.
   *       Don't use in performance-critical code.
   * @tparam T The compile-time type to interpret the state element as
   * @param[out] container The output container to store the results.
   * @param element_offset The offset of the element in the thread-local state
   */
  template <typename T>
  void CollectThreadLocalStateElementsAs(std::vector<T *> *container, std::size_t element_offset) const {
    std::vector<byte *> tmp;
    CollectThreadLocalStateElements(&tmp, element_offset);
    container->clear();
    container->resize(tmp.size());
    for (uint32_t idx = 0; idx < tmp.size(); idx++) {
      (*container)[idx] = reinterpret_cast<T *>(tmp[idx]);
    }
  }

  /**
   * Iterate over all thread-local states in this container invoking the given
   * callback function @em iterate_fn for each such state.
   * @param ctx An opaque context object.
   * @param iterate_fn The function to call for each state.
   */
  void IterateStates(void *ctx, IterateFn iterate_fn) const;

  /**
   * Apply a function on each thread local state. This is mostly for tests from
   * C++.
   * @tparam F Functor with signature void(const void*)
   * @param fn The function to apply
   */
  template <typename T, typename F>
  void ForEach(const F &fn) const {
    IterateStates(const_cast<F *>(&fn), [](void *ctx, void *raw_state) {
      auto *state = reinterpret_cast<T *>(raw_state);
      std::invoke(*reinterpret_cast<const F *>(ctx), state);
    });
  }

 private:
  /**
   * A handle to a single thread's state
   */
  class TLSHandle {
   public:
    // No-arg constructor
    TLSHandle();

    // Constructor
    explicit TLSHandle(ThreadStateContainer *container);

    // Destructor
    ~TLSHandle();

    // Thread-local state
    byte *State() { return state_; }

   private:
    // Handle to container
    ThreadStateContainer *container_{nullptr};
    // Owned memory
    byte *state_{nullptr};
  };

 private:
  // Memory allocator
  MemoryPool *memory_;
  // Size of each thread's state
  std::size_t state_size_;
  // The function to initialize a thread's local state upon first use
  InitFn init_fn_;
  // The function to destroy a thread's local state when no longer needed
  DestroyFn destroy_fn_;
  // An opaque context that passed into the constructor and destructor
  void *ctx_;

  // PIMPL
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace terrier::execution::sql
