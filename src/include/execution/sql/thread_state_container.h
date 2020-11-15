#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "common/strong_typedef.h"
#include "execution/sql/memory_pool.h"

namespace noisepage::execution::sql {
/**
 * General Information:
 * -------------------
 * This class serves as a container for thread-local data structures. The <b>type</b> of the
 * structure is not known at compile-time, but is a dynamically generated type. To enable storing
 * opaque and generic structures, this class needs three ingredients:
 *
 * 1. We need to know the <b>size</b> of the structure, in bytes.
 * 2. We need a method to <b>initialize</b> the structure.
 * 3. We need a method to <b>destroy</b> this structure in the cases where it further allocates
 *    elements that must recursively destroyed.
 *
 * We refer to these three ingredients as a <b>type schema</b>. Users configure the container to
 * store a given type schema through ThreadStateContainer::Reset() and provide the necessary type
 * ingredients.
 *
 * ThreadStateContainer::Reset() does not allocate any memory itself. State allocation is deferred
 * to first access through ThreadStateContainer::AccessCurrentThreadState() where it is lazily
 * allocated and initialized using the type schema provided during reset. This is done to ensure
 * locality of the allocation to the thread it is associated to. An example usage is provided below:
 *
 * @code
 * struct YourStateStruct {
 *   // Stuff
 * };
 *
 * static void InitThreadState(void *context, void *memory) {
 *   // 'context' is an opaque pointers provided and bypassed in Reset();
 *   // 'memory' points to allocated, but uninitialized memory large enough to store thread state
 *   auto *your_state = reinterpret_cast<YourStateStruct *>(memory);
 *   // ...
 * }
 * static void DestroyThreadState(void *context, void *memory) {
 *   // 'context' is an opaque pointers provided and bypassed in Reset();
 *   // 'memory' points to some thread state whose memory must be destructed. Do not free the memory
 *   //          here, it will be done on your behalf. But, you must clean up any memory your state
 *   //          allocated.
 *   std::destroy_at(reinterpret_cast<YourStateStruct *>(memory));
 * }
 *
 * ThreadStateContainer thread_states;
 * thread_states.Reset(sizeof(YourStateStruct), InitThreadState, DestroyThreadState, nullptr);
 * @endcode
 *
 * Ingredients:
 * -----------
 * The type's initialization function accepts two void pointer arguments: an opaque "context" object
 * and a pointer to the uninitialized memory for the thread's state. It is the responsibility of the
 * initialization function to set up the thread's state by invoking the state's constructor, or
 * performing the construction manually.
 *
 * The destruction function is invoked when the container is destroyed, or when the container is
 * reset (through ThreadStateContainer::Reset()) to store a different type. The destruction function
 * must accept two void pointer arguments: an opaque "context" object and a pointer to the thread's
 * state. It is the responsibility of the destruction function to invoke the destructor of the
 * state. The memory underlying the thread state will be deallocated by the container!
 *
 * Reuse & Recycle:
 * ---------------
 * Containers can be reused multiple times to store different types of state objects. Each
 * invocation to ThreadStateContainer::Reset() configures it to store a new state object type by
 * cleaning up all previous state (invoking destruction) and setting up new state. Resetting the
 * container does not allocate memory. The container can be cleared <b>without</b> configuring new
 * state through ThreadStateContainer::Clear().
 *
 * Iteration:
 * ---------
 * All thread-local states stored in the container can be retrieved through
 * ThreadStateContainer::CollectThreadLocalStates(). Specific elements at an offset within each
 * thread-state can also be collected through
 * ThreadStateContainer::CollectThreadLocalStateElements(). Additionally, this container supports
 * generic iteration over states through ThreadStateContainer::ForEach() that will iterate over all
 * states and invoke a callback function to "use" the state.
 *
 */
class EXPORT ThreadStateContainer {
 public:
  /**
   * Function used to initialize a thread's local state upon first use.
   * The opaque context must contain the execution settings.
   * Arguments: pointer to opaque context, pointer to a thread-local state.
   */
  using InitFn = void (*)(void *, void *);

  /**
   * Function used to destroy a thread's local state.
   * Called if the container is destructed, or if the states are reset.
   * Arguments: pointer to opaque context, pointer to a thread-local state.
   */
  using DestroyFn = void (*)(void *, void *);

  /**
   * Function to iterate over all thread-local states in this container.
   * Arguments: pointer to opaque context, pointer to a thread-local state.
   */
  using IterateFn = void (*)(void *, void *);

  /**
   * Construct a container for all thread state using the given allocator.
   * @param memory The memory allocator to use to allocate thread states.
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
   * Clear all thread local data. This will destroy and clean up all allocated thread states.
   */
  void Clear();

  /**
   * Reset this container to store some state whose size is @em state_size in bytes. If an
   * initialization function is provided (i.e., @em init_fn), it will be invoked by the calling
   * thread upon first access to its state. Similarly, if a destruction function is provided, it
   * will be called when the thread state is requested to be destroyed either through another call
   * to ThreadStateContainer::Reset(), or when the container itself is destroyed. The @em ctx
   * pointer is passed into both initialization and destruction functions as a context.
   * @param state_size The size in bytes of the state.
   * @param init_fn The (optional) initialization function to call on first access. This is called
   *                in the thread that first accesses it.
   * @param destroy_fn The (optional) destruction function called to destroy the state.
   * @param ctx The (optional) context object to pass to both initialization and destruction
   *            functions.
   */
  void Reset(std::size_t state_size, InitFn init_fn, DestroyFn destroy_fn, void *ctx);

  /**
   * @return The calling thread's thread-local state.
   */
  byte *AccessCurrentThreadState();

  /**
   * @return The calling thread's thread-local state and interpret it as the templated type.
   */
  template <typename T>
  T *AccessCurrentThreadStateAs() {
    return reinterpret_cast<T *>(AccessCurrentThreadState());
  }

  /**
   * Collect all thread-local states and store pointers in the output container @em container.
   * @param container The output container to store the results.
   */
  void CollectThreadLocalStates(std::vector<byte *> *container) const;

  /**
   * Collect an element at offset @em element_offset from all thread-local states in this container
   * and store pointers in the output container.
   * @param[out] container The output container to store the results.
   * @param element_offset The offset of the element in the thread-local state.
   */
  void CollectThreadLocalStateElements(std::vector<byte *> *container, std::size_t element_offset) const;

  /**
   * Collect an element at offset @em element_offset from all thread-local states, interpret them as
   * @em T, and store pointers in the output container.
   *
   * NOTE: This is slightly inefficient because two copies are performed: one into a temporary
   *       vector and a copy into the output container. Don't use in performance-critical code.
   *
   * @tparam T The compile-time type to interpret the state element as
   * @param[out] container The output container to store the results.
   * @param element_offset The offset of the element in the thread-local state.
   */
  template <typename T>
  void CollectThreadLocalStateElementsAs(std::vector<T *> *container, const std::size_t element_offset) const {
    std::vector<byte *> tmp;
    CollectThreadLocalStateElements(&tmp, element_offset);
    container->clear();
    container->resize(tmp.size());
    for (uint64_t idx = 0; idx < tmp.size(); idx++) {
      (*container)[idx] = reinterpret_cast<T *>(tmp[idx]);
    }
  }

  /**
   * Apply the provided callback function to each thread-state element in this container.
   * Callback invocations are made serially.
   * @param ctx An opaque context object.
   * @param iterate_fn The function to call for each state.
   */
  void IterateStates(void *ctx, IterateFn iterate_fn) const;

  /**
   * Apply the provided callback function to each thread-state element in this container.
   * Callback invocations are made in parallel.
   * @param ctx An opaque context object.
   * @param iterate_fn The function to call for each state in parallel.
   */
  void IterateStatesParallel(void *ctx, IterateFn iterate_fn) const;

  /**
   * Apply a function on each thread local state. This is mostly for tests from C++.
   * @param f The function to apply.
   */
  template <typename T, typename F>
  void ForEach(F &&f) const {
    static_assert(std::is_invocable_v<F, T *>,
                  "Callback must be a single-argument functor accepting a pointer to state");
    std::vector<T *> states;
    CollectThreadLocalStateElementsAs<T>(&states, 0);
    for (const auto state : states) {
      f(state);
    }
  }

  /**
   * @return The number of allocated thread-local states in this container.
   */
  uint32_t GetThreadStateCount() const;

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
  // Memory allocator.
  MemoryPool *memory_;
  // Size of each thread's state.
  std::size_t state_size_;
  // The function to initialize a thread's local state upon first use.
  InitFn init_fn_;
  // The function to destroy a thread's local state when no longer needed.
  DestroyFn destroy_fn_;
  // An opaque context passed into the initialization and destruction functions.
  void *ctx_;

  // PIMPL
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace noisepage::execution::sql
