#include "execution/sql/sorter.h"

#include <algorithm>
#include <utility>

#include "ips4o/ips4o.hpp"

#include "execution/util/timer.h"

#include "loggers/execution_logger.h"

namespace tpl::sql {

Sorter::Sorter(util::Region *region, ComparisonFunction cmp_fn,
               u32 tuple_size) noexcept
    : tuple_storage_(region, tuple_size),
      cmp_fn_(cmp_fn),
      tuples_(region),
      sorted_(false) {}

Sorter::~Sorter() = default;

byte *Sorter::AllocInputTuple() noexcept {
  byte *ret = tuple_storage_.append();
  tuples_.push_back(ret);
  return ret;
}

byte *Sorter::AllocInputTupleTopK(UNUSED u64 top_k) noexcept {
  return AllocInputTuple();
}

void Sorter::AllocInputTupleTopKFinish(u64 top_k) noexcept {
  // If the number of buffered tuples is less than the bound, we're done
  if (tuples_.size() < top_k) {
    return;
  }

  // If the number of buffered tuples exactly equals the bound, let's build the
  // heap (from scratch for the first time).
  if (tuples_.size() == top_k) {
    BuildHeap();
    return;
  }

  //
  // We may need to reorder the heap. Check if the most recently inserted tuple
  // belongs in the heap.
  //

  // The most recent insert
  const byte *last_insert = tuples_.back();

  // The current top
  const byte *heap_top = tuples_.front();

  if (cmp_fn_(last_insert, heap_top) <= 0) {
    // The last inserted tuples belongs in the top-k. Swap it with the current
    // maximum and sift it down.
    tuples_.front() = last_insert;
    tuples_.pop_back();
    HeapSiftDown();
  } else {
    tuples_.pop_back();
  }
}

void Sorter::BuildHeap() {
  const auto compare = [this](const byte *left, const byte *right) {
    return cmp_fn_(left, right) < 0;
  };
  std::make_heap(tuples_.begin(), tuples_.end(), compare);
}

void Sorter::HeapSiftDown() {
  uint64_t size = tuples_.size();
  uint32_t idx = 0;

  const byte *top = tuples_[idx];

  while (true) {
    uint32_t child = (2 * idx) + 1;

    if (child >= size) {
      break;
    }

    if (child + 1 < size && cmp_fn_(tuples_[child], tuples_[child + 1]) < 0) {
      child++;
    }

    if (cmp_fn_(top, tuples_[child]) >= 0) {
      break;
    }
    // TODO(Amadou): Could save space by memcpying instead of just swapping
    // pointers (Too slow for large tuples?) i.e we can call
    // tuple_storage_.pop_back() only if we memcpy. Otherwise, the same memory
    // location is reused by subsequent calls AllocateInputTuple(), which will
    // overwrite existing values.
    std::swap(tuples_[idx], tuples_[child]);
    idx = child;
  }
}

void Sorter::Sort() {
  // Exit if the input tuples have already been sorted
  if (sorted_) {
    return;
  }

  // Exit if there are no input tuples
  if (tuples_.empty()) {
    return;
  }

  // Time it
  util::Timer<std::milli> timer;
  timer.Start();

  // Sort the sucker
  const auto compare = [this](const byte *left, const byte *right) {
    return cmp_fn_(left, right) < 0;
  };
  ips4o::sort(tuples_.begin(), tuples_.end(), compare);

  timer.Stop();

#ifndef NDEBUG
  auto rate = (static_cast<double>(tuples_.size()) / timer.elapsed()) / 1000.0;
  EXECUTION_LOG_DEBUG("Sorted %zu tuples in %.2f ms (%.2lf TPS)", tuples_.size(),
            timer.elapsed(), rate);
#endif

  // Mark complete
  sorted_ = true;
}

}  // namespace tpl::sql
