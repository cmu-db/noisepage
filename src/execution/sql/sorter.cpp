#include "execution/sql/sorter.h"

#include <algorithm>
#include <queue>
#include <utility>
#include <vector>

#include <tbb/tbb.h>  // NOLINT

#include "llvm/ADT/STLExtras.h"

#include "ips4o/ips4o.hpp"

#include "execution/sql/thread_state_container.h"
#include "execution/util/stage_timer.h"
#include "loggers/execution_logger.h"

namespace terrier::sql {

Sorter::Sorter(MemoryPool *memory, ComparisonFunction cmp_fn, u32 tuple_size)
    : tuple_storage_(tuple_size, MemoryPoolAllocator<byte>(memory)),
      owned_tuples_(memory),
      cmp_fn_(cmp_fn),
      tuples_(memory),
      sorted_(false) {}

Sorter::~Sorter() = default;

byte *Sorter::AllocInputTuple() {
  byte *ret = tuple_storage_.append();
  tuples_.push_back(ret);
  return ret;
}

byte *Sorter::AllocInputTupleTopK(UNUSED u64 top_k) { return AllocInputTuple(); }

void Sorter::AllocInputTupleTopKFinish(const u64 top_k) {
  // If the number of buffered tuples is less than top_k, we're done
  if (tuples_.size() < top_k) {
    return;
  }

  // If the number of buffered tuples matches tok_k, let's build the heap. Note,
  // this will only ever be done once!
  if (tuples_.size() == top_k) {
    BuildHeap();
    return;
  }

  //
  // We've buffered one more tuple than should be in the top-K, so we may need
  // to reorder the heap. Check if the most recently inserted tuple belongs in
  // the heap.
  //

  const byte *last_insert = tuples_.back();
  tuples_.pop_back();

  const byte *heap_top = tuples_.front();

  if (cmp_fn_(last_insert, heap_top) <= 0) {
    // The last inserted tuples belongs in the top-k. Swap it with the current
    // maximum and sift it down.
    tuples_.front() = last_insert;
    HeapSiftDown();
  }
}

void Sorter::BuildHeap() {
  const auto compare = [this](const byte *left, const byte *right) { return cmp_fn_(left, right) < 0; };
  std::make_heap(tuples_.begin(), tuples_.end(), compare);
}

void Sorter::HeapSiftDown() {
  const u64 size = tuples_.size();
  u32 idx = 0;

  const byte *top = tuples_[idx];

  while (true) {
    u32 child = (2 * idx) + 1;

    if (child >= size) {
      break;
    }

    if (child + 1 < size && cmp_fn_(tuples_[child], tuples_[child + 1]) < 0) {
      child++;
    }

    if (cmp_fn_(top, tuples_[child]) >= 0) {
      break;
    }

    std::swap(tuples_[idx], tuples_[child]);
    idx = child;
  }

  tuples_[idx] = top;
}

void Sorter::Sort() {
  // Exit if the input tuples have already been sorted
  if (is_sorted()) {
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
  const auto compare = [this](const byte *left, const byte *right) { return cmp_fn_(left, right) < 0; };
  ips4o::sort(tuples_.begin(), tuples_.end(), compare);

  timer.Stop();

  UNUSED double tps = (static_cast<double>(tuples_.size()) / timer.elapsed()) / 1000.0;
  EXECUTION_LOG_DEBUG("Sorted {} tuples in {} ms ({:.2f} tps)", tuples_.size(), timer.elapsed(), tps);

  // Mark complete
  sorted_ = true;
}

namespace {

// Structure we use to track a package of merging work.
template <typename IterType>
struct MergeWork {
  using Range = std::pair<IterType, IterType>;

  std::vector<Range> input_ranges;
  IterType destination;

  MergeWork(std::vector<Range> &&inputs, IterType dest) : input_ranges(std::move(inputs)), destination(dest) {}
};

}  // namespace

void Sorter::SortParallel(const ThreadStateContainer *thread_state_container, const u32 sorter_offset) {
  const auto comp = [this](const byte *left, const byte *right) { return cmp_fn_(left, right) < 0; };

  // -------------------------------------------------------
  // First, collect all non-empty thread-local sorters
  // -------------------------------------------------------

  std::vector<Sorter *> tl_sorters;
  thread_state_container->CollectThreadLocalStateElementsAs(&tl_sorters, sorter_offset);
  llvm::erase_if(tl_sorters, [](Sorter *const sorter) { return sorter->NumTuples() == 0; });

  // If there's nothing to sort, quit
  if (tl_sorters.empty()) {
    sorted_ = true;
    return;
  }

  // -------------------------------------------------------
  // 1. Make room in this sorter for all result tuples
  // -------------------------------------------------------

  util::StageTimer<std::milli> timer;
  timer.EnterStage("Resize Main Sorter");

  const u64 num_tuples =
      std::accumulate(tl_sorters.begin(), tl_sorters.end(), u64(0),
                      [](const u64 partial, const Sorter *const sorter) { return partial + sorter->NumTuples(); });
  tuples_.resize(num_tuples);

  timer.ExitStage();

  // -------------------------------------------------------
  // 2. Sort each thread-local sorter in parallel
  // -------------------------------------------------------

  timer.EnterStage("Parallel Sort Thread-Local Instances");

  tbb::task_scheduler_init sched;
  tbb::parallel_for_each(tl_sorters.begin(), tl_sorters.end(), [](Sorter *const sorter) { sorter->Sort(); });

  timer.ExitStage();

  // -------------------------------------------------------
  // 3. Compute splitters
  // -------------------------------------------------------

  timer.EnterStage("Compute Splitters");

  // Let B be the number of buckets we wish to decompose our input into, let N
  // be the number of sorter instances we have; then, 'splitters' is a [B-1 x N]
  // matrix where each row of the matrix contains a list of candidate splitters
  // found in each sorter, and each column indicates the set of splitter keys in
  // a single sorter. In other words, splitters[i][j] indicates the i-th
  // splitter key found in the j-th sorter instance.
  const auto num_buckets = static_cast<u32>(tl_sorters.size());
  std::vector<std::vector<const byte *>> splitters(num_buckets - 1);
  for (auto &splitter : splitters) {
    splitter.resize(tl_sorters.size());
  }

  for (u32 sorter_idx = 0; sorter_idx < tl_sorters.size(); sorter_idx++) {
    const Sorter *const sorter = tl_sorters[sorter_idx];
    auto part_size = sorter->NumTuples() / (splitters.size() + 1);
    for (u32 i = 0; i < splitters.size(); i++) {
      splitters[i][sorter_idx] = sorter->tuples_[(i + 1) * part_size];
    }
  }

  timer.ExitStage();

  // -------------------------------------------------------
  // 4. Compute work packages
  // -------------------------------------------------------

  timer.EnterStage("Compute Work Packages");

  // Where the merging work units are collected
  using SeqType = decltype(tuples_);
  using SeqTypeIter = SeqType::iterator;
  using MergeWorkType = MergeWork<SeqTypeIter>;
  std::vector<MergeWorkType> merge_work;

  {
    // This tracks the current position in the global output (i.e., this
    // sorter's tuples vector) where the next merge package will begin writing
    // results into. It begins at the front; as we generate merge packages, we
    // calculate the next position by computing the sizes of the merge packages.
    // We've already perfectly sized the output so this memory is allocated and
    // ready to be written to.
    auto write_pos = tuples_.begin();

    // This vector tracks, for each sorter, the position of the start of the
    // next input range. As we move through the splitters, we bump this pointer
    // so that we don't need to perform two binary searches to find the lower
    // and upper range around the splitter key.
    std::vector<SeqTypeIter> next_start(tl_sorters.size());

    for (u32 idx = 0; idx < splitters.size(); idx++) {
      // Sort the local separators and choose the median
      ips4o::sort(splitters[idx].begin(), splitters[idx].end(), comp);

      // Find the median-of-medians splitter key
      const byte *splitter = splitters[idx][tl_sorters.size() / 2];

      // The vector where we collect all input ranges that feed the merge work
      std::vector<MergeWork<SeqTypeIter>::Range> input_ranges;

      SeqTypeIter::difference_type part_size = 0;
      for (u32 sorter_idx = 0; sorter_idx < tl_sorters.size(); sorter_idx++) {
        // Get the [start,end) range in the current sorter such that
        // start <= splitter < end
        Sorter *const sorter = tl_sorters[sorter_idx];
        auto start = (idx == 0 ? sorter->tuples_.begin() : next_start[sorter_idx]);
        auto end = sorter->tuples_.end();
        if (idx < splitters.size() - 1) {
          end = std::upper_bound(start, end, splitter, comp);
        }

        // If the the range [start, end) is non-empty, push it in as work
        if (start != end) {
          input_ranges.emplace_back(start, end);
        }

        part_size += (end - start);
        next_start[sorter_idx] = end;
      }

      // Add work
      merge_work.emplace_back(std::move(input_ranges), write_pos);

      // Bump new write position
      write_pos += part_size;
    }
  }

  timer.ExitStage();

  // -------------------------------------------------------
  // 5. Parallel merge
  // -------------------------------------------------------

  timer.EnterStage("Parallel Merge");

  auto heap_cmp = [this](const MergeWorkType::Range &l, const MergeWorkType::Range &r) {
    return cmp_fn_(*l.first, *r.first) >= 0;
  };

  tbb::parallel_for_each(merge_work.begin(), merge_work.end(), [&heap_cmp](const MergeWork<SeqTypeIter> &work) {
    std::priority_queue<MergeWorkType::Range, std::vector<MergeWorkType::Range>, decltype(heap_cmp)> heap(
        heap_cmp, work.input_ranges);
    SeqTypeIter dest = work.destination;
    while (!heap.empty()) {
      auto top = heap.top();
      heap.pop();
      *dest++ = *top.first;
      if (top.first + 1 != top.second) {
        heap.emplace(top.first + 1, top.second);
      }
    }
  });

  timer.ExitStage();

  // -------------------------------------------------------
  // 6. Move thread-local data into this sorter
  // -------------------------------------------------------

  timer.EnterStage("Transfer Tuple Ownership");

  owned_tuples_.reserve(tl_sorters.size());
  for (auto *tl_sorter : tl_sorters) {
    owned_tuples_.emplace_back(std::move(tl_sorter->tuple_storage_));
    tl_sorter->tuples_.clear();
  }

  timer.ExitStage();

  // -------------------------------------------------------
  // Done
  // -------------------------------------------------------

  sorted_ = true;

  EXECUTION_LOG_DEBUG("Parallel Sort:");
  for (const auto &stage : timer.GetStages()) {
    EXECUTION_LOG_DEBUG("  {}: {.2f} ms", stage.name(), stage.time());
  }
}

void Sorter::SortTopKParallel(const ThreadStateContainer *thread_state_container, const u32 sorter_offset,
                              const u64 top_k) {
  // Parallel sort
  SortParallel(thread_state_container, sorter_offset);

  // Trim to top-K
  tuples_.resize(top_k);
}

}  // namespace terrier::sql
