#pragma once

#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/sql/tuple_id_list.h"

namespace noisepage::execution::exec {
class ExecutionContext;
class ExecutionSettings;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::sql {

class VectorProjection;
class VectorProjectionIterator;

/**
 * An adaptive filter that tries to discover the optimal filter configuration. Users build up the
 * filter in disjunctive normal form (DNF). Each disjunctive clause (i.e., clause in this context)
 * begins with a call to FilterManager::StartNewClause(). Conjunctive clauses (i.e., terms in this
 * context) are inserted into the active disjunction through repeated invocations of
 * FilterManager::InsertClauseTerm(). When finished, use FilterManager::Finalize(). The filter is
 * immutable after finalization.
 *
 * @code
 * FilterManager filter;
 * filter.StartNewClause();
 * filter.InsertClauseFilter(Clause0Term0);
 * // Remaining clauses and terms ...
 * filter.Finalize();
 *
 * // Once finalized, the filter can be applied to projections
 * VectorProjectionIterator *vpi = ...
 * filter.RunFilters(vpi);
 *
 * // At this point, iteration over the VPI will only hit selected, i.e., active, tuples.
 * for (; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
 *   // Only touch unfiltered tuples ...
 * }
 * @endcode
 */
class FilterManager {
 public:
  /**
   * A vectorized filter function.
   * The first argument is the execution context, used for execution settings.
   * The second argument is the projection to be filtered.
   * The third argument is the list of TIDs in the input projection that are active.
   * The fourth argument is an opaque context provided to the filter manager at construction time.
   */
  using MatchFn = void (*)(exec::ExecutionContext *, VectorProjection *, TupleIdList *, void *);

  /**
   * A clause in a multi-clause disjunctive normal form filter. A clause is composed of one or more
   * terms which can be safely reordered.
   */
  class Clause {
   public:
    /**
     * Create a new empty clause.
     * @param opaque_context The opaque context to run with.
     * @param stat_sample_freq The frequency to sample term runtime/selectivity stats.
     */
    explicit Clause(void *opaque_context, double stat_sample_freq);

    /**
     * Add a term to the clause.
     * @param term The term to add to this clause.
     */
    void AddTerm(MatchFn term);

    /**
     * Run the clause over the given input projection.
     * @param exec_ctx The execution context to run with.
     * @param input_batch The projection to filter.
     * @param tid_list The input TID list.
     */
    void RunFilter(exec::ExecutionContext *exec_ctx, VectorProjection *input_batch, TupleIdList *tid_list);

    /**
     * @return The number of times the clause has samples its terms' selectivities.
     * */
    uint32_t GetResampleCount() const { return sample_count_; }

    /**
     * @return The order of application of the terms in this clause the filter manage believes is
     *         currently optimal. This order may change over the course of its usage.
     */
    std::vector<uint32_t> GetOptimalTermOrder() const;

    /**
     * @return The total time spend in adaptive overhead when processing this filter clause. Time is
     *         reported in microseconds.
     */
    double GetOverheadMicros() const { return overhead_micros_; }

   private:
    // Indicates if statistics for all terms should be recollected.
    bool ShouldReRank();

    // A term in the clause.
    struct Term {
      // The index of the term when it was inserted into the clause.
      const uint32_t insertion_index_;
      // The function implementing the term.
      const MatchFn fn_;
      // The current rank.
      double rank_;
      // Create a new term with no rank.
      Term(uint32_t insertion_index, MatchFn term_fn) : insertion_index_(insertion_index), fn_(term_fn), rank_(0.0) {}
    };

   private:
    // An injected context object.
    void *opaque_context_;
    // The terms (i.e., factors) of the conjunction.
    std::vector<std::unique_ptr<Term>> terms_;
    // Temporary lists only used during re-sampling.
    TupleIdList input_copy_;
    TupleIdList temp_;
    // Frequency at which to sample stats, a number in the range [0.0, 1.0].
    double sample_freq_;
    // The number of times samples have been collected.
    uint32_t sample_count_;
    double overhead_micros_;
    // Random number generator.
    std::mt19937 gen_;
    std::uniform_real_distribution<double> dist_;
  };

  /**
   * Construct an empty filter.
   */
  explicit FilterManager(const exec::ExecutionSettings &exec_settings, bool adapt = true, void *context = nullptr);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(FilterManager);

  /**
   * Start a new clause.
   */
  void StartNewClause();

  /**
   * Insert a term in the current clause in the filter.
   * @param term A term.
   */
  void InsertClauseTerm(MatchFn term);

  /**
   * Insert a list of terms in the currently active clause in the filter.
   * @param terms The terms of the clause.
   */
  void InsertClauseTerms(std::initializer_list<MatchFn> terms);

  /**
   * Insert a vector of terms in the currently active clause in the filter.
   * @param terms The terms of the clause.
   */
  void InsertClauseTerms(const std::vector<MatchFn> &terms);

  /**
   * Run the filters over the given vector projection.
   * @param exec_ctx The execution context to run with.
   * @param input_batch The projection to filter.
   */
  void RunFilters(exec::ExecutionContext *exec_ctx, VectorProjection *input_batch);

  /**
   * Run all configured filters over the vector projection the input iterator is iterating over.
   * @param exec_ctx The execution context to run with.
   * @param input_batch The input projection iterator storing the vector projection to filter.
   */
  void RunFilters(exec::ExecutionContext *exec_ctx, VectorProjectionIterator *input_batch);

  /**
   * @return True if the filter is adaptive; false otherwise.
   */
  bool IsAdaptive() const { return adapt_; }

  /**
   * @return The number of clauses in this filter.
   */
  uint32_t GetClauseCount() const { return clauses_.size(); }

  /**
   * @return The ordering of clauses this manager believe is currently optimal. This ordering may
   *         change over the course of a manager's use.
   */
  std::vector<const Clause *> GetOptimalClauseOrder() const;

  /**
   * @return The total time spent in adaptive overhead when processing the filter. Time is reported
   *         in microseconds.
   */
  double GetTotalOverheadMicros() const {
    double overhead = 0;
    for (const auto &clause : clauses_) {
      overhead += clause->GetOverheadMicros();
    }
    return overhead;
  }

 private:
  // The execution settings to run with.
  const exec::ExecutionSettings &exec_settings_;
  // Flag indicating if the filter should try to optimize itself.
  bool adapt_;
  // An injected context object.
  void *opaque_context_;
  // The clauses in the filter.
  std::vector<std::unique_ptr<Clause>> clauses_;
  // The input and output TID lists, and a temporary list. These are used during
  // filter evaluation to carry TIDs across disjunctive clauses.
  TupleIdList input_list_;
  TupleIdList output_list_;
  TupleIdList tmp_list_;
};

}  // namespace noisepage::execution::sql
