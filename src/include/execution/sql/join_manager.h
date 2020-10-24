#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/sql.h"

namespace noisepage::execution::sql {

struct HashTableEntry;
class JoinHashTable;
class JoinHashTableVectorProbe;
class VectorProjectionIterator;

/**
 * A flexible join processor that dynamically adapts the ordering of a series of joins in order to
 * improve runtime performance. The manager is initially configured with a sequence of joins through
 * consecutive calls to InsertJoinStep(). Each invocation provides (1) the join hash table to probe
 * (2) the indexes of the join keys within the input batch (as they arrive) and (3) the matching
 * function used to evaluation the join.
 *
 * Once configured, the manage is "effectively" immutable and can be used to issue multi-step
 * joins. The process begins with an invocation to SetInputBatch() to prepare the join for a new
 * input batch of tuples. Then, all result tuples can be retrieved by looping while Next() returns
 * true:
 *
 * @code
 * JoinManager *jm = ...
 * VectorProjectionIterator *iter = ...
 * // Prepare the join, then loop results.
 * jm->SetInputBatch(iter);
 * while (jm->Next()) {
 *   HashTableEntry **matches[];
 *   jm->GetOutputBatch(matches);
 *   // Process matches
 * }
 * @endcode
 */
class JoinManager {
 public:
  /**
   * Create a new join manager using the provided opaque context.
   * @param exec_settings The execution settings to be used.
   * @param opaque_context An opaque context passed through each join function.
   */
  explicit JoinManager(const exec::ExecutionSettings &exec_settings, void *opaque_context);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(JoinManager);

  /**
   * Destructor.
   */
  ~JoinManager();

  /**
   * Insert a join-probe step into the manager. The step will probe the provided join hash table
   * @em probe_table and use the columns indexes in @em key_cols as join keys.
   * @param table The table to probe in this step.
   * @param key_cols The indexes of the columns in the
   * @param match_fn The join function.
   */
  void InsertJoinStep(const JoinHashTable &table, const std::vector<uint32_t> &key_cols,
                      FilterManager::MatchFn match_fn);

  /**
   * Set the next set of input into the join.
   * @param exec_ctx The execution context to be used.
   * @param input_vpi The next input batch into the join.
   */
  void SetInputBatch(exec::ExecutionContext *exec_ctx, VectorProjectionIterator *input_vpi);

  /**
   * Attempt to advance this fancy-ass multi-step join for the current input batch.
   * @return True if there is more output for the current input batch; false otherwise.
   */
  bool Next();

  /**
   * Get the next output from this fancy-ass multi-step join. It's assumed the matches vector is
   * large enough to store match vectors for each step.
   * @param[out] matches The array of match vectors, one for each join step.
   */
  void GetOutputBatch(const HashTableEntry **matches[]);

  /**
   * Perform a single join of the input batch against the provided join hash table.
   * @param input_batch The input into the join.
   * @param tid_list The list of TIDs in the input on which the join should be performed.
   * @param step_idx The index join to perform.
   */
  void PrepareSingleJoin(VectorProjection *input_batch, TupleIdList *tid_list, uint32_t step_idx);

 private:
  // Perform the initial join.
  bool AdvanceInitial(uint32_t idx);
  // Advance the probe at the given index.
  bool Advance(uint32_t idx);

 private:
  // The adaptive filter.
  FilterManager filter_;
  // The probe state for each join.
  std::vector<std::unique_ptr<JoinHashTableVectorProbe>> probes_;
  // Each iteration of the join filters different tuples. This saves the initial
  // set of active TIDs through iterations.
  TupleIdList input_tid_list_;
  // The current input batch.
  VectorProjectionIterator *curr_vpi_;
  // Has the initial join been performed?
  bool first_join_;
};

}  // namespace noisepage::execution::sql
