#pragma once

#include <unordered_map>
#include <map>
#include <vector>
#include <string>
#include <tuple>
#include <utility>
#include <iostream>
#include <fstream>

#include "brain/forecast/workload_forecast.h"
#include "execution/exec_defs.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::brain {

/**
 * The garbage collector is responsible for processing a queue of completed transactions from the transaction manager.
 * Based on the contents of this queue, it unlinks the UndoRecords from their version chains when no running
 * transactions can view those versions anymore. It then stores those transactions to attempt to deallocate on the next
 * iteration if no running transactions can still hold references to them.
 */
class Pilot {
 public:
  /**
   * Constructor for the Garbage Collector that requires a pointer to the TransactionManager. This is necessary for the
   * PL to invoke the TM's function for handing off the completed transactions queue.
   * @param 
   * @param 
   */
  Pilot(uint64_t forecast_interval);

  /**
   * Deallocates transactions that can no longer be referenced by running transactions, and unlinks UndoRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's UndoRecords, while the second time around will allow the PL to free
   * the transaction if safe to do so. The only exception is read-only transactions, which can be deallocated in a
   * single PL pass.
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  // void PerformPilotLogic();

  std::unique_ptr<brain::WorkloadForecast> forecastor_;

 private:
  void LoadQueryTrace();
  void LoadQueryText();
  std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>> query_id_to_params_;
  std::unordered_map<execution::query_id_t, std::string> query_id_to_text_;
  std::unordered_map<std::string, execution::query_id_t> query_text_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions_;
  uint64_t num_sample_ {5};
  uint64_t forecast_interval_ {10000000};

};

}  // namespace terrier::storage
