#pragma once

#include "binder/bind_node_visitor.h"
#include "brain/forecast/workload_forecast.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "main/db_main.h"
#include "metrics/metrics_store.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "parser/expression/constant_value_expression.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace noisepage::brain {

/**
 * Utility class for helper functions
 */
class PilotUtil {
 public:
  /**
   * Executing forecasted queries and collect pipeline features for cost estimation to be used in action selection
   * @param db_main pointer to DBMain to access settings & metrics managers, transaction layer, and catalog layer
   * @param forecast pointer to object storing result of workload forecast
   */
  static void CollectPipelineFeatures(common::ManagedPointer<DBMain> db_main,
                                      common::ManagedPointer<brain::WorkloadForecast> forecast);
};

}  // namespace noisepage::brain
