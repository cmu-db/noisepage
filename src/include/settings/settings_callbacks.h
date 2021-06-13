#pragma once

#include <memory>

#include "common/action_context.h"
#include "common/managed_pointer.h"

namespace noisepage {
class DBMain;
}

namespace noisepage::settings {

/**
 * Utility class for defining callbacks for settings in settings_defs.h.
 */
class Callbacks {
 public:
  Callbacks() = delete;

  /**
   * Default callback for settings in the Settings Manager. Does nothing but set the state to SUCCESS.
   * @param old_value old settings value
   * @param new_value new settings value
   * @param db_main pointer to db_main
   * @param action_context pointer to the action context for this settings change
   */
  static void NoOp(void *old_value, void *new_value, DBMain *db_main,
                   common::ManagedPointer<common::ActionContext> action_context);

  /** Change the buffer segment pool size limit. */
  static void BufferSegmentPoolSizeLimit(void *old_value, void *new_value, DBMain *db_main,
                                         common::ManagedPointer<common::ActionContext> action_context);

  /** Change the buffer segment pool reuse limit. */
  static void BufferSegmentPoolReuseLimit(void *old_value, void *new_value, DBMain *db_main,
                                          common::ManagedPointer<common::ActionContext> action_context);

  /** Change the block store size limit. */
  static void BlockStoreSizeLimit(void *old_value, void *new_value, DBMain *db_main,
                                  common::ManagedPointer<common::ActionContext> action_context);

  /** Change the block store reuse limit. */
  static void BlockStoreReuseLimit(void *old_value, void *new_value, DBMain *db_main,
                                   common::ManagedPointer<common::ActionContext> action_context);

  /** Change the number of buffers the log manager uses. */
  static void WalNumBuffers(void *old_value, void *new_value, DBMain *db_main,
                            common::ManagedPointer<common::ActionContext> action_context);

  /** Change the number of buffers the log manager uses. */
  static void WalSerializationInterval(void *old_value, void *new_value, DBMain *db_main,
                                       common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for Logging component. */
  static void MetricsLogging(void *old_value, void *new_value, DBMain *db_main,
                             common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for TransactionManager component. */
  static void MetricsTransaction(void *old_value, void *new_value, DBMain *db_main,
                                 common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for GarbageCollector component. */
  static void MetricsGC(void *old_value, void *new_value, DBMain *db_main,
                        common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for Execution component. */
  static void MetricsExecution(void *old_value, void *new_value, DBMain *db_main,
                               common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for ExecutionEngine pipeline. */
  static void MetricsPipeline(void *old_value, void *new_value, DBMain *db_main,
                              common::ManagedPointer<common::ActionContext> action_context);

  /** Update the sampling interval for logging. */
  static void MetricsLoggingSampleRate(void *old_value, void *new_value, DBMain *db_main,
                                       common::ManagedPointer<common::ActionContext> action_context);

  /** Update the sampling interval for ExecutionEngine pipelines. */
  static void MetricsPipelineSampleRate(void *old_value, void *new_value, DBMain *db_main,
                                        common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for bind command. */
  static void MetricsBindCommand(void *old_value, void *new_value, DBMain *db_main,
                                 common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for execute command. */
  static void MetricsExecuteCommand(void *old_value, void *new_value, DBMain *db_main,
                                    common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable metrics collection for Query Trace component. */
  static void MetricsQueryTrace(void *old_value, void *new_value, DBMain *db_main,
                                common::ManagedPointer<common::ActionContext> action_context);

  /** Update the metrics output type being used by a metric component. */
  static void MetricsQueryTraceOutput(void *old_value, void *new_value, DBMain *db_main,
                                      common::ManagedPointer<common::ActionContext> action_context);

  /** Update the query execution mode in TrafficCop */
  static void CompiledQueryExecution(void *old_value, void *new_value, DBMain *db_main,
                                     common::ManagedPointer<common::ActionContext> action_context);

  /** Set the forecast sample limit. */
  static void ForecastSampleLimit(void *old_value, void *new_value, DBMain *db_main,
                                  common::ManagedPointer<common::ActionContext> action_context);

  /** Set the number of task manager threads. */
  static void TaskPoolSize(void *old_value, void *new_value, DBMain *db_main,
                           common::ManagedPointer<common::ActionContext> action_context);

  /** Enable or disable planning in Pilot thread. */
  static void PilotEnablePlanning(void *old_value, void *new_value, DBMain *db_main,
                                  common::ManagedPointer<common::ActionContext> action_context);

  /** Train the forecast model. */
  static void TrainForecastModel(void *old_value, void *new_value, DBMain *db_main,
                                 common::ManagedPointer<common::ActionContext> action_context);

  /** Train the interference model. */
  static void TrainInterferenceModel(void *old_value, void *new_value, DBMain *db_main,
                                     common::ManagedPointer<common::ActionContext> action_context);

  /** Train the OU model. */
  static void TrainOUModel(void *old_value, void *new_value, DBMain *db_main,
                           common::ManagedPointer<common::ActionContext> action_context);

#define SETTINGS_GENERATE_LOGGER_CALLBACK(component)                                    \
  /** Set the log level for the component. */                                           \
  static void LogLevelSet##component(void *old_value, void *new_value, DBMain *db_main, \
                                     common::ManagedPointer<common::ActionContext> action_context);

  SETTINGS_GENERATE_LOGGER_CALLBACK(binder)
  SETTINGS_GENERATE_LOGGER_CALLBACK(catalog)
  SETTINGS_GENERATE_LOGGER_CALLBACK(common)
  SETTINGS_GENERATE_LOGGER_CALLBACK(execution)
  SETTINGS_GENERATE_LOGGER_CALLBACK(index)
  SETTINGS_GENERATE_LOGGER_CALLBACK(messenger)
  SETTINGS_GENERATE_LOGGER_CALLBACK(metrics)
  SETTINGS_GENERATE_LOGGER_CALLBACK(modelserver)
  SETTINGS_GENERATE_LOGGER_CALLBACK(network)
  SETTINGS_GENERATE_LOGGER_CALLBACK(optimizer)
  SETTINGS_GENERATE_LOGGER_CALLBACK(parser)
  SETTINGS_GENERATE_LOGGER_CALLBACK(replication)
  SETTINGS_GENERATE_LOGGER_CALLBACK(selfdriving)
  SETTINGS_GENERATE_LOGGER_CALLBACK(settings)
  SETTINGS_GENERATE_LOGGER_CALLBACK(storage)
  SETTINGS_GENERATE_LOGGER_CALLBACK(transaction)

#undef SETTINGS_GENERATE_LOGGER_CALLBACK
};
}  // namespace noisepage::settings
