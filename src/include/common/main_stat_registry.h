#pragma once

#include <memory>
#include "common/stat_registry.h"

extern std::shared_ptr<terrier::common::StatisticsRegistry> main_stat_reg;

/**
 * Registers the given performance counter pc at the module specified in the main registry.
 * If the specified module contains a binding which conflicts with pc's name,
 * pc's name will be modified until the binding succeeds.
 *
 * @param module_path path to destination module
 * @param pc performance counter to be registered
 * @param registrant pointer to instance that is registering this counter
 */
#define STAT_REGISTER(...) ::main_stat_reg->Register(__VA_ARGS__);

/**
 * Deregister and remove the named performance counter from the module specified in the main registry.
 *
 * @param module_path path to destination module
 * @param name name of the performance counter to be deregistered
 * @param free_pc true if the performance counter should be freed, false otherwise
 * @return true if the performance counter was successfully removed, false otherwise
 */
#define STAT_DEREGISTER(...) ::main_stat_reg->Deregister(__VA_ARGS__);

/**
 * Shuts down the main registry.
 * @param free_pc true if the performance counters inside should be freed, false otherwise
 */
#define STAT_SHUTDOWN(...) ::main_stat_reg->Shutdown(__VA_ARGS__);

/**
 * Return a pointer to the performance counter at the specified module in the main registry.
 * @param module_path path to destination module
 * @param name name of the performance counter requested
 * @return pointer to said performance counter
 */
#define STAT_GET_PERFORMANCE_COUNTER(...) ::main_stat_reg->GetPerformanceCounter(__VA_ARGS__);

/**
 * Return a pointer to the registrant of the specified performance counter in the main registry.
 * @param module_path path to destination module
 * @param name name of the performance counter
 * @return pointer to whoever registered the performance counter
 */
#define STAT_GET_REGISTRANT(...) ::main_stat_reg->GetRegistrant(__VA_ARGS__);

/**
 * Return all the items registered at the specified module in the main registry.
 * @param module_path path to destination module
 * @return names of all items at the specified module
 */
#define STAT_GET_REGISTRY_LISTING(...) ::main_stat_reg->GetRegistryListing(__VA_ARGS__);

/**
 * Dumps out a formatted JSON string snapshot of the stats available at the specified module in the main registry.
 * @param module_path path to destination module
 * @param num_indents number of indents to use
 * @return a formatted JSON string representing a snapshot of all the stats at the specified module
 */
#define STAT_DUMP_STATS(...) ::main_stat_reg->DumpStats(__VA_ARGS__);
