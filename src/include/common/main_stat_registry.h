#pragma once

#include <memory>
#include "common/stat_registry.h"

extern std::shared_ptr<terrier::common::StatisticsRegistry> main_stat_reg;

void init_main_stat_reg();

void shutdown_main_stat_reg();

#define STAT_REGISTER(...) ::main_stat_reg->Register(__VA_ARGS__);

#define STAT_DEREGISTER(...) ::main_stat_reg->Deregister(__VA_ARGS__);

#define STAT_SHUTDOWN(...) ::main_stat_reg->Shutdown(__VA_ARGS__);

#define STAT_GET_PERFORMANCE_COUNTER(...) ::main_stat_reg->GetPerformanceCounter(__VA_ARGS__);

#define STAT_GET_REGISTRANT(...) ::main_stat_reg->GetRegistrant(__VA_ARGS__);

#define STAT_GET_REGISTRY_LISTING(...) ::main_stat_reg->GetRegistryListing(__VA_ARGS__);

#define STAT_DUMP_STATS(...) ::main_stat_reg->DumpStats(__VA_ARGS__);
