#pragma once
#include "di/di_help.h"
#include "storage/garbage_collector.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier::di {
/**
 * When applied, returns an injector that contains object bindings with the correct scope for the storage
 * engine (storage, logging, GC, transactions). Note that this does not contain bindings for constants
 * such as object pool sizes. Unless those are supplied in the top level injector, the code will not compile.
 */
// TODO(Tianyu): Should we provide default values for constants here?
auto storage_injector UNUSED_ATTRIBUTE = [] {
  return di::make_injector<StrictBindingPolicy>(
      di::bind<common::DedicatedThreadRegistry>().in(di::terrier_shared_module),
      di::bind<storage::BlockStore>().in(di::terrier_shared_module),
      di::bind<storage::RecordBufferSegmentPool>().in(di::terrier_shared_module),
      di::bind<storage::LogManager>.in(di::terrier_shared_module),
      di::bind<transaction::TransactionManager>().in(di::terrier_shared_module),
      di::bind<storage::GarbageCollector>().in(di::terrier_shared_module));
};
}  // namespace terrier::di
