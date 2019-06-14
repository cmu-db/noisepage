#pragma once
#include "di/di_help.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"

namespace terrier::di {
/**
 * When applied, returns an injector that contains object bindings with the correct scope for the storage
 * engine (storage, logging, GC, transactions). Note that this does not contain bindings for constants
 * such as object pool sizes. Unless those are supplied in the top level injector, the code will not compile.
 */
// TODO(Tianyu): Should we provide default values for constants here?
auto storage_injector = []{
  return di::make_injector<StrictBinding>(
      di::bind<storage::BlockStore>().in(di::terrier_module),
      di::bind<storage::RecordBufferSegmentPool>().in(di::terrier_module),
      di::bind<storage::LogManager>.in(di::terrier_module),
      di::bind<transaction::TransactionManager>().in(di::terrier_module)
  );
};
}  // namespace terrier::di
