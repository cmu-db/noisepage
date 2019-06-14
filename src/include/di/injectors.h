#pragma once
#include "dependency/di_help.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"

namespace terrier::di {
/**
 * When applied, returns an injector that contains object bindings with the correct scope for the storage
 * engine (storage, logging, GC, transactions). Note that this does not contain bindings for constants
 * such as object pool sizes.
 */
auto storage_injector = []{
  return di::make_injector<StrictBinding>(
  );
};
}  // namespace terrier::di
