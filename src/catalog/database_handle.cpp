#include "catalog/database_handle.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/main_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

std::shared_ptr<DatabaseHandle> pg_database;

DatabaseHandle::DatabaseHandle() {}

void DatabaseHandle::BootStrap() {}
}  // namespace terrier::catalog
