#pragma once
#include "common/strong_typedef.h"

namespace terrier::catalog {
STRONG_TYPEDEF(col_oid_t, uint32_t);
STRONG_TYPEDEF(database_oid_t, uint32_t);
STRONG_TYPEDEF(index_oid_t, uint32_t);
STRONG_TYPEDEF(table_oid_t, uint32_t);  // TODO(Matt): remove this -- DataTables shouldn't have oids (not a SQL concept)
STRONG_TYPEDEF(sqltable_oid_t, uint32_t);
}  // namespace terrier::catalog
