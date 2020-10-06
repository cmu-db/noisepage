#include "catalog/catalog_defs.h"

#include "common/strong_typedef_body.h"

namespace terrier::catalog {

STRONG_TYPEDEF_BODY(col_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(constraint_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(db_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(index_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(indexkeycol_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(namespace_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(language_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(proc_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(settings_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(table_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(tablespace_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(trigger_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(type_oid_t, uint32_t);
STRONG_TYPEDEF_BODY(view_oid_t, uint32_t);

}  // namespace terrier::catalog
