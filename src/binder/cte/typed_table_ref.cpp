#include "binder/cte/typed_table_ref.h"

#include "parser/table_ref.h"

namespace noisepage::binder::cte {

TypedTableRef::TypedTableRef(common::ManagedPointer<parser::TableRef> table, RefType type)
    : table_{table}, type_{type} {}

}  // namespace noisepage::binder::cte
