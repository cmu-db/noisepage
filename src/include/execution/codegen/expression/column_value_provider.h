#pragma once

#include "catalog/catalog_defs.h"

namespace terrier::execution::codegen {

class WorkContext;

class ColumnValueProvider {
 public:
  /**
   * @return the child's output at the given index
   */
  virtual ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const = 0;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  virtual ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const = 0;
};

}  // namespace terrier::execution::codegen
