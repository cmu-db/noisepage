#pragma once

#include <memory>
#include "storage/sql_table.h"
namespace terrier::catalog {

class NamespaceHandle {
 public:
  NamespaceHandle();
  ~NamespaceHandle();

 private:
  oid_t oid_;
  storage::SqlTable *pg_database_;
};

}  // namespace terrier::catalog
