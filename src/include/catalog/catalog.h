#pragma once
#include <mutex>
#include <unordered_map>
#include <memory>
#include "common/strong_typedef.h"
#include "catalog/system_catalogs.h"

namespace terrier::catalog {

class Catalog {
 public:
  // Global Singleton
  Catalog();

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog();

 private:
  std::mutex catalog_mutex;

  // key: database oid
  // value: SystemCatalog object(including pg_table, pg_index and pg_attribute)
  std::unordered_map<oid_t, std::shared_ptr<SystemCatalogs>> catalog_map_;
};

}