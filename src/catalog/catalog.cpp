#include "catalog/database_catalog.h"
#include "catalog/catalog.h"

namespace terrier::catalog {

/* Initialization of catalog, including:
 * 1) create database_catalog
 * create terrier database, create catalog tables, add them into
 * terrier database, insert columns into pg_attribute
 * 2) create necessary indexes, insert into pg_index
 * 3) insert terrier into pg_database, catalog tables into pg_table
 */
Catalog::Catalog() {
//  // create database_catalog (pg_database)
//  LOG_INFO("HELLO");
}
}