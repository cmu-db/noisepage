#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/postgres/pg_constraint.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "catalog/database_catalog.h"

namespace terrier {
class StorageTestUtil;
class TpccPlanTest;
}  // namespace terrier

namespace terrier::catalog {

// A UNION of metadata structure for each of the constraints
// stored in the pg_constraint class instances
using PGConstraintMetadata = union PGConstraintMetadata {
  struct UNIQUEMetadata;
  struct FKMetadata;
  struct CHECKMetadata;
  struct EXCLUSIONMetadata;
};

// the metadata for unique constraints
struct UNIQUEMetadata {
  std::vector<col_oid_t> unique_cols_;  // columns where the uniqueness apply
};

// the metadata for FK
using FKMetadata = struct FKMetadata {
  table_oid_t confrelid_;                           // the referenced table oid
  std::vector<col_oid_t> fk_srcs_;            // the column indcies in the current table for foreign key
  std::vector<col_oid_t> fk_refs_;  // the column indicies in the parent table that are reference for the foreign key
  postgres::FKActionType update_action_;
  postgres::FKActionType delete_action_;
};

// TODO: future modification for check support
struct CHECKMetadata {
  col_oid_t check_ref_table_oid_;          // thie referenced table oid
  std::vector<col_oid_t> check_cols_;      // columns where the check apply
  std::vector<col_oid_t> check_ref_cols_;  // columns where the check reference for checking
  size_t conbin_;                          // an internal representation of the expression, pg_node_tree
  std::string consrc_;                     // a human-readable representation of the expression
};

// TODO: future modification for exclusion support
struct EXCLUSIONMetadata {
  col_oid_t exclusion_ref_table_oid_;          // the referenced table oid
  std::vector<col_oid_t> exclusion_cols_;      // columns where the exclusion applies
  std::vector<col_oid_t> exclusion_ref_cols_;  // columns where the exclusion references
};

/**
 * The class datastructure for one pg_constraint instance
 * Including the attribute for characterizing a constraint on a table
 * Currently support NOT NULL, FOREIGN KEY, UNIQUE
 * Each pg_constraint
 *
 ********************* Multi Column Support *********************
 * This claos includes support for multi column senario:
 * CREATE TABLE example (
    a integer,
    b integer,
    c integer,
    UNIQUE (a, c)
);

CREATE TABLE t1 (
  a integer PRIMARY KEY,
  b integer,
  c integer,
  FOREIGN KEY (b, c) REFERENCES other_table (c1, c2)
);
 */
class PG_Constraint {
 public:

  /**
   * Constructor going from pg_constraint projected row of the  into constraint class instance
   */
   PG_Constraint(DatabaseCatalog *dbc, constraint_oid_t con_oid, std::string con_name, namespace_oid_t con_namespace_id,
                 postgres::ConstraintType con_type, bool con_deferrable, bool con_deferred, bool con_validated,
                 table_oid_t con_relid, index_oid_t con_index_id,
                 std::string con_col_varchar) {
      dbc_ = dbc;
      conoid_ = con_oid;
      conname_ = con_name;
      connamespaceid_ = con_namespace_id;
      contype_ = con_type;
      condeferrable_ = con_deferrable;
      condeferred_ = con_deferred;
      convalidated_ = con_validated;
      conrelid_ = con_relid;
      conindid_ = con_index_id;
      FillConCol(con_col_varchar);
   }
   void AddCheckConstraintMetaData(constraint_oid_t con_check) {
   }
   void AddExclusionConstraintMetadata(constraint_oid_t con_exc) {
   }


  friend class DatabaseCatalog;
  constraint_oid_t conoid_;  // oid of the constraint
  std::string conname_;
  namespace_oid_t connamespaceid_; /* OID of namespace containing constraint */
  postgres::ConstraintType contype_;         // type of the constraint

  bool condeferrable_;    /* deferrable constraint? */
  bool condeferred_;      /* deferred by default? */
  bool convalidated_;     /* Has the constraint been validated? Currently, can only be false for foreign keys */
  table_oid_t conrelid_;  // table this constraint applies to
  index_oid_t conindid_;  /* index supporting this constraint */
  std::vector<col_oid_t> concol_; /* the column id that this index applies to */
  catalog::DatabaseCatalog *dbc_;
  FKMetadata fkMetadata_;  // pther metadata depending on the constraint type

  friend class Catalog;
  friend class postgres::Builder;
  friend class terrier::TpccPlanTest;
 private:
  // fill the columns that the constraint is effective on: this is for UNIQUE, PK, NOTNULL
  void FillConCol(std::string con_col_str) {
    if (contype_ == postgres::ConstraintType::PRIMARY_KEY ||
        contype_ == postgres::ConstraintType::UNIQUE ||
        contype_ == postgres::ConstraintType::NOTNULL) {
      std::vector<std::string> raw_oid_vec = SplitString(con_col_str, postgres::VARCHAR_ARRAY_DELIMITER);
      concol_.reserve(raw_oid_vec.size());
      for (std::string col_oid : raw_oid_vec) {
        concol_.push_back(static_cast<col_oid_t>(stoi(col_oid)));
      }
    }
  }

  // python style spliting a string into vector according to a delimiting char
  std::vector<std::string> SplitString(std::string str, char delimiter = ' ') {
    std::replace(str.begin(), str.end(), delimiter, ' ');
    std::istringstream buf(str);
    std::istream_iterator<std::string> beg(buf), end;
    std::vector<std::string> tokens(beg, end);  // done!
    return tokens;
  }
};
}  // namespace terrier::catalog::postgres