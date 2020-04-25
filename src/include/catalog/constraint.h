#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/postgres/check_constraint.h"
#include "catalog/postgres/exclusion_constraint.h"
#include "catalog/postgres/fk_constraint.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier {
class StorageTestUtil;
class TpccPlanTest;
}  // namespace terrier

namespace terrier::catalog::postgres {

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
struct FKMetadata {
  col_oid_t confrelid_;                           // the referenced table oid
  std::vector<constraint_oid_t> fk_constraints_;  // the pks for each fk constraints
  std::vector<col_oid_t> fk_children_;            // the column indcies in the current table for foreign key
  std::vector<col_oid_t> fk_refs_;  // the column indicies in the parent table that are reference for the foreign key
  FKActionType update_action_;
  FKActionType delete_action_;
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
   * Constructor going from pg_constraint table dataformat into constraint class instance
   */
  PG_Constraint(constraint_oid_t con_oid, std::string con_name, namespace_oid_t con_namespace_id,
                ConstraintType con_type, bool con_deferrable, bool con_deferred, bool con_validated,
                table_oid_t con_relid, index_oid_t con_index_id, std::string con_frelid_varchar = "",
                std::string con_unique_col_varchar = "", constraint_oid_t check_id = INVALID_CONSTRAINT_OID,
                constraint_oid_t exclusion_id = INVALID_CONSTRAINT_OID) {
    conoid_ = con_oid;
    conname_ = con_name;
    connamespaceid_ = con_namespace_id;
    contype_ = con_type;
    condeferrable_ = con_deferrable;
    condeferred_ = con_deferred;
    convalidated_ = con_validated;
    conrelid_ = con_relid;
    conindid_ = con_index_id;
    InitializeMetaDataFromTableData(con_frelid_varchar, con_unique_col_varchar, check_id, exclusion_id);
  }

  /**
   * Constructor going from pg_constraint definition into constraint class instance
   */
  // PG_Constraint(constraint_oid_t con_oid, std::string con_name, namespace_oid_t con_namespace_id,
  //               ConstraintType con_type, bool con_deferrable, bool con_deferred, bool con_validated,
  //               table_oid_t con_relid, index_oid_t con_index_id, std::vector<col_oid_t> con_frelid = std::vector<col_oid_t>(),
  //               std::vector<col_oid_t> con_unique_col = std::vector<col_oid_t>(),constraint_oid_t check_id = INVALID_CONSTRAINT_OID,
  //               constraint_oid_t exclusion_id = INVALID_CONSTRAINT_OID) {
  //   conoid_ = con_oid;
  //   conname_ = con_name;
  //   connamespaceid_ = con_namespace_id;
  //   contype_ = con_type;
  //   condeferrable_ = con_deferrable;
  //   condeferred_ = con_deferred;
  //   convalidated_ = con_validated;
  //   conrelid_ = con_relid;
  //   conindid_ = con_index_id;
  //   InitializeMetaDataFromTableData(con_frelid_varchar, con_unique_col_varchar, check_id, exclusion_id);
  // }

 private:
  friend class DatabaseCatalog;
  constraint_oid_t conoid_;  // oid of the constraint
  std::string conname_;
  namespace_oid_t connamespaceid_; /* OID of namespace containing constraint */
  ConstraintType contype_;         // type of the constraint

  bool condeferrable_;    /* deferrable constraint? */
  bool condeferred_;      /* deferred by default? */
  bool convalidated_;     /* Has the constraint been validated? Currently, can only be false for foreign keys */
  table_oid_t conrelid_;  // table this constraint applies to
  index_oid_t conindid_;  /* index supporting this constraint */

  PGConstraintMetadata metadata_;  // pther metadata depending on the constraint type

  friend class Catalog;
  friend class postgres::Builder;
  friend class terrier::TpccPlanTest;

  void InitializeMetaDataFromTableData(std::string con_frelid_varchar, std::string con_unique_col_varchar,
                                       constraint_oid_t check_id, constraint_oid_t exclusion_id) {
    std::vector<std::string> raw_oid_vec;
    switch (this->contype_) {
      case (ConstraintType::CHECK):
        TERRIER_ASSERT(check_id != INVALID_CONSTRAINT_OID,
                       "CHECK constraint should be initialized with a valid constraint id");
        // TODO: Implement check support
        break;
      case (ConstraintType::FOREIGN_KEY):
        TERRIER_ASSERT(con_frelid_varchar.compare("") != 0, "FK should be initialized with none-empty string");
        raw_oid_vec = SplitString(con_frelid_varchar, VARCHAR_ARRAY_DELIMITER);

        break;
      case (ConstraintType::PRIMARY_KEY):
        // TODO: Implement support for PK
        break;
      case (ConstraintType::UNIQUE):
        TERRIER_ASSERT(con_unique_col_varchar.compare("") != 0, "UNIQUE should be initialized with none-empty string");
        break;
      case (ConstraintType::TRIGGER):
        // TODO: Implement trigger support
        break;
      case (ConstraintType::EXCLUSION):
        TERRIER_ASSERT(exclusion_id != INVALID_CONSTRAINT_OID,
                       "EXCLUSION constraint should be initialized with a valid constraint id");
        // TODO: Implement Exclusion support
        break;
      case (ConstraintType::NOTNULL):
        // nothing to do for NOTNULL
        break;
      default:
        break;
    }
  }
};

/**
 * Managing all the pg_constraints within the entire db
 * Exposes APIs for creating, enforcing, updating, deleting constraints
 * This is created in the database catalog
 */
class PG_Constraint_Manager {

};


// python style spliting a string into vector according to a delimiting char
std::vector<std::string> SplitString(std::string str, char delimiter = ',') {
  std::replace(str.begin(), str.end(), delimiter, ' ');
  std::istringstream buf(str);
  std::istream_iterator<std::string> beg(buf), end;
  std::vector<std::string> tokens(beg, end);  // done!

  return tokens;
}

}  // namespace terrier::catalog::postgres