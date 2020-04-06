#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t CONSTRAINT_TABLE_OID = table_oid_t(61);
constexpr index_oid_t CONSTRAINT_OID_INDEX_OID = index_oid_t(62);
constexpr index_oid_t CONSTRAINT_NAME_INDEX_OID = index_oid_t(63);
constexpr index_oid_t CONSTRAINT_NAMESPACE_INDEX_OID = index_oid_t(64);
constexpr index_oid_t CONSTRAINT_TABLE_INDEX_OID = index_oid_t(65);
constexpr index_oid_t CONSTRAINT_INDEX_INDEX_OID = index_oid_t(66);
constexpr index_oid_t CONSTRAINT_FOREIGNTABLE_INDEX_OID = index_oid_t(67);

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t CONOID_COL_OID = col_oid_t(1);         // INTEGER (pkey)
constexpr col_oid_t CONNAME_COL_OID = col_oid_t(2);        // VARCHAR
constexpr col_oid_t CONNAMESPACE_COL_OID = col_oid_t(3);   // INTEGER (fkey: pg_namespace)
constexpr col_oid_t CONTYPE_COL_OID = col_oid_t(4);        // CHAR
constexpr col_oid_t CONDEFERRABLE_COL_OID = col_oid_t(5);  // BOOLEAN
constexpr col_oid_t CONDEFERRED_COL_OID = col_oid_t(6);    // BOOLEAN
constexpr col_oid_t CONVALIDATED_COL_OID = col_oid_t(7);   // BOOLEAN
constexpr col_oid_t CONRELID_COL_OID = col_oid_t(8);       // INTEGER (fkey: pg_class)
constexpr col_oid_t CONINDID_COL_OID = col_oid_t(9);       // INTEGER (fkey: pg_class)
constexpr col_oid_t CONFRELID_COL_OID = col_oid_t(10);     // INTEGER (fkey: pg_class)
constexpr col_oid_t CONBIN_COL_OID = col_oid_t(11);        // BIGINT (assumes 64-bit pointers)
constexpr col_oid_t CONSRC_COL_OID = col_oid_t(12);        // VARCHAR

constexpr uint8_t NUM_PG_CONSTRAINT_COLS = 12;

constexpr std::array<col_oid_t, NUM_PG_CONSTRAINT_COLS> PG_CONSTRAINT_ALL_COL_OIDS = {
    CONOID_COL_OID,        CONNAME_COL_OID,     CONNAMESPACE_COL_OID, CONTYPE_COL_OID,
    CONDEFERRABLE_COL_OID, CONDEFERRED_COL_OID, CONVALIDATED_COL_OID, CONRELID_COL_OID,
    CONINDID_COL_OID,      CONFRELID_COL_OID,   CONBIN_COL_OID,       CONSRC_COL_OID};

enum class ConstraintType : char {
  CHECK = 'c',
  FOREIGN_KEY = 'f',
  PRIMARY_KEY = 'p',
  UNIQUE = 'u',
  TRIGGER = 't',
  EXCLUSION = 'x',
};

/*
 * Identify constraint type for lookup purposes
 */
typedef enum ConstraintCategory {
  CONSTRAINT_RELATION,
  CONSTRAINT_DOMAIN,
  CONSTRAINT_ASSERTION /* for future expansion */
} ConstraintCategory;

/**
 * The class datastructure for the pg_constraint
 * Including the attribute for characterizing a constraint on a table for a single column
 */
class PG_Constraint {
 public:
  constraint_oid_t oid_;         // oid of the constraint
  namespace_oid_t namespace_id_; /* OID of namespace containing constraint */
  ConstraintType type_;          // type of the constraint
  table_oid_t table_id_;         // the table that this constraint applies to
  col_oid_t col_id_;             // the column that this constraint applies to

  // bool condeferrable_; /* deferrable constraint? */
  // bool condeferred_;   /* deferred by default? */
  // bool convalidated_;  /* constraint has been validated? */

  /*
   * conindid links to the index supporting the constraint, if any;
   * otherwise it's 0.  This is used for unique, primary-key, and exclusion
   * constraints, and less obviously for foreign-key constraints (where the
   * index is a unique index on the referenced relation's referenced
   * columns).  Notice that the index is on conrelid in the first case but
   * confrelid in the second.
   */
  // index_oid_t conindid_; /* index supporting this constraint */

  /*
   * If this constraint is on a partition inherited from a partitioned
   * table, this is the OID of the corresponding constraint in the parent.
   */
  // constraint_oid_t conparentid_;

  /************************ Foreign Key specific ******************************/
  // only applies to foreign keys, set to zero if other types of constraints

  table_oid_t fk_ref_table_id_;  // id of the table this constraint refers to
  col_oid_t fk_ref_col_id_;      // column that this constraint refers to
  bool fk_update_cascade_;       // true if cascade on update
  bool fk_delete_cascade_;       // true if cascade on deletion

  /**
   * default constructor for constraints other than FK constraint
   * set fk related parameters to zero or false to occupy space
   */
  PG_Constraint(constraint_oid_t con_id, namespace_oid_t namespace_id, ConstraintType con_type,
                table_oid_t con_table_id, col_oid_t col_id) {
    oid_ = con_id;
    namespace_id_ = namespace_id;
    type_ = con_type;
    table_id_ = con_table_id;
    col_id_ = col_id;

    // void FK attr
    fk_ref_col_id_ = 0;
    fk_ref_table_id_ = 0;
    fk_update_cascade_ = false;
    fk_delete_cascade_ = false;
  }

  /**
   * Constructor for FK constraint
   * requires all attribbutes including those for FK to be set
   */
  PG_Constraint(constraint_oid_t con_id, namespace_oid_t namespace_id, ConstraintType con_type,
                table_oid_t con_table_id, col_oid_t col_id, table_oid_t ref_table, col_oid_t ref_col,
                bool update_cascade, bool delete_cascade) {
    oid_ = con_id;
    namespace_id_ = namespace_id;
    type_ = con_type;
    table_id_ = con_table_id;
    col_id_ = col_id;

    // void FK attr
    fk_ref_col_id_ = ref_col;
    fk_ref_table_id_ = ref_table;
    fk_update_cascade_ = update_cascade;
    fk_delete_cascade_ = delete_cascade;
  }
};

/**
 * Manager class for pg_constraints
 * responsible for creating and registering a constraint in the system
 *  create this when database start 
 */
class PG_Constraint_Manager {
 public:
  // <oid_, constraint> map to get constraint from its oid
  std::unordered_map<constraint_oid_t, PG_Constraint *> con_id_map_;
  // the current highest oid available to be assigned to the new one
  constraint_oid_t cur_oid_;
  // queue used to record voided constraint id to be assigned to new constraints
  std::queue<constraint_oid_t> unused_con_oid_;

  PG_Constraint_Manager() {
    cur_oid_ = 1;  // starting with 1 as 0 is the default voided id
  }
  // create a new normal constraint
  PG_Constraint *GetNewConstraint(namespace_oid_t namespace_id, ConstraintType con_type, table_oid_t con_table_id,
                                  col_oid_t col_id) {
    constraint_oid_t tmp_oid;
    PG_Constraint *con;
    latch_.Lock();
    if (!unused_con_oid_.empty()) {
      tmp_oid = unused_con_oid_.front();
      unused_con_oid_.pop();
    } else {
      tmp_oid = cur_oid_;
      cur_oid_++;
    }
    con = new PG_Constraint(tmp_oid, namespace_id, con_type, con_table_id, col_id);
    con_id_map_.emplace(tmp_oid, con);
    latch_.Unlock();
    return con;
  }

  // create a new FK constraint
  PG_Constraint *GetNewFKConstraint(namespace_oid_t namespace_id, ConstraintType con_type, table_oid_t con_table_id,
                                    col_oid_t col_id, table_oid_t ref_table, col_oid_t ref_col, bool update_cascade,
                                    bool delete_cascade) {
    constraint_oid_t tmp_oid;
    PG_Constraint *con;
    latch_.Lock();
    if (!unused_con_oid_.empty()) {
      tmp_oid = unused_con_oid_.front();
      unused_con_oid_.pop();
    } else {
      tmp_oid = cur_oid_;
      cur_oid_++;
    }
    con = new PG_Constraint(tmp_oid, namespace_id, con_type, con_table_id, col_id, ref_table, ref_col, update_cascade,
                            delete_cascade);
    con_id_map_.emplace(tmp_oid, con);
    latch_.Unlock();
    return con;
  }

  // delete a constraint according to its oid
  // return false if failed
  // failed when constraint with current oid does not exists
  bool DeleteConstraint(constraint_oid_t oid) {
    PG_Constraint_Manager *con;
    bool res = false;
    latch_.Lock();
    if (con_id_map_.count() != 0) {
      con = con_id_map_[oid];
      con_id_map_.erase(oid);
      unused_con_oid_.push(oid);
      delete con;
      res = true;
    }
    latch.UnLock();
    return res;
  }

 private:
  // the latch for protecting constraint manager access
  mutable common::SpinLatch latch_;
};
}  // namespace terrier::catalog::postgres
