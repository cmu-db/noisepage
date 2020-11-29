#pragma once

#include <array>

#include "catalog/catalog_defs.h"

// TODO(Deepayan): change back to lower value once all builtins are added
#define HIGHEST_BUILTIN_PROC_ID proc_oid_t(1000)
#define IS_BUILTIN_PROC(x) (x < HIGHEST_BUILTIN_PROC_ID)

// Forward declarations for tests.
namespace noisepage {
class BinderCorrectnessTest_SimpleFunctionCallTest_Test;
}  // namespace noisepage

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgProcImpl;

/** The OIDs used by the NoisePage version of pg_proc. */
class PgProc {
 public:
  /** The type of the argument to the procedure. */
  enum class ArgModes : char {
    IN = 'i',       ///< Input argument.
    OUT = 'o',      ///< Output argument.
    INOUT = 'b',    ///< Both input and output argument.
    VARIADIC = 'v'  ///< Variadic number of arguments.
  };

 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgProcImpl;

  friend class noisepage::BinderCorrectnessTest_SimpleFunctionCallTest_Test;

  static constexpr table_oid_t PRO_TABLE_OID = table_oid_t(81);
  static constexpr index_oid_t PRO_OID_INDEX_OID = index_oid_t(82);
  static constexpr index_oid_t PRO_NAME_INDEX_OID = index_oid_t(83);

  /*
   * Column names of the form "PRO[name]_COL_OID" are present in the PostgreSQL
   * catalog specification and columns of the form "PRO_[name]_COL_OID" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr col_oid_t PROOID_COL_OID = col_oid_t(1);   // INTEGER (pkey) [proc_oid_t]
  static constexpr col_oid_t PRONAME_COL_OID = col_oid_t(2);  // VARCHAR (skey)
  static constexpr col_oid_t PRONAMESPACE_COL_OID =
      col_oid_t(3);                                           // INTEGER (skey) (fkey: pg_namespace) [namespace_oid_t]
  static constexpr col_oid_t PROLANG_COL_OID = col_oid_t(4);  // INTEGER (skey) (fkey: pg_language) [language_oid_t]
  static constexpr col_oid_t PROCOST_COL_OID = col_oid_t(5);  // DECIMAL (skey)
  static constexpr col_oid_t PROROWS_COL_OID = col_oid_t(6);  // DECIMAL (skey)
  static constexpr col_oid_t PROVARIADIC_COL_OID = col_oid_t(7);  // INTEGER (skey) (fkey: pg_type) [type_oid_t]

  static constexpr col_oid_t PROISAGG_COL_OID = col_oid_t(8);      // BOOLEAN (skey)
  static constexpr col_oid_t PROISWINDOW_COL_OID = col_oid_t(9);   // BOOLEAN (skey)
  static constexpr col_oid_t PROISSTRICT_COL_OID = col_oid_t(10);  // BOOLEAN (skey)
  static constexpr col_oid_t PRORETSET_COL_OID = col_oid_t(11);    // BOOLEAN (skey)
  static constexpr col_oid_t PROVOLATILE_COL_OID = col_oid_t(12);  // BOOLEAN (skey)

  static constexpr col_oid_t PRONARGS_COL_OID = col_oid_t(13);         // TINYINT (skey)
  static constexpr col_oid_t PRONARGDEFAULTS_COL_OID = col_oid_t(14);  // TINYINT (skey)
  static constexpr col_oid_t PRORETTYPE_COL_OID = col_oid_t(15);       // INTEGER (skey) (fkey: pg_type) [type_oid_t]
  static constexpr col_oid_t PROARGTYPES_COL_OID = col_oid_t(16);      // VARBINARY (skey) [type_oid_t[]]
  static constexpr col_oid_t PROALLARGTYPES_COL_OID = col_oid_t(17);   // VARBINARY (skey) [type_oid_t[]]

  static constexpr col_oid_t PROARGMODES_COL_OID = col_oid_t(18);  // VARCHAR (skey)
  static constexpr col_oid_t PROARGNAMES_COL_OID = col_oid_t(19);  // VARBINARY (skey) [text[]]

  static constexpr col_oid_t PROARGDEFAULTS_COL_OID = col_oid_t(20);  // BIGINT (assumes 64-bit pointers)

  static constexpr col_oid_t PROSRC_COL_OID = col_oid_t(21);  // VARCHAR (skey)

  static constexpr col_oid_t PROCONFIG_COL_OID = col_oid_t(22);  // VARBINARY (skey) [text[]]

  static constexpr col_oid_t PRO_CTX_PTR_COL_OID = col_oid_t(23);  // BIGINT (assumes 64-bit pointers)

  static constexpr uint8_t NUM_PG_PROC_COLS = 23;

  static constexpr std::array<col_oid_t, NUM_PG_PROC_COLS> PG_PRO_ALL_COL_OIDS = {
      PROOID_COL_OID,      PRONAME_COL_OID,        PRONAMESPACE_COL_OID, PROLANG_COL_OID,         PROCOST_COL_OID,
      PROROWS_COL_OID,     PROVARIADIC_COL_OID,    PROISAGG_COL_OID,     PROISWINDOW_COL_OID,     PROISSTRICT_COL_OID,
      PRORETSET_COL_OID,   PROVOLATILE_COL_OID,    PRONARGS_COL_OID,     PRONARGDEFAULTS_COL_OID, PRORETTYPE_COL_OID,
      PROARGTYPES_COL_OID, PROALLARGTYPES_COL_OID, PROARGMODES_COL_OID,  PROARGDEFAULTS_COL_OID,  PROARGNAMES_COL_OID,
      PROSRC_COL_OID,      PROCONFIG_COL_OID,      PRO_CTX_PTR_COL_OID};
};

}  // namespace noisepage::catalog::postgres
