#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

// Forward declarations for tests.
namespace noisepage {
class BinderCorrectnessTest_SimpleFunctionCallTest_Test;
}  // namespace noisepage

namespace noisepage::execution::functions {
class FunctionContext;
}  // namespace noisepage::execution::functions

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

  /** The possible values for provolatile, which describe whether a function's result depends only on its input. */
  enum class ProVolatile : char {
    IMMUTABLE = 'i',  ///< Immutable: Always deliver the same result for the same inputs.
    STABLE = 's',     ///< Stable: For fixed inputs, results do not change within a scan.
    VOLATILE = 'v'    ///< Volatile: Results might change at any time, or the function has side-effects.
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
   * Column names of the form "PRO[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "PRO_[name]" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<proc_oid_t, uint32_t> PROOID{col_oid_t{1}};   // INTEGER (pkey) [proc_oid_t]
  static constexpr CatalogColumnDef<storage::VarlenEntry> PRONAME{col_oid_t{2}};  // VARCHAR (skey)
  static constexpr CatalogColumnDef<namespace_oid_t, uint32_t> PRONAMESPACE{
      col_oid_t{3}};  // INTEGER (skey) (fkey: pg_namespace)
  static constexpr CatalogColumnDef<language_oid_t, uint32_t> PROLANG{
      col_oid_t{4}};                                                        // INTEGER (skey) (fkey: pg_language)
  static constexpr CatalogColumnDef<double> PROCOST{col_oid_t{5}};          // DECIMAL (skey)
  static constexpr CatalogColumnDef<double> PROROWS{col_oid_t{6}};          // DECIMAL (skey)
  static constexpr CatalogColumnDef<type_oid_t> PROVARIADIC{col_oid_t{7}};  // INTEGER (skey) (fkey: pg_type)

  static constexpr CatalogColumnDef<bool> PROISAGG{col_oid_t{8}};               // BOOLEAN (skey)
  static constexpr CatalogColumnDef<bool> PROISWINDOW{col_oid_t{9}};            // BOOLEAN (skey)
  static constexpr CatalogColumnDef<bool> PROISSTRICT{col_oid_t{10}};           // BOOLEAN (skey)
  static constexpr CatalogColumnDef<bool> PRORETSET{col_oid_t{11}};             // BOOLEAN (skey)
  static constexpr CatalogColumnDef<char, uint8_t> PROVOLATILE{col_oid_t{12}};  // CHAR (skey)

  static constexpr CatalogColumnDef<uint16_t> PRONARGS{col_oid_t{13}};                // SMALLINT (skey)
  static constexpr CatalogColumnDef<uint16_t> PRONARGDEFAULTS{col_oid_t{14}};         // SMALLINT (skey)
  static constexpr CatalogColumnDef<type_oid_t, uint32_t> PRORETTYPE{col_oid_t{15}};  // INTEGER (skey) (fkey: pg_type)
  static constexpr CatalogColumnDef<storage::VarlenEntry> PROARGTYPES{
      col_oid_t{16}};  // VARBINARY (skey) [type_oid_t[]]
  static constexpr CatalogColumnDef<storage::VarlenEntry> PROALLARGTYPES{
      col_oid_t{17}};  // VARBINARY (skey) [type_oid_t[]]

  static constexpr CatalogColumnDef<storage::VarlenEntry> PROARGMODES{col_oid_t{18}};  // VARCHAR (skey)
  static constexpr CatalogColumnDef<storage::VarlenEntry> PROARGNAMES{col_oid_t{19}};  // VARBINARY (skey) [text[]]

  static constexpr CatalogColumnDef<storage::VarlenEntry> PROARGDEFAULTS{
      col_oid_t{20}};  // BIGINT (assumes 64-bit pointers)

  static constexpr CatalogColumnDef<storage::VarlenEntry> PROSRC{col_oid_t{21}};  // VARCHAR (skey)

  static constexpr CatalogColumnDef<storage::VarlenEntry> PROCONFIG{col_oid_t{22}};  // VARBINARY (skey) [text[]]

  static constexpr CatalogColumnDef<execution::functions::FunctionContext *> PRO_CTX_PTR{
      col_oid_t{23}};  // BIGINT (assumes 64-bit pointers)

  static constexpr uint8_t NUM_PG_PROC_COLS = 23;

  static constexpr std::array<col_oid_t, NUM_PG_PROC_COLS> PG_PRO_ALL_COL_OIDS = {
      PROOID.oid_,      PRONAME.oid_,        PRONAMESPACE.oid_, PROLANG.oid_,         PROCOST.oid_,
      PROROWS.oid_,     PROVARIADIC.oid_,    PROISAGG.oid_,     PROISWINDOW.oid_,     PROISSTRICT.oid_,
      PRORETSET.oid_,   PROVOLATILE.oid_,    PRONARGS.oid_,     PRONARGDEFAULTS.oid_, PRORETTYPE.oid_,
      PROARGTYPES.oid_, PROALLARGTYPES.oid_, PROARGMODES.oid_,  PROARGDEFAULTS.oid_,  PROARGNAMES.oid_,
      PROSRC.oid_,      PROCONFIG.oid_,      PRO_CTX_PTR.oid_};
};

}  // namespace noisepage::catalog::postgres
