#pragma once

#include <array>

#include "catalog/catalog_defs.h"

// TODO(Deepayan): change back to lower value once all builtins are added
#define HIGHEST_BUILTIN_PROC_ID proc_oid_t(1000)
#define IS_BUILTIN_PROC(x) (x < HIGHEST_BUILTIN_PROC_ID)

// Forward declarations for tests.
namespace noisepage {
class BinderCorrectnessTest_SimpleFunctionCallTest_Test;
namespace tpch {
class TPCHQuery;
}  // namespace tpch
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
  enum class ProArgModes : char {
    IN = 'i',       ///< Input argument.
    OUT = 'o',      ///< Output argument.
    INOUT = 'b',    ///< Both input and output argument.
    VARIADIC = 'v'  ///< Variadic number of arguments.
  };

 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgProcImpl;

  friend class noisepage::tpch::TPCHQuery;
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

  static constexpr proc_oid_t ATAN2_PRO_OID = proc_oid_t(84);
  static constexpr proc_oid_t ACOS_PRO_OID = proc_oid_t(85);
  static constexpr proc_oid_t ASIN_PRO_OID = proc_oid_t(86);
  static constexpr proc_oid_t ATAN_PRO_OID = proc_oid_t(87);
  static constexpr proc_oid_t COS_PRO_OID = proc_oid_t(88);
  static constexpr proc_oid_t SIN_PRO_OID = proc_oid_t(89);

  // TODO(tanujnay112) This overflows into the next internal oid range and will continue to do so
  static constexpr proc_oid_t TAN_PRO_OID = proc_oid_t(90);
  static constexpr proc_oid_t COT_PRO_OID = proc_oid_t(91);
  static constexpr proc_oid_t COSH_PRO_OID = proc_oid_t(92);
  static constexpr proc_oid_t SINH_PRO_OID = proc_oid_t(93);
  static constexpr proc_oid_t TANH_PRO_OID = proc_oid_t(94);
  static constexpr proc_oid_t EXP_PRO_OID = proc_oid_t(95);
  static constexpr proc_oid_t CEIL_PRO_OID = proc_oid_t(96);
  static constexpr proc_oid_t FLOOR_PRO_OID = proc_oid_t(97);
  static constexpr proc_oid_t TRUNCATE_PRO_OID = proc_oid_t(98);
  static constexpr proc_oid_t LOG10_PRO_OID = proc_oid_t(99);
  static constexpr proc_oid_t LOG2_PRO_OID = proc_oid_t(100);
  static constexpr proc_oid_t MOD_PRO_OID = proc_oid_t(101);
  static constexpr proc_oid_t INTMOD_PRO_OID = proc_oid_t(102);
  static constexpr proc_oid_t SQRT_PRO_OID = proc_oid_t(103);
  static constexpr proc_oid_t CBRT_PRO_OID = proc_oid_t(104);
  static constexpr proc_oid_t ROUND_PRO_OID = proc_oid_t(105);
  static constexpr proc_oid_t ROUND2_PRO_OID = proc_oid_t(106);
  static constexpr proc_oid_t POW_PRO_OID = proc_oid_t(107);
  static constexpr proc_oid_t LOWER_PRO_OID = proc_oid_t(108);
  static constexpr proc_oid_t UPPER_PRO_OID = proc_oid_t(109);
  static constexpr proc_oid_t STARTSWITH_PRO_OID = proc_oid_t(110);
  static constexpr proc_oid_t SUBSTR_PRO_OID = proc_oid_t(111);
  static constexpr proc_oid_t LEFT_PRO_OID = proc_oid_t(112);
  static constexpr proc_oid_t RIGHT_PRO_OID = proc_oid_t(113);
  static constexpr proc_oid_t REVERSE_PRO_OID = proc_oid_t(114);
  static constexpr proc_oid_t REPEAT_PRO_OID = proc_oid_t(115);
  static constexpr proc_oid_t TRIM_PRO_OID = proc_oid_t(116);
  static constexpr proc_oid_t TRIM2_PRO_OID = proc_oid_t(117);
  static constexpr proc_oid_t ABS_INT_PRO_OID = proc_oid_t(118);
  static constexpr proc_oid_t ABS_REAL_PRO_OID = proc_oid_t(119);
  static constexpr proc_oid_t ASCII_PRO_OID = proc_oid_t(120);
  static constexpr proc_oid_t CHR_PRO_OID = proc_oid_t(121);
  static constexpr proc_oid_t CHARLENGTH_PRO_OID = proc_oid_t(122);
  static constexpr proc_oid_t POSITION_PRO_OID = proc_oid_t(123);
  static constexpr proc_oid_t LENGTH_PRO_OID = proc_oid_t(124);
  static constexpr proc_oid_t INITCAP_PRO_OID = proc_oid_t(125);
  static constexpr proc_oid_t SPLIT_PART_PRO_OID = proc_oid_t(126);
  static constexpr proc_oid_t LPAD_PRO_OID = proc_oid_t(127);
  static constexpr proc_oid_t LPAD2_PRO_OID = proc_oid_t(128);
  static constexpr proc_oid_t LTRIM2ARG_PRO_OID = proc_oid_t(129);
  static constexpr proc_oid_t LTRIM1ARG_PRO_OID = proc_oid_t(130);
  static constexpr proc_oid_t RPAD_PRO_OID = proc_oid_t(131);
  static constexpr proc_oid_t RPAD2_PRO_OID = proc_oid_t(132);
  static constexpr proc_oid_t RTRIM2ARG_PRO_OID = proc_oid_t(133);
  static constexpr proc_oid_t RTRIM1ARG_PRO_OID = proc_oid_t(134);
  static constexpr proc_oid_t CONCAT_PRO_OID = proc_oid_t(135);
  static constexpr proc_oid_t VERSION_PRO_OID = proc_oid_t(136);
  static constexpr proc_oid_t DATE_PART_PRO_OID = proc_oid_t(137);

  static constexpr proc_oid_t NP_RUNNERS_EMIT_INT_PRO_OID = proc_oid_t(900);
  static constexpr proc_oid_t NP_RUNNERS_EMIT_REAL_PRO_OID = proc_oid_t(901);
  static constexpr proc_oid_t NP_RUNNERS_DUMMY_INT_PRO_OID = proc_oid_t(902);
  static constexpr proc_oid_t NP_RUNNERS_DUMMY_REAL_PRO_OID = proc_oid_t(903);
};

}  // namespace noisepage::catalog::postgres
