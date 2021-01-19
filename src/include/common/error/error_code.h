#pragma once

// PostgreSQL is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT
// licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and
// without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the
// following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR
// CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
// THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
// BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
// MODIFICATIONS.

#include <string_view>

namespace noisepage::common {

// This was generated from https://github.com/postgres/postgres/blob/master/src/backend/utils/errcodes.txt so stuck the
// Postgres license at the top to be safe.

enum class ErrorCode : uint16_t {
  // Section: Class 00 - Successful Completion

  ERRCODE_SUCCESSFUL_COMPLETION,  // 00000

  // Section: Class 01 - Warning

  // do not use this class for failure conditions
  ERRCODE_WARNING,                                        // 01000
  ERRCODE_WARNING_DYNAMIC_RESULT_SETS_RETURNED,           // 0100C
  ERRCODE_WARNING_IMPLICIT_ZERO_BIT_PADDING,              // 01008
  ERRCODE_WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION,  // 01003
  ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED,                  // 01007
  ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED,                  // 01006
  ERRCODE_WARNING_STRING_DATA_RIGHT_TRUNCATION,           // 01004
  ERRCODE_WARNING_DEPRECATED_FEATURE,                     // 01P01

  // Section: Class 02 - No Data (this is also a warning class per the SQL standard)

  // do not use this class for failure conditions
  ERRCODE_NO_DATA,                                     // 02000
  ERRCODE_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED,  // 02001

  // Section: Class 03 - SQL Statement Not Yet Complete

  ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE,  // 03000

  // Section: Class 08 - Connection Exception

  ERRCODE_CONNECTION_EXCEPTION,                               // 08000
  ERRCODE_CONNECTION_DOES_NOT_EXIST,                          // 08003
  ERRCODE_CONNECTION_FAILURE,                                 // 08006
  ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,        // 08001
  ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,  // 08004
  ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN,                     // 08007
  ERRCODE_PROTOCOL_VIOLATION,                                 // 08P01

  // Section: Class 09 - Triggered Action Exception

  ERRCODE_TRIGGERED_ACTION_EXCEPTION,  // 09000

  // Section: Class 0A - Feature Not Supported

  ERRCODE_FEATURE_NOT_SUPPORTED,  // 0A000

  // Section: Class 0B - Invalid Transaction Initiation

  ERRCODE_INVALID_TRANSACTION_INITIATION,  // 0B000

  // Section: Class 0F - Locator Exception

  ERRCODE_LOCATOR_EXCEPTION,          // 0F000
  ERRCODE_L_E_INVALID_SPECIFICATION,  // 0F001

  // Section: Class 0L - Invalid Grantor

  ERRCODE_INVALID_GRANTOR,          // 0L000
  ERRCODE_INVALID_GRANT_OPERATION,  // 0LP01

  // Section: Class 0P - Invalid Role Specification

  ERRCODE_INVALID_ROLE_SPECIFICATION,  // 0P000

  // Section: Class 0Z - Diagnostics Exception

  ERRCODE_DIAGNOSTICS_EXCEPTION,                                // 0Z000
  ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER,  // 0Z002

  // Section: Class 20 - Case Not Found

  ERRCODE_CASE_NOT_FOUND,  // 20000

  // Section: Class 21 - Cardinality Violation

  // this means something returned the wrong number of rows
  ERRCODE_CARDINALITY_VIOLATION,  // 21000

  // Section: Class 22 - Data Exception

  ERRCODE_DATA_EXCEPTION,       // 22000
  ERRCODE_ARRAY_ELEMENT_ERROR,  // 2202E
  // SQL99's actual definition of "array element error" is subscript error
  ERRCODE_ARRAY_SUBSCRIPT_ERROR,                            // 2202E
  ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,                      // 22021
  ERRCODE_DATETIME_FIELD_OVERFLOW,                          // 22008
  ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,                      // 22008
  ERRCODE_DIVISION_BY_ZERO,                                 // 22012
  ERRCODE_ERROR_IN_ASSIGNMENT,                              // 22005
  ERRCODE_ESCAPE_CHARACTER_CONFLICT,                        // 2200B
  ERRCODE_INDICATOR_OVERFLOW,                               // 22022
  ERRCODE_INTERVAL_FIELD_OVERFLOW,                          // 22015
  ERRCODE_INVALID_ARGUMENT_FOR_LOG,                         // 2201E
  ERRCODE_INVALID_ARGUMENT_FOR_NTILE,                       // 22014
  ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE,                   // 22016
  ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,              // 2201F
  ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION,       // 2201G
  ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST,                 // 22018
  ERRCODE_INVALID_DATETIME_FORMAT,                          // 22007
  ERRCODE_INVALID_ESCAPE_CHARACTER,                         // 22019
  ERRCODE_INVALID_ESCAPE_OCTET,                             // 2200D
  ERRCODE_INVALID_ESCAPE_SEQUENCE,                          // 22025
  ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,              // 22P06
  ERRCODE_INVALID_INDICATOR_PARAMETER_VALUE,                // 22010
  ERRCODE_INVALID_PARAMETER_VALUE,                          // 22023
  ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,              // 22013
  ERRCODE_INVALID_REGULAR_EXPRESSION,                       // 2201B
  ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,                // 2201W
  ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,        // 2201X
  ERRCODE_INVALID_TABLESAMPLE_ARGUMENT,                     // 2202H
  ERRCODE_INVALID_TABLESAMPLE_REPEAT,                       // 2202G
  ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,             // 22009
  ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER,                  // 2200C
  ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH,                      // 2200G
  ERRCODE_NULL_VALUE_NOT_ALLOWED,                           // 22004
  ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER,                // 22002
  ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,                       // 22003
  ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED,                // 2200H
  ERRCODE_STRING_DATA_LENGTH_MISMATCH,                      // 22026
  ERRCODE_STRING_DATA_RIGHT_TRUNCATION,                     // 22001
  ERRCODE_SUBSTRING_ERROR,                                  // 22011
  ERRCODE_TRIM_ERROR,                                       // 22027
  ERRCODE_UNTERMINATED_C_STRING,                            // 22024
  ERRCODE_ZERO_LENGTH_CHARACTER_STRING,                     // 2200F
  ERRCODE_FLOATING_POINT_EXCEPTION,                         // 22P01
  ERRCODE_INVALID_TEXT_REPRESENTATION,                      // 22P02
  ERRCODE_INVALID_BINARY_REPRESENTATION,                    // 22P03
  ERRCODE_BAD_COPY_FILE_FORMAT,                             // 22P04
  ERRCODE_UNTRANSLATABLE_CHARACTER,                         // 22P05
  ERRCODE_NOT_AN_XML_DOCUMENT,                              // 2200L
  ERRCODE_INVALID_XML_DOCUMENT,                             // 2200M
  ERRCODE_INVALID_XML_CONTENT,                              // 2200N
  ERRCODE_INVALID_XML_COMMENT,                              // 2200S
  ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION,               // 2200T
  ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE,                  // 22030
  ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION,  // 22031
  ERRCODE_INVALID_JSON_TEXT,                                // 22032
  ERRCODE_INVALID_SQL_JSON_SUBSCRIPT,                       // 22033
  ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM,                      // 22034
  ERRCODE_NO_SQL_JSON_ITEM,                                 // 22035
  ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,                        // 22036
  ERRCODE_NON_UNIQUE_KEYS_IN_A_JSON_OBJECT,                 // 22037
  ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED,                 // 22038
  ERRCODE_SQL_JSON_ARRAY_NOT_FOUND,                         // 22039
  ERRCODE_SQL_JSON_MEMBER_NOT_FOUND,                        // 2203A
  ERRCODE_SQL_JSON_NUMBER_NOT_FOUND,                        // 2203B
  ERRCODE_SQL_JSON_OBJECT_NOT_FOUND,                        // 2203C
  ERRCODE_TOO_MANY_JSON_ARRAY_ELEMENTS,                     // 2203D
  ERRCODE_TOO_MANY_JSON_OBJECT_MEMBERS,                     // 2203E
  ERRCODE_SQL_JSON_SCALAR_REQUIRED,                         // 2203F

  // Section: Class 23 - Integrity Constraint Violation

  ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION,  // 23000
  ERRCODE_RESTRICT_VIOLATION,              // 23001
  ERRCODE_NOT_NULL_VIOLATION,              // 23502
  ERRCODE_FOREIGN_KEY_VIOLATION,           // 23503
  ERRCODE_UNIQUE_VIOLATION,                // 23505
  ERRCODE_CHECK_VIOLATION,                 // 23514
  ERRCODE_EXCLUSION_VIOLATION,             // 23P01

  // Section: Class 24 - Invalid Cursor State

  ERRCODE_INVALID_CURSOR_STATE,  // 24000

  // Section: Class 25 - Invalid Transaction State

  ERRCODE_INVALID_TRANSACTION_STATE,                             // 25000
  ERRCODE_ACTIVE_SQL_TRANSACTION,                                // 25001
  ERRCODE_BRANCH_TRANSACTION_ALREADY_ACTIVE,                     // 25002
  ERRCODE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL,             // 25008
  ERRCODE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION,      // 25003
  ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION,  // 25004
  ERRCODE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION,      // 25005
  ERRCODE_READ_ONLY_SQL_TRANSACTION,                             // 25006
  ERRCODE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED,        // 25007
  ERRCODE_NO_ACTIVE_SQL_TRANSACTION,                             // 25P01
  ERRCODE_IN_FAILED_SQL_TRANSACTION,                             // 25P02
  ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,                   // 25P03

  // Section: Class 26 - Invalid SQL Statement Name

  // (we take this to mean prepared statements
  ERRCODE_INVALID_SQL_STATEMENT_NAME,  // 26000

  // Section: Class 27 - Triggered Data Change Violation

  ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION,  // 27000

  // Section: Class 28 - Invalid Authorization Specification

  ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,  // 28000
  ERRCODE_INVALID_PASSWORD,                     // 28P01

  // Section: Class 2B - Dependent Privilege Descriptors Still Exist

  ERRCODE_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST,  // 2B000
  ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST,                // 2BP01

  // Section: Class 2D - Invalid Transaction Termination

  ERRCODE_INVALID_TRANSACTION_TERMINATION,  // 2D000

  // Section: Class 2F - SQL Routine Exception

  ERRCODE_SQL_ROUTINE_EXCEPTION,                        // 2F000
  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT,  // 2F005
  ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED,       // 2F002
  ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED,     // 2F003
  ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED,         // 2F004

  // Section: Class 34 - Invalid Cursor Name

  ERRCODE_INVALID_CURSOR_NAME,  // 34000

  // Section: Class 38 - External Routine Exception

  ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,                // 38000
  ERRCODE_E_R_E_CONTAINING_SQL_NOT_PERMITTED,        // 38001
  ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED,    // 38002
  ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED,  // 38003
  ERRCODE_E_R_E_READING_SQL_DATA_NOT_PERMITTED,      // 38004

  // Section: Class 39 - External Routine Invocation Exception

  ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION,    // 39000
  ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED,        // 39001
  ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED,           // 39004
  ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED,        // 39P01
  ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED,            // 39P02
  ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED,  // 39P03

  // Section: Class 3B - Savepoint Exception

  ERRCODE_SAVEPOINT_EXCEPTION,        // 3B000
  ERRCODE_S_E_INVALID_SPECIFICATION,  // 3B001

  // Section: Class 3D - Invalid Catalog Name

  ERRCODE_INVALID_CATALOG_NAME,  // 3D000

  // Section: Class 3F - Invalid Schema Name

  ERRCODE_INVALID_SCHEMA_NAME,  // 3F000

  // Section: Class 40 - Transaction Rollback

  ERRCODE_TRANSACTION_ROLLBACK,                // 40000
  ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION,  // 40002
  ERRCODE_T_R_SERIALIZATION_FAILURE,           // 40001
  ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN,    // 40003
  ERRCODE_T_R_DEADLOCK_DETECTED,               // 40P01

  // Section: Class 42 - Syntax Error or Access Rule Violation

  ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,  // 42000
  // never use the above; use one of these two if no specific code exists:
  ERRCODE_SYNTAX_ERROR,             // 42601
  ERRCODE_INSUFFICIENT_PRIVILEGE,   // 42501
  ERRCODE_CANNOT_COERCE,            // 42846
  ERRCODE_GROUPING_ERROR,           // 42803
  ERRCODE_WINDOWING_ERROR,          // 42P20
  ERRCODE_INVALID_RECURSION,        // 42P19
  ERRCODE_INVALID_FOREIGN_KEY,      // 42830
  ERRCODE_INVALID_NAME,             // 42602
  ERRCODE_NAME_TOO_LONG,            // 42622
  ERRCODE_RESERVED_NAME,            // 42939
  ERRCODE_DATATYPE_MISMATCH,        // 42804
  ERRCODE_INDETERMINATE_DATATYPE,   // 42P18
  ERRCODE_COLLATION_MISMATCH,       // 42P21
  ERRCODE_INDETERMINATE_COLLATION,  // 42P22
  ERRCODE_WRONG_OBJECT_TYPE,        // 42809
  ERRCODE_GENERATED_ALWAYS,         // 428C9

  // Note: for ERRCODE purposes, we divide namable objects into these categories:
  // databases, schemas, prepared statements, cursors, tables, columns,
  // functions (including operators), and all else (lumped as "objects").
  // (The first four categories are mandated by the existence of separate
  // SQLSTATE classes for them in the spec; in this file, however, we group
  // the ERRCODE names with all the rest under class 42.)  Parameters are
  // sort-of-named objects and get their own ERRCODE.
  //
  // The same breakdown is used for "duplicate" and "ambiguous" complaints,
  // as well as complaints associated with incorrect declarations.

  ERRCODE_UNDEFINED_COLUMN,               // 42703
  ERRCODE_UNDEFINED_CURSOR,               // 34000
  ERRCODE_UNDEFINED_DATABASE,             // 3D000
  ERRCODE_UNDEFINED_FUNCTION,             // 42883
  ERRCODE_UNDEFINED_PSTATEMENT,           // 26000
  ERRCODE_UNDEFINED_SCHEMA,               // 3F000
  ERRCODE_UNDEFINED_TABLE,                // 42P01
  ERRCODE_UNDEFINED_PARAMETER,            // 42P02
  ERRCODE_UNDEFINED_OBJECT,               // 42704
  ERRCODE_DUPLICATE_COLUMN,               // 42701
  ERRCODE_DUPLICATE_CURSOR,               // 42P03
  ERRCODE_DUPLICATE_DATABASE,             // 42P04
  ERRCODE_DUPLICATE_FUNCTION,             // 42723
  ERRCODE_DUPLICATE_PSTATEMENT,           // 42P05
  ERRCODE_DUPLICATE_SCHEMA,               // 42P06
  ERRCODE_DUPLICATE_TABLE,                // 42P07
  ERRCODE_DUPLICATE_ALIAS,                // 42712
  ERRCODE_DUPLICATE_OBJECT,               // 42710
  ERRCODE_AMBIGUOUS_COLUMN,               // 42702
  ERRCODE_AMBIGUOUS_FUNCTION,             // 42725
  ERRCODE_AMBIGUOUS_PARAMETER,            // 42P08
  ERRCODE_AMBIGUOUS_ALIAS,                // 42P09
  ERRCODE_INVALID_COLUMN_REFERENCE,       // 42P10
  ERRCODE_INVALID_COLUMN_DEFINITION,      // 42611
  ERRCODE_INVALID_CURSOR_DEFINITION,      // 42P11
  ERRCODE_INVALID_DATABASE_DEFINITION,    // 42P12
  ERRCODE_INVALID_FUNCTION_DEFINITION,    // 42P13
  ERRCODE_INVALID_PSTATEMENT_DEFINITION,  // 42P14
  ERRCODE_INVALID_SCHEMA_DEFINITION,      // 42P15
  ERRCODE_INVALID_TABLE_DEFINITION,       // 42P16
  ERRCODE_INVALID_OBJECT_DEFINITION,      // 42P17

  // Section: Class 44 - WITH CHECK OPTION Violation

  ERRCODE_WITH_CHECK_OPTION_VIOLATION,  // 44000

  // Section: Class 53 - Insufficient Resources

  // (PostgreSQL-specific error class)
  ERRCODE_INSUFFICIENT_RESOURCES,        // 53000
  ERRCODE_DISK_FULL,                     // 53100
  ERRCODE_OUT_OF_MEMORY,                 // 53200
  ERRCODE_TOO_MANY_CONNECTIONS,          // 53300
  ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,  // 53400

  // Section: Class 54 - Program Limit Exceeded

  // this is for wired-in limits, not resource exhaustion problems (class borrowed from DB2)
  ERRCODE_PROGRAM_LIMIT_EXCEEDED,  // 54000
  ERRCODE_STATEMENT_TOO_COMPLEX,   // 54001
  ERRCODE_TOO_MANY_COLUMNS,        // 54011
  ERRCODE_TOO_MANY_ARGUMENTS,      // 54023

  // Section: Class 55 - Object Not In Prerequisite State

  // (class borrowed from DB2)
  ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,  // 55000
  ERRCODE_OBJECT_IN_USE,                     // 55006
  ERRCODE_CANT_CHANGE_RUNTIME_PARAM,         // 55P02
  ERRCODE_LOCK_NOT_AVAILABLE,                // 55P03
  ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE,       // 55P04

  // Section: Class 57 - Operator Intervention

  // (class borrowed from DB2)
  ERRCODE_OPERATOR_INTERVENTION,  // 57000
  ERRCODE_QUERY_CANCELED,         // 57014
  ERRCODE_ADMIN_SHUTDOWN,         // 57P01
  ERRCODE_CRASH_SHUTDOWN,         // 57P02
  ERRCODE_CANNOT_CONNECT_NOW,     // 57P03
  ERRCODE_DATABASE_DROPPED,       // 57P04

  // Section: Class 58 - System Error (errors external to PostgreSQL itself)

  // (class borrowed from DB2)
  ERRCODE_SYSTEM_ERROR,    // 58000
  ERRCODE_IO_ERROR,        // 58030
  ERRCODE_UNDEFINED_FILE,  // 58P01
  ERRCODE_DUPLICATE_FILE,  // 58P02

  // Section: Class 72 - Snapshot Failure
  // (class borrowed from Oracle)
  ERRCODE_SNAPSHOT_TOO_OLD,  // 72000

  // Section: Class F0 - Configuration File Error

  // (PostgreSQL-specific error class)
  ERRCODE_CONFIG_FILE_ERROR,  // F0000
  ERRCODE_LOCK_FILE_EXISTS,   // F0001

  // Section: Class HV - Foreign Data Wrapper Error (SQL/MED)

  // (SQL/MED-specific error class)
  ERRCODE_FDW_ERROR,                                   // HV000
  ERRCODE_FDW_COLUMN_NAME_NOT_FOUND,                   // HV005
  ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED,          // HV002
  ERRCODE_FDW_FUNCTION_SEQUENCE_ERROR,                 // HV010
  ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION,     // HV021
  ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,                 // HV024
  ERRCODE_FDW_INVALID_COLUMN_NAME,                     // HV007
  ERRCODE_FDW_INVALID_COLUMN_NUMBER,                   // HV008
  ERRCODE_FDW_INVALID_DATA_TYPE,                       // HV004
  ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS,           // HV006
  ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER,     // HV091
  ERRCODE_FDW_INVALID_HANDLE,                          // HV00B
  ERRCODE_FDW_INVALID_OPTION_INDEX,                    // HV00C
  ERRCODE_FDW_INVALID_OPTION_NAME,                     // HV00D
  ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH,  // HV090
  ERRCODE_FDW_INVALID_STRING_FORMAT,                   // HV00A
  ERRCODE_FDW_INVALID_USE_OF_NULL_POINTER,             // HV009
  ERRCODE_FDW_TOO_MANY_HANDLES,                        // HV014
  ERRCODE_FDW_OUT_OF_MEMORY,                           // HV001
  ERRCODE_FDW_NO_SCHEMAS,                              // HV00P
  ERRCODE_FDW_OPTION_NAME_NOT_FOUND,                   // HV00J
  ERRCODE_FDW_REPLY_HANDLE,                            // HV00K
  ERRCODE_FDW_SCHEMA_NOT_FOUND,                        // HV00Q
  ERRCODE_FDW_TABLE_NOT_FOUND,                         // HV00R
  ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,              // HV00L
  ERRCODE_FDW_UNABLE_TO_CREATE_REPLY,                  // HV00M
  ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION,          // HV00N

  // Section: Class P0 - PL/pgSQL Error

  // (PostgreSQL-specific error class)
  ERRCODE_PLPGSQL_ERROR,    // P0000
  ERRCODE_RAISE_EXCEPTION,  // P0001
  ERRCODE_NO_DATA_FOUND,    // P0002
  ERRCODE_TOO_MANY_ROWS,    // P0003
  ERRCODE_ASSERT_FAILURE,   // P0004

  // Section: Class XX - Internal Error

  // this is for "can't-happen" conditions and software bugs (PostgreSQL-specific error class)
  ERRCODE_INTERNAL_ERROR,  // XX000
  ERRCODE_DATA_CORRUPTED,  // XX001
  ERRCODE_INDEX_CORRUPTED  // XX002
};

constexpr std::string_view ErrorCodeToString(const ErrorCode code) {
  switch (code) {
    case ErrorCode::ERRCODE_SUCCESSFUL_COMPLETION:
      return "00000";
    case ErrorCode::ERRCODE_WARNING:
      return "01000";
    case ErrorCode::ERRCODE_WARNING_DYNAMIC_RESULT_SETS_RETURNED:
      return "0100C";
    case ErrorCode::ERRCODE_WARNING_IMPLICIT_ZERO_BIT_PADDING:
      return "01008";
    case ErrorCode::ERRCODE_WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION:
      return "01003";
    case ErrorCode::ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED:
      return "01007";
    case ErrorCode::ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED:
      return "01006";
    case ErrorCode::ERRCODE_WARNING_STRING_DATA_RIGHT_TRUNCATION:
      return "01004";
    case ErrorCode::ERRCODE_WARNING_DEPRECATED_FEATURE:
      return "01P01";
    case ErrorCode::ERRCODE_NO_DATA:
      return "02000";
    case ErrorCode::ERRCODE_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED:
      return "02001";
    case ErrorCode::ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE:
      return "03000";
    case ErrorCode::ERRCODE_CONNECTION_EXCEPTION:
      return "08000";
    case ErrorCode::ERRCODE_CONNECTION_DOES_NOT_EXIST:
      return "08003";
    case ErrorCode::ERRCODE_CONNECTION_FAILURE:
      return "08006";
    case ErrorCode::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION:
      return "08001";
    case ErrorCode::ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION:
      return "08004";
    case ErrorCode::ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN:
      return "08007";
    case ErrorCode::ERRCODE_PROTOCOL_VIOLATION:
      return "08P01";
    case ErrorCode::ERRCODE_TRIGGERED_ACTION_EXCEPTION:
      return "09000";
    case ErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED:
      return "0A000";
    case ErrorCode::ERRCODE_INVALID_TRANSACTION_INITIATION:
      return "0B000";
    case ErrorCode::ERRCODE_LOCATOR_EXCEPTION:
      return "0F000";
    case ErrorCode::ERRCODE_L_E_INVALID_SPECIFICATION:
      return "0F001";
    case ErrorCode::ERRCODE_INVALID_GRANTOR:
      return "0L000";
    case ErrorCode::ERRCODE_INVALID_GRANT_OPERATION:
      return "0LP01";
    case ErrorCode::ERRCODE_INVALID_ROLE_SPECIFICATION:
      return "0P000";
    case ErrorCode::ERRCODE_DIAGNOSTICS_EXCEPTION:
      return "0Z000";
    case ErrorCode::ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER:
      return "0Z002";
    case ErrorCode::ERRCODE_CASE_NOT_FOUND:
      return "20000";
    case ErrorCode::ERRCODE_CARDINALITY_VIOLATION:
      return "21000";
    case ErrorCode::ERRCODE_DATA_EXCEPTION:
      return "22000";
    case ErrorCode::ERRCODE_ARRAY_ELEMENT_ERROR:
      return "2202E";
    case ErrorCode::ERRCODE_ARRAY_SUBSCRIPT_ERROR:
      return "2202E";
    case ErrorCode::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE:
      return "22021";
    case ErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW:
      return "22008";
    case ErrorCode::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE:
      return "22008";
    case ErrorCode::ERRCODE_DIVISION_BY_ZERO:
      return "22012";
    case ErrorCode::ERRCODE_ERROR_IN_ASSIGNMENT:
      return "22005";
    case ErrorCode::ERRCODE_ESCAPE_CHARACTER_CONFLICT:
      return "2200B";
    case ErrorCode::ERRCODE_INDICATOR_OVERFLOW:
      return "22022";
    case ErrorCode::ERRCODE_INTERVAL_FIELD_OVERFLOW:
      return "22015";
    case ErrorCode::ERRCODE_INVALID_ARGUMENT_FOR_LOG:
      return "2201E";
    case ErrorCode::ERRCODE_INVALID_ARGUMENT_FOR_NTILE:
      return "22014";
    case ErrorCode::ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE:
      return "22016";
    case ErrorCode::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION:
      return "2201F";
    case ErrorCode::ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION:
      return "2201G";
    case ErrorCode::ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST:
      return "22018";
    case ErrorCode::ERRCODE_INVALID_DATETIME_FORMAT:
      return "22007";
    case ErrorCode::ERRCODE_INVALID_ESCAPE_CHARACTER:
      return "22019";
    case ErrorCode::ERRCODE_INVALID_ESCAPE_OCTET:
      return "2200D";
    case ErrorCode::ERRCODE_INVALID_ESCAPE_SEQUENCE:
      return "22025";
    case ErrorCode::ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER:
      return "22P06";
    case ErrorCode::ERRCODE_INVALID_INDICATOR_PARAMETER_VALUE:
      return "22010";
    case ErrorCode::ERRCODE_INVALID_PARAMETER_VALUE:
      return "22023";
    case ErrorCode::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE:
      return "22013";
    case ErrorCode::ERRCODE_INVALID_REGULAR_EXPRESSION:
      return "2201B";
    case ErrorCode::ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE:
      return "2201W";
    case ErrorCode::ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE:
      return "2201X";
    case ErrorCode::ERRCODE_INVALID_TABLESAMPLE_ARGUMENT:
      return "2202H";
    case ErrorCode::ERRCODE_INVALID_TABLESAMPLE_REPEAT:
      return "2202G";
    case ErrorCode::ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE:
      return "22009";
    case ErrorCode::ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER:
      return "2200C";
    case ErrorCode::ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH:
      return "2200G";
    case ErrorCode::ERRCODE_NULL_VALUE_NOT_ALLOWED:
      return "22004";
    case ErrorCode::ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER:
      return "22002";
    case ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE:
      return "22003";
    case ErrorCode::ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED:
      return "2200H";
    case ErrorCode::ERRCODE_STRING_DATA_LENGTH_MISMATCH:
      return "22026";
    case ErrorCode::ERRCODE_STRING_DATA_RIGHT_TRUNCATION:
      return "22001";
    case ErrorCode::ERRCODE_SUBSTRING_ERROR:
      return "22011";
    case ErrorCode::ERRCODE_TRIM_ERROR:
      return "22027";
    case ErrorCode::ERRCODE_UNTERMINATED_C_STRING:
      return "22024";
    case ErrorCode::ERRCODE_ZERO_LENGTH_CHARACTER_STRING:
      return "2200F";
    case ErrorCode::ERRCODE_FLOATING_POINT_EXCEPTION:
      return "22P01";
    case ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION:
      return "22P02";
    case ErrorCode::ERRCODE_INVALID_BINARY_REPRESENTATION:
      return "22P03";
    case ErrorCode::ERRCODE_BAD_COPY_FILE_FORMAT:
      return "22P04";
    case ErrorCode::ERRCODE_UNTRANSLATABLE_CHARACTER:
      return "22P05";
    case ErrorCode::ERRCODE_NOT_AN_XML_DOCUMENT:
      return "2200L";
    case ErrorCode::ERRCODE_INVALID_XML_DOCUMENT:
      return "2200M";
    case ErrorCode::ERRCODE_INVALID_XML_CONTENT:
      return "2200N";
    case ErrorCode::ERRCODE_INVALID_XML_COMMENT:
      return "2200S";
    case ErrorCode::ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION:
      return "2200T";
    case ErrorCode::ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE:
      return "22030";
    case ErrorCode::ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION:
      return "22031";
    case ErrorCode::ERRCODE_INVALID_JSON_TEXT:
      return "22032";
    case ErrorCode::ERRCODE_INVALID_SQL_JSON_SUBSCRIPT:
      return "22033";
    case ErrorCode::ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM:
      return "22034";
    case ErrorCode::ERRCODE_NO_SQL_JSON_ITEM:
      return "22035";
    case ErrorCode::ERRCODE_NON_NUMERIC_SQL_JSON_ITEM:
      return "22036";
    case ErrorCode::ERRCODE_NON_UNIQUE_KEYS_IN_A_JSON_OBJECT:
      return "22037";
    case ErrorCode::ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED:
      return "22038";
    case ErrorCode::ERRCODE_SQL_JSON_ARRAY_NOT_FOUND:
      return "22039";
    case ErrorCode::ERRCODE_SQL_JSON_MEMBER_NOT_FOUND:
      return "2203A";
    case ErrorCode::ERRCODE_SQL_JSON_NUMBER_NOT_FOUND:
      return "2203B";
    case ErrorCode::ERRCODE_SQL_JSON_OBJECT_NOT_FOUND:
      return "2203C";
    case ErrorCode::ERRCODE_TOO_MANY_JSON_ARRAY_ELEMENTS:
      return "2203D";
    case ErrorCode::ERRCODE_TOO_MANY_JSON_OBJECT_MEMBERS:
      return "2203E";
    case ErrorCode::ERRCODE_SQL_JSON_SCALAR_REQUIRED:
      return "2203F";
    case ErrorCode::ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION:
      return "23000";
    case ErrorCode::ERRCODE_RESTRICT_VIOLATION:
      return "23001";
    case ErrorCode::ERRCODE_NOT_NULL_VIOLATION:
      return "23502";
    case ErrorCode::ERRCODE_FOREIGN_KEY_VIOLATION:
      return "23503";
    case ErrorCode::ERRCODE_UNIQUE_VIOLATION:
      return "23505";
    case ErrorCode::ERRCODE_CHECK_VIOLATION:
      return "23514";
    case ErrorCode::ERRCODE_EXCLUSION_VIOLATION:
      return "23P01";
    case ErrorCode::ERRCODE_INVALID_CURSOR_STATE:
      return "24000";
    case ErrorCode::ERRCODE_INVALID_TRANSACTION_STATE:
      return "25000";
    case ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION:
      return "25001";
    case ErrorCode::ERRCODE_BRANCH_TRANSACTION_ALREADY_ACTIVE:
      return "25002";
    case ErrorCode::ERRCODE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL:
      return "25008";
    case ErrorCode::ERRCODE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION:
      return "25003";
    case ErrorCode::ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION:
      return "25004";
    case ErrorCode::ERRCODE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION:
      return "25005";
    case ErrorCode::ERRCODE_READ_ONLY_SQL_TRANSACTION:
      return "25006";
    case ErrorCode::ERRCODE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED:
      return "25007";
    case ErrorCode::ERRCODE_NO_ACTIVE_SQL_TRANSACTION:
      return "25P01";
    case ErrorCode::ERRCODE_IN_FAILED_SQL_TRANSACTION:
      return "25P02";
    case ErrorCode::ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT:
      return "25P03";
    case ErrorCode::ERRCODE_INVALID_SQL_STATEMENT_NAME:
      return "26000";
    case ErrorCode::ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION:
      return "27000";
    case ErrorCode::ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION:
      return "28000";
    case ErrorCode::ERRCODE_INVALID_PASSWORD:
      return "28P01";
    case ErrorCode::ERRCODE_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST:
      return "2B000";
    case ErrorCode::ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST:
      return "2BP01";
    case ErrorCode::ERRCODE_INVALID_TRANSACTION_TERMINATION:
      return "2D000";
    case ErrorCode::ERRCODE_SQL_ROUTINE_EXCEPTION:
      return "2F000";
    case ErrorCode::ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT:
      return "2F005";
    case ErrorCode::ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED:
      return "2F002";
    case ErrorCode::ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED:
      return "2F003";
    case ErrorCode::ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED:
      return "2F004";
    case ErrorCode::ERRCODE_INVALID_CURSOR_NAME:
      return "34000";
    case ErrorCode::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION:
      return "38000";
    case ErrorCode::ERRCODE_E_R_E_CONTAINING_SQL_NOT_PERMITTED:
      return "38001";
    case ErrorCode::ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED:
      return "38002";
    case ErrorCode::ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED:
      return "38003";
    case ErrorCode::ERRCODE_E_R_E_READING_SQL_DATA_NOT_PERMITTED:
      return "38004";
    case ErrorCode::ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION:
      return "39000";
    case ErrorCode::ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED:
      return "39001";
    case ErrorCode::ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED:
      return "39004";
    case ErrorCode::ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED:
      return "39P01";
    case ErrorCode::ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED:
      return "39P02";
    case ErrorCode::ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED:
      return "39P03";
    case ErrorCode::ERRCODE_SAVEPOINT_EXCEPTION:
      return "3B000";
    case ErrorCode::ERRCODE_S_E_INVALID_SPECIFICATION:
      return "3B001";
    case ErrorCode::ERRCODE_INVALID_CATALOG_NAME:
      return "3D000";
    case ErrorCode::ERRCODE_INVALID_SCHEMA_NAME:
      return "3F000";
    case ErrorCode::ERRCODE_TRANSACTION_ROLLBACK:
      return "40000";
    case ErrorCode::ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION:
      return "40002";
    case ErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE:
      return "40001";
    case ErrorCode::ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN:
      return "40003";
    case ErrorCode::ERRCODE_T_R_DEADLOCK_DETECTED:
      return "40P01";
    case ErrorCode::ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION:
      return "42000";
    case ErrorCode::ERRCODE_SYNTAX_ERROR:
      return "42601";
    case ErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE:
      return "42501";
    case ErrorCode::ERRCODE_CANNOT_COERCE:
      return "42846";
    case ErrorCode::ERRCODE_GROUPING_ERROR:
      return "42803";
    case ErrorCode::ERRCODE_WINDOWING_ERROR:
      return "42P20";
    case ErrorCode::ERRCODE_INVALID_RECURSION:
      return "42P19";
    case ErrorCode::ERRCODE_INVALID_FOREIGN_KEY:
      return "42830";
    case ErrorCode::ERRCODE_INVALID_NAME:
      return "42602";
    case ErrorCode::ERRCODE_NAME_TOO_LONG:
      return "42622";
    case ErrorCode::ERRCODE_RESERVED_NAME:
      return "42939";
    case ErrorCode::ERRCODE_DATATYPE_MISMATCH:
      return "42804";
    case ErrorCode::ERRCODE_INDETERMINATE_DATATYPE:
      return "42P18";
    case ErrorCode::ERRCODE_COLLATION_MISMATCH:
      return "42P21";
    case ErrorCode::ERRCODE_INDETERMINATE_COLLATION:
      return "42P22";
    case ErrorCode::ERRCODE_WRONG_OBJECT_TYPE:
      return "42809";
    case ErrorCode::ERRCODE_GENERATED_ALWAYS:
      return "428C9";
    case ErrorCode::ERRCODE_UNDEFINED_COLUMN:
      return "42703";
    case ErrorCode::ERRCODE_UNDEFINED_CURSOR:
      return "34000";
    case ErrorCode::ERRCODE_UNDEFINED_DATABASE:
      return "3D000";
    case ErrorCode::ERRCODE_UNDEFINED_FUNCTION:
      return "42883";
    case ErrorCode::ERRCODE_UNDEFINED_PSTATEMENT:
      return "26000";
    case ErrorCode::ERRCODE_UNDEFINED_SCHEMA:
      return "3F000";
    case ErrorCode::ERRCODE_UNDEFINED_TABLE:
      return "42P01";
    case ErrorCode::ERRCODE_UNDEFINED_PARAMETER:
      return "42P02";
    case ErrorCode::ERRCODE_UNDEFINED_OBJECT:
      return "42704";
    case ErrorCode::ERRCODE_DUPLICATE_COLUMN:
      return "42701";
    case ErrorCode::ERRCODE_DUPLICATE_CURSOR:
      return "42P03";
    case ErrorCode::ERRCODE_DUPLICATE_DATABASE:
      return "42P04";
    case ErrorCode::ERRCODE_DUPLICATE_FUNCTION:
      return "42723";
    case ErrorCode::ERRCODE_DUPLICATE_PSTATEMENT:
      return "42P05";
    case ErrorCode::ERRCODE_DUPLICATE_SCHEMA:
      return "42P06";
    case ErrorCode::ERRCODE_DUPLICATE_TABLE:
      return "42P07";
    case ErrorCode::ERRCODE_DUPLICATE_ALIAS:
      return "42712";
    case ErrorCode::ERRCODE_DUPLICATE_OBJECT:
      return "42710";
    case ErrorCode::ERRCODE_AMBIGUOUS_COLUMN:
      return "42702";
    case ErrorCode::ERRCODE_AMBIGUOUS_FUNCTION:
      return "42725";
    case ErrorCode::ERRCODE_AMBIGUOUS_PARAMETER:
      return "42P08";
    case ErrorCode::ERRCODE_AMBIGUOUS_ALIAS:
      return "42P09";
    case ErrorCode::ERRCODE_INVALID_COLUMN_REFERENCE:
      return "42P10";
    case ErrorCode::ERRCODE_INVALID_COLUMN_DEFINITION:
      return "42611";
    case ErrorCode::ERRCODE_INVALID_CURSOR_DEFINITION:
      return "42P11";
    case ErrorCode::ERRCODE_INVALID_DATABASE_DEFINITION:
      return "42P12";
    case ErrorCode::ERRCODE_INVALID_FUNCTION_DEFINITION:
      return "42P13";
    case ErrorCode::ERRCODE_INVALID_PSTATEMENT_DEFINITION:
      return "42P14";
    case ErrorCode::ERRCODE_INVALID_SCHEMA_DEFINITION:
      return "42P15";
    case ErrorCode::ERRCODE_INVALID_TABLE_DEFINITION:
      return "42P16";
    case ErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION:
      return "42P17";
    case ErrorCode::ERRCODE_WITH_CHECK_OPTION_VIOLATION:
      return "44000";
    case ErrorCode::ERRCODE_INSUFFICIENT_RESOURCES:
      return "53000";
    case ErrorCode::ERRCODE_DISK_FULL:
      return "53100";
    case ErrorCode::ERRCODE_OUT_OF_MEMORY:
      return "53200";
    case ErrorCode::ERRCODE_TOO_MANY_CONNECTIONS:
      return "53300";
    case ErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED:
      return "53400";
    case ErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED:
      return "54000";
    case ErrorCode::ERRCODE_STATEMENT_TOO_COMPLEX:
      return "54001";
    case ErrorCode::ERRCODE_TOO_MANY_COLUMNS:
      return "54011";
    case ErrorCode::ERRCODE_TOO_MANY_ARGUMENTS:
      return "54023";
    case ErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE:
      return "55000";
    case ErrorCode::ERRCODE_OBJECT_IN_USE:
      return "55006";
    case ErrorCode::ERRCODE_CANT_CHANGE_RUNTIME_PARAM:
      return "55P02";
    case ErrorCode::ERRCODE_LOCK_NOT_AVAILABLE:
      return "55P03";
    case ErrorCode::ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE:
      return "55P04";
    case ErrorCode::ERRCODE_OPERATOR_INTERVENTION:
      return "57000";
    case ErrorCode::ERRCODE_QUERY_CANCELED:
      return "57014";
    case ErrorCode::ERRCODE_ADMIN_SHUTDOWN:
      return "57P01";
    case ErrorCode::ERRCODE_CRASH_SHUTDOWN:
      return "57P02";
    case ErrorCode::ERRCODE_CANNOT_CONNECT_NOW:
      return "57P03";
    case ErrorCode::ERRCODE_DATABASE_DROPPED:
      return "57P04";
    case ErrorCode::ERRCODE_SYSTEM_ERROR:
      return "58000";
    case ErrorCode::ERRCODE_IO_ERROR:
      return "58030";
    case ErrorCode::ERRCODE_UNDEFINED_FILE:
      return "58P01";
    case ErrorCode::ERRCODE_DUPLICATE_FILE:
      return "58P02";
    case ErrorCode::ERRCODE_SNAPSHOT_TOO_OLD:
      return "72000";
    case ErrorCode::ERRCODE_CONFIG_FILE_ERROR:
      return "F0000";
    case ErrorCode::ERRCODE_LOCK_FILE_EXISTS:
      return "F0001";
    case ErrorCode::ERRCODE_FDW_ERROR:
      return "HV000";
    case ErrorCode::ERRCODE_FDW_COLUMN_NAME_NOT_FOUND:
      return "HV005";
    case ErrorCode::ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED:
      return "HV002";
    case ErrorCode::ERRCODE_FDW_FUNCTION_SEQUENCE_ERROR:
      return "HV010";
    case ErrorCode::ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION:
      return "HV021";
    case ErrorCode::ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE:
      return "HV024";
    case ErrorCode::ERRCODE_FDW_INVALID_COLUMN_NAME:
      return "HV007";
    case ErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER:
      return "HV008";
    case ErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE:
      return "HV004";
    case ErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS:
      return "HV006";
    case ErrorCode::ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER:
      return "HV091";
    case ErrorCode::ERRCODE_FDW_INVALID_HANDLE:
      return "HV00B";
    case ErrorCode::ERRCODE_FDW_INVALID_OPTION_INDEX:
      return "HV00C";
    case ErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME:
      return "HV00D";
    case ErrorCode::ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH:
      return "HV090";
    case ErrorCode::ERRCODE_FDW_INVALID_STRING_FORMAT:
      return "HV00A";
    case ErrorCode::ERRCODE_FDW_INVALID_USE_OF_NULL_POINTER:
      return "HV009";
    case ErrorCode::ERRCODE_FDW_TOO_MANY_HANDLES:
      return "HV014";
    case ErrorCode::ERRCODE_FDW_OUT_OF_MEMORY:
      return "HV001";
    case ErrorCode::ERRCODE_FDW_NO_SCHEMAS:
      return "HV00P";
    case ErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND:
      return "HV00J";
    case ErrorCode::ERRCODE_FDW_REPLY_HANDLE:
      return "HV00K";
    case ErrorCode::ERRCODE_FDW_SCHEMA_NOT_FOUND:
      return "HV00Q";
    case ErrorCode::ERRCODE_FDW_TABLE_NOT_FOUND:
      return "HV00R";
    case ErrorCode::ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION:
      return "HV00L";
    case ErrorCode::ERRCODE_FDW_UNABLE_TO_CREATE_REPLY:
      return "HV00M";
    case ErrorCode::ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION:
      return "HV00N";
    case ErrorCode::ERRCODE_PLPGSQL_ERROR:
      return "P0000";
    case ErrorCode::ERRCODE_RAISE_EXCEPTION:
      return "P0001";
    case ErrorCode::ERRCODE_NO_DATA_FOUND:
      return "P0002";
    case ErrorCode::ERRCODE_TOO_MANY_ROWS:
      return "P0003";
    case ErrorCode::ERRCODE_ASSERT_FAILURE:
      return "P0004";
    case ErrorCode::ERRCODE_INTERNAL_ERROR:
      return "XX000";
    case ErrorCode::ERRCODE_DATA_CORRUPTED:
      return "XX001";
    case ErrorCode::ERRCODE_INDEX_CORRUPTED:
      return "XX002";
    default:
      return "UNKNOWN";
  }
}

}  // namespace noisepage::common
