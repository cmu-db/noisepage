#pragma once

/*
 * A modified copy of parsenodes.h from
 * libpg_query/src/postgres/include/nodes/parsenodes.h
 *
 * Unnecessary items removed. Modifications? Additions?
 */

#include <cstdint>

#include "libpg_query/pg_list.h"
#include "parser/nodes.h"

using SetOperation = enum SetOperation { SETOP_NONE = 0, SETOP_UNION, SETOP_INTERSECT, SETOP_EXCEPT };

using Alias = struct Alias {
  NodeTag type_;
  char *aliasname_; /* aliased rel name (never qualified) */
  List *colnames_;  /* optional list of column aliases */
};

using InhOption = enum InhOption {
  INH_NO,     /* Do NOT scan child tables */
  INH_YES,    /* DO scan child tables */
  INH_DEFAULT /* Use current SQL_inheritance option */
};

using BoolExprType = enum BoolExprType { AND_EXPR, OR_EXPR, NOT_EXPR };

using Expr = struct Expr { NodeTag type_; };

/*
 * SubLink
 *
 * A SubLink represents a subselect appearing in an expression, and in some
 * cases also the combining operator(s) just above it.  The subLinkType
 * indicates the form of the expression represented:
 *	EXISTS_SUBLINK		EXISTS(SELECT ...)
 *	ALL_SUBLINK			(lefthand) op ALL (SELECT ...)
 *	ANY_SUBLINK			(lefthand) op ANY (SELECT ...)
 *	ROWCOMPARE_SUBLINK	(lefthand) op (SELECT ...)
 *	EXPR_SUBLINK		(SELECT with single targetlist item ...)
 *	MULTIEXPR_SUBLINK	(SELECT with multiple targetlist items ...)
 *	ARRAY_SUBLINK		ARRAY(SELECT with single targetlist item ...)
 *	CTE_SUBLINK			WITH query (never actually part of an expression)
 * For ALL, ANY, and ROWCOMPARE, the lefthand is a list of expressions of the
 * same length as the subselect's targetlist.  ROWCOMPARE will *always* have
 * a list with more than one entry; if the subselect has just one target
 * then the parser will create an EXPR_SUBLINK instead (and any operator
 * above the subselect will be represented separately).
 * ROWCOMPARE, EXPR, and MULTIEXPR require the subselect to deliver at most
 * one row (if it returns no rows, the result is NULL).
 * ALL, ANY, and ROWCOMPARE require the combining operators to deliver boolean
 * results.  ALL and ANY combine the per-row results using AND and OR
 * semantics respectively.
 * ARRAY requires just one target column, and creates an array of the target
 * column's type using any number of rows resulting from the subselect.
 *
 * SubLink is classed as an Expr node, but it is not actually executable;
 * it must be replaced in the expression tree by a SubPlan node during
 * planning.
 *
 * NOTE: in the raw output of gram.y, testexpr contains just the raw form
 * of the lefthand expression (if any), and operName is the String name of
 * the combining operator.  Also, subselect is a raw parsetree.  During parse
 * analysis, the parser transforms testexpr into a complete boolean expression
 * that compares the lefthand value(s) to PARAM_SUBLINK nodes representing the
 * output columns of the subselect.  And subselect is transformed to a Query.
 * This is the representation seen in saved rules and in the rewriter.
 *
 * In EXISTS, EXPR, MULTIEXPR, and ARRAY SubLinks, testexpr and operName
 * are unused and are always null.
 *
 * subLinkId is currently used only for MULTIEXPR SubLinks, and is zero in
 * other SubLinks.  This number identifies different multiple-assignment
 * subqueries within an UPDATE statement's SET list.  It is unique only
 * within a particular targetlist.  The output column(s) of the MULTIEXPR
 * are referenced by PARAM_MULTIEXPR Params appearing elsewhere in the tlist.
 *
 * The CTE_SUBLINK case never occurs in actual SubLink nodes, but it is used
 * in SubPlans generated for WITH subqueries.
 */
using SubLinkType = enum SubLinkType {
  EXISTS_SUBLINK,
  ALL_SUBLINK,
  ANY_SUBLINK,
  ROWCOMPARE_SUBLINK,
  EXPR_SUBLINK,
  MULTIEXPR_SUBLINK,
  ARRAY_SUBLINK,
  CTE_SUBLINK /* for SubPlans only */
};

using SubLink = struct SubLink {
  Expr xpr_;
  SubLinkType sub_link_type_; /* see above */
  int sub_link_id_;           /* ID (1..n); 0 if not MULTIEXPR */
  Node *testexpr_;            /* outer-query test for ALL/ANY/ROWCOMPARE */
  List *oper_name_;           /* originally specified operator name */
  Node *subselect_;           /* subselect as Query* or raw parsetree */
  int location_;              /* token location, or -1 if unknown */
};

using BoolExpr = struct BoolExpr {
  Expr xpr_;
  BoolExprType boolop_;
  List *args_;   /* arguments to this expression */
  int location_; /* token location, or -1 if unknown */
};

using A_Expr_Kind = enum AExprKind {
  AEXPR_OP,              /* normal operator */
  AEXPR_OP_ANY,          /* scalar op ANY (array) */
  AEXPR_OP_ALL,          /* scalar op ALL (array) */
  AEXPR_DISTINCT,        /* IS DISTINCT FROM - name must be "=" */
  AEXPR_NULLIF,          /* NULLIF - name must be "=" */
  AEXPR_OF,              /* IS [NOT] OF - name must be "=" or "<>" */
  AEXPR_IN,              /* [NOT] IN - name must be "=" or "<>" */
  AEXPR_LIKE,            /* [NOT] LIKE - name must be "~~" or "!~~" */
  AEXPR_ILIKE,           /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
  AEXPR_SIMILAR,         /* [NOT] SIMILAR - name must be "~" or "!~" */
  AEXPR_BETWEEN,         /* name must be "BETWEEN" */
  AEXPR_NOT_BETWEEN,     /* name must be "NOT BETWEEN" */
  AEXPR_BETWEEN_SYM,     /* name must be "BETWEEN SYMMETRIC" */
  AEXPR_NOT_BETWEEN_SYM, /* name must be "NOT BETWEEN SYMMETRIC" */
  AEXPR_PAREN            /* nameless dummy node for parentheses */
};

using A_Expr = struct AExpr {
  NodeTag type_;
  A_Expr_Kind kind_; /* see above */
  List *name_;       /* possibly-qualified name of operator */
  Node *lexpr_;      /* left argument, or NULL if none */
  Node *rexpr_;      /* right argument, or NULL if none */
  int location_;     /* token location, or -1 if unknown */
};

using NullTestType = enum NullTestType { IS_NULL, IS_NOT_NULL };

using NullTest = struct NullTest {
  Expr xpr_;
  Expr *arg_;                 /* input expression */
  NullTestType nulltesttype_; /* IS NULL, IS NOT NULL */
  bool argisrow_;             /* T if input is of a composite type */
  int location_;              /* token location, or -1 if unknown */
};

using JoinExpr = struct JoinExpr {
  NodeTag type_;
  JoinType jointype_;  /* type of join */
  bool is_natural_;    /* Natural join? Will need to shape table */
  Node *larg_;         /* left subtree */
  Node *rarg_;         /* right subtree */
  List *using_clause_; /* USING clause, if any (list of String) */
  Node *quals_;        /* qualifiers on join, if any */
  Alias *alias_;       /* user-written alias clause, if any */
  int rtindex_;        /* RT index assigned for join, or 0 */
};

using CaseExpr = struct CaseExpr {
  Expr xpr_;
  Oid casetype_;    /* type of expression result */
  Oid casecollid_;  /* OID of collation, or InvalidOid if none */
  Expr *arg_;       /* implicit equality comparison argument */
  List *args_;      /* the arguments (list of WHEN clauses) */
  Expr *defresult_; /* the default result (ELSE clause) */
  int location_;    /* token location, or -1 if unknown */
};

using CaseWhen = struct CaseWhen {
  Expr xpr_;
  Expr *expr_;   /* condition expression */
  Expr *result_; /* substitution result */
  int location_; /* token location, or -1 if unknown */
};

using RangeSubselect = struct RangeSubselect {
  NodeTag type_;
  bool lateral_;   /* does it have LATERAL prefix? */
  Node *subquery_; /* the untransformed sub-select clause */
  Alias *alias_;   /* table alias & optional column aliases */
};

using RangeVar = struct RangeVar {
  NodeTag type_;
  char *catalogname_;  /* the catalog (database) name, or NULL */
  char *schemaname_;   /* the schema name, or NULL */
  char *relname_;      /* the relation/sequence name */
  InhOption inh_opt_;  // expand rel by inheritance?
  // recursively act on children?
  char relpersistence_; /* see RELPERSISTENCE_* in pg_class.h */
  Alias *alias_;        /* table alias & optional column aliases */
  int location_;        /* token location, or -1 if unknown */
};

using WithClause = struct WithClause {
  NodeTag type_;
  List *ctes_;     /* list of CommonTableExprs */
  bool recursive_; /* true = WITH RECURSIVE */
  int location_;   /* token location, or -1 if unknown */
};

using OnCommitAction = enum OnCommitAction {
  ONCOMMIT_NOOP,          /* No ON COMMIT clause (do nothing) */
  ONCOMMIT_PRESERVE_ROWS, /* ON COMMIT PRESERVE ROWS (do nothing) */
  ONCOMMIT_DELETE_ROWS,   /* ON COMMIT DELETE ROWS */
  ONCOMMIT_DROP           /* ON COMMIT DROP */
};

using IntoClause = struct IntoClause {
  NodeTag type_;

  RangeVar *rel_;            /* target relation name */
  List *col_names_;          /* column names to assign, or NIL */
  List *options_;            /* options from WITH clause */
  OnCommitAction on_commit_; /* what do we do at COMMIT? */
  char *table_space_name_;   /* table space to use, or NULL */
  Node *view_query_;         /* materialized view's SELECT query */
  bool skip_data_;           /* true for WITH NO DATA */
};

using SortByDir = enum SortByDir {
  SORTBY_DEFAULT,
  SORTBY_ASC,
  SORTBY_DESC,
  SORTBY_USING /* not allowed in CREATE INDEX ... */
};

using SortByNulls = enum SortByNulls { SORTBY_NULLS_DEFAULT, SORTBY_NULLS_FIRST, SORTBY_NULLS_LAST };

using SortBy = struct SortBy {
  NodeTag type_;
  Node *node_;               /* expression to sort on */
  SortByDir sortby_dir_;     /* ASC/DESC/USING/default */
  SortByNulls sortby_nulls_; /* NULLS FIRST/LAST */
  List *use_op_;             /* name of op to use, if SORTBY_USING */
  int location_;             /* operator location, or -1 if none/unknown */
};

using InferClause = struct InferClause {
  NodeTag type_;
  List *index_elems_;  /* IndexElems to infer unique index */
  Node *where_clause_; /* qualification (partial-index predicate) */
  char *conname_;      /* Constraint name, or NULL if unnamed */
  int location_;       /* token location, or -1 if unknown */
};

using OnConflictClause = struct OnConflictClause {
  NodeTag type_;
  OnConflictAction action_; /* DO NOTHING or UPDATE? */
  InferClause *infer_;      /* Optional index inference clause */
  List *target_list_;       /* the target list (of ResTarget) */
  Node *where_clause_;      /* qualifications */
  int location_;            /* token location, or -1 if unknown */
};

using InsertStmt = struct InsertStmt {
  NodeTag type_;
  RangeVar *relation_;                   /* relation to insert into */
  List *cols_;                           /* optional: names of the target columns */
  Node *select_stmt_;                    /* the source SELECT/VALUES, or NULL */
  OnConflictClause *on_conflict_clause_; /* ON CONFLICT clause */
  List *returning_list_;                 /* list of expressions to return */
  WithClause *with_clause_;              /* WITH clause */
};

using SelectStmt = struct SelectStmt {
  NodeTag type_;

  /*
   * These fields are used only in "leaf" SelectStmts.
   */
  List *distinct_clause_;  // NULL, list of DISTINCT ON exprs, or
  // lcons(NIL,NIL) for all (SELECT DISTINCT)
  IntoClause *into_clause_; /* target for SELECT INTO */
  List *target_list_;       /* the target list (of ResTarget) */
  List *from_clause_;       /* the FROM clause */
  Node *where_clause_;      /* WHERE qualification */
  List *group_clause_;      /* GROUP BY clauses */
  Node *having_clause_;     /* HAVING conditional-expression */
  List *window_clause_;     /* WINDOW window_name AS (...), ... */

  /*
   * In a "leaf" node representing a VALUES list, the above fields are all
   * null, and instead this field is set.  Note that the elements of the
   * sublists are just expressions, without ResTarget decoration. Also note
   * that a list element can be DEFAULT (represented as a SetToDefault
   * node), regardless of the context of the VALUES list. It's up to parse
   * analysis to reject that where not valid.
   */
  List *values_lists_; /* untransformed list of expression lists */

  /*
   * These fields are used in both "leaf" SelectStmts and upper-level
   * SelectStmts.
   */
  List *sort_clause_;       /* sort clause (a list of SortBy's) */
  Node *limit_offset_;      /* # of result tuples to skip */
  Node *limit_count_;       /* # of result tuples to return */
  List *locking_clause_;    /* FOR UPDATE (list of LockingClause's) */
  WithClause *with_clause_; /* WITH clause */

  /*
   * These fields are used only in upper-level SelectStmts.
   */
  SetOperation op_;         /* type of set op */
  bool all_;                /* ALL specified? */
  struct SelectStmt *larg_; /* left child */
  struct SelectStmt *rarg_; /* right child */
  /* Eventually add fields for CORRESPONDING spec here */
};

/*
 * Explain Statement
 *
 * The "query" field is initially a raw parse tree, and is converted to a
 * Query node during parse analysis.  Note that rewriting and planning
 * of the query are always postponed until execution.
 */
using ExplainStmt = struct ExplainStmt {
  NodeTag type_;
  Node *query_;   /* the query (see comments above) */
  List *options_; /* list of DefElem nodes */
};

using TypeName = struct TypeName {
  NodeTag type_;
  List *names_;        /* qualified name (list of Value strings) */
  Oid type_oid_;       /* type identified by OID */
  bool setof_;         /* is a set? */
  bool pct_type_;      /* %TYPE specified? */
  List *typmods_;      /* type modifier expression(s) */
  int typemod_;        /* prespecified type modifier */
  List *array_bounds_; /* array bounds */
  int location_;       /* token location, or -1 if unknown */
};

using IndexElem = struct IndexElem {
  NodeTag type_;
  char *name_;                 /* name of attribute to index, or NULL */
  Node *expr_;                 /* expression to index, or NULL */
  char *indexcolname_;         /* name for index column; NULL = default */
  List *collation_;            /* name of collation; NIL = default */
  List *opclass_;              /* name of desired opclass; NIL = default */
  SortByDir ordering_;         /* ASC/DESC/default */
  SortByNulls nulls_ordering_; /* FIRST/LAST/default */
};

using IndexStmt = struct IndexStmt {
  NodeTag type_;
  char *idxname_;          /* name of new index, or NULL for default */
  RangeVar *relation_;     /* relation to build index on */
  char *access_method_;    /* name of access method (eg. btree) */
  char *table_space_;      /* tablespace, or NULL for default */
  List *index_params_;     /* columns to index: a list of IndexElem */
  List *options_;          /* WITH clause options: a list of DefElem */
  Node *where_clause_;     /* qualification (partial-index predicate) */
  List *exclude_op_names_; /* exclusion operator names, or NIL if none */
  char *idxcomment_;       /* comment to apply to index, or NULL */
  Oid index_oid_;          /* OID of an existing index, if any */
  Oid old_node_;           /* relfilenode of existing storage, if any */
  bool unique_;            /* is index unique? */
  bool primary_;           /* is index a primary key? */
  bool isconstraint_;      /* is it for a pkey/unique constraint? */
  bool deferrable_;        /* is the constraint DEFERRABLE? */
  bool initdeferred_;      /* is the constraint INITIALLY DEFERRED? */
  bool transformed_;       /* true when transformIndexStmt is finished */
  bool concurrent_;        /* should this be a concurrent index build? */
  bool if_not_exists_;     /* just do nothing if index already exists? */
};

using CreateTrigStmt = struct CreateTrigStmt {
  NodeTag type_;
  char *trigname_;     /* TRIGGER's name */
  RangeVar *relation_; /* relation trigger is on */
  List *funcname_;     /* qual. name of function to call */
  List *args_;         /* list of (T_String) Values or NIL */
  bool row_;           /* ROW/STATEMENT */
  /* timing uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
  int16_t timing_; /* BEFORE, AFTER, or INSTEAD */
  /* events uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
  int16_t events_;    /* "OR" of INSERT/UPDATE/DELETE/TRUNCATE */
  List *columns_;     /* column names, or NIL for all columns */
  Node *when_clause_; /* qual expression, or NULL if none */
  bool isconstraint_; /* This is a constraint trigger */
  /* The remaining fields are only used for constraint triggers */
  bool deferrable_;     /* [NOT] DEFERRABLE */
  bool initdeferred_;   /* INITIALLY {DEFERRED|IMMEDIATE} */
  RangeVar *constrrel_; /* opposite relation, if RI trigger */
};

using ColumnDef = struct ColumnDef {
  NodeTag type_;
  char *colname_;        /* name of column */
  TypeName *type_name_;  /* type of column */
  int inhcount_;         /* number of times column is inherited */
  bool is_local_;        /* column has local (non-inherited) def'n */
  bool is_not_null_;     /* NOT NULL constraint specified? */
  bool is_from_type_;    /* column definition came from table type */
  char storage_;         /* attstorage setting, or 0 for default */
  Node *raw_default_;    /* default value (untransformed parse tree) */
  Node *cooked_default_; /* default value (transformed expr tree) */
  Node *coll_clause_;    /* untransformed COLLATE spec, if any */
  Oid coll_oid_;         /* collation OID (InvalidOid if not set) */
  List *constraints_;    /* other constraints on column */
  List *fdwoptions_;     /* per-column FDW options */
  int location_;         /* parse location, or -1 if none/unknown */
};

using CreateStmt = struct CreateStmt {
  NodeTag type_;
  RangeVar *relation_;   /* relation to create */
  List *table_elts_;     /* column definitions (list of ColumnDef) */
  List *inh_relations_;  // relations to inherit from (list of
  // inhRelation)
  TypeName *of_typename_;   /* OF typename */
  List *constraints_;       /* constraints (list of Constraint nodes) */
  List *options_;           /* options from WITH clause */
  OnCommitAction oncommit_; /* what do we do at COMMIT? */
  char *tablespacename_;    /* table space to use, or NULL */
  bool if_not_exists_;      /* just do nothing if it already exists? */
};

using ConstrType = enum ConstrType /* types of constraints */
{ CONSTR_NULL,                     // not standard SQL, but a lot of people
                                   // expect it
  CONSTR_NOTNULL,
  CONSTR_DEFAULT,
  CONSTR_CHECK,
  CONSTR_PRIMARY,
  CONSTR_UNIQUE,
  CONSTR_EXCLUSION,
  CONSTR_FOREIGN,
  CONSTR_ATTR_DEFERRABLE, /* attributes for previous constraint node */
  CONSTR_ATTR_NOT_DEFERRABLE,
  CONSTR_ATTR_DEFERRED,
  CONSTR_ATTR_IMMEDIATE };

/* Foreign key action codes */
#define FKCONSTR_ACTION_NOACTION 'a'
#define FKCONSTR_ACTION_RESTRICT 'r'
#define FKCONSTR_ACTION_CASCADE 'c'
#define FKCONSTR_ACTION_SETNULL 'n'
#define FKCONSTR_ACTION_SETDEFAULT 'd'

/* Foreign key matchtype codes */
#define FKCONSTR_MATCH_FULL 'f'
#define FKCONSTR_MATCH_PARTIAL 'p'
#define FKCONSTR_MATCH_SIMPLE 's'

using Constraint = struct Constraint {
  NodeTag type_;
  ConstrType contype_; /* see above */

  /* Fields used for most/all constraint types: */
  char *conname_;     /* Constraint name, or NULL if unnamed */
  bool deferrable_;   /* DEFERRABLE? */
  bool initdeferred_; /* INITIALLY DEFERRED? */
  int location_;      /* token location, or -1 if unknown */

  /* Fields used for constraints with expressions (CHECK and DEFAULT): */
  bool is_no_inherit_; /* is constraint non-inheritable? */
  Node *raw_expr_;     /* expr, as untransformed parse tree */
  char *cooked_expr_;  /* expr, as nodeToString representation */

  /* Fields used for unique constraints (UNIQUE and PRIMARY KEY): */
  List *keys_; /* String nodes naming referenced column(s) */

  /* Fields used for EXCLUSION constraints: */
  List *exclusions_; /* list of (IndexElem, operator name) pairs */

  /* Fields used for index constraints (UNIQUE, PRIMARY KEY, EXCLUSION): */
  List *options_;    /* options from WITH clause */
  char *indexname_;  /* existing index to use; otherwise NULL */
  char *indexspace_; /* index tablespace; NULL for default */
  /* These could be, but currently are not, used for UNIQUE/PKEY: */
  char *access_method_; /* index access method; NULL for default */
  Node *where_clause_;  /* partial index predicate */

  /* Fields used for FOREIGN KEY constraints: */
  RangeVar *pktable_;   /* Primary key table */
  List *fk_attrs_;      /* Attributes of foreign key */
  List *pk_attrs_;      /* Corresponding attrs in PK table */
  char fk_matchtype_;   /* FULL, PARTIAL, SIMPLE */
  char fk_upd_action_;  /* ON UPDATE action */
  char fk_del_action_;  /* ON DELETE action */
  List *old_conpfeqop_; /* pg_constraint.conpfeqop of my former self */
  Oid old_pktable_oid_; /* pg_constraint.confrelid of my former self */

  /* Fields used for constraints that allow a NOT VALID specification */
  bool skip_validation_; /* skip validation of existing rows? */
  bool initially_valid_; /* mark the new constraint as valid? */
};

using DeleteStmt = struct DeleteStmt {
  NodeTag type_;
  RangeVar *relation_;      /* relation to delete from */
  List *using_clause_;      /* optional using clause for more tables */
  Node *where_clause_;      /* qualifications */
  List *returning_list_;    /* list of expressions to return */
  WithClause *with_clause_; /* WITH clause */
};

using ResTarget = struct ResTarget {
  NodeTag type_;
  char *name_;        /* column name or NULL */
  List *indirection_; /* subscripts, field names, and '*', or NIL */
  Node *val_;         /* the value expression to compute or assign */
  int location_;      /* token location, or -1 if unknown */
};

using ColumnRef = struct ColumnRef {
  NodeTag type_;
  List *fields_; /* field names (Value strings) or A_Star */
  int location_; /* token location, or -1 if unknown */
};

using A_Const = struct AConst {
  NodeTag type_;
  value val_;    /* value (includes type info, see value.h) */
  int location_; /* token location, or -1 if unknown */
};

using TypeCast = struct TypeCast {
  NodeTag type_;
  Node *arg_;           /* the expression being casted */
  TypeName *type_name_; /* the target type */
  int location_;        /* token location, or -1 if unknown */
};

using WindowDef = struct WindowDef {
  NodeTag type_;
  char *name_;             /* window's own name */
  char *refname_;          /* referenced window name, if any */
  List *partition_clause_; /* PARTITION BY expression list */
  List *order_clause_;     /* ORDER BY (list of SortBy) */
  int frame_options_;      /* frame_clause options, see below */
  Node *start_offset_;     /* expression for starting bound, if any */
  Node *end_offset_;       /* expression for ending bound, if any */
  int location_;           /* parse location, or -1 if none/unknown */
};

using FuncCall = struct FuncCall {
  NodeTag type_;
  List *funcname_;         /* qualified name of function */
  List *args_;             /* the arguments (list of exprs) */
  List *agg_order_;        /* ORDER BY (list of SortBy) */
  Node *agg_filter_;       /* FILTER clause, if any */
  bool agg_within_group_;  /* ORDER BY appeared in WITHIN GROUP */
  bool agg_star_;          /* argument was really '*' */
  bool agg_distinct_;      /* arguments were labeled DISTINCT */
  bool func_variadic_;     /* last argument was labeled VARIADIC */
  struct WindowDef *over_; /* OVER clause, if any */
  int location_;           /* token location, or -1 if unknown */
};

using UpdateStmt = struct UpdateStmt {
  NodeTag type_;
  RangeVar *relation_;      /* relation to update */
  List *target_list_;       /* the target list (of ResTarget) */
  Node *where_clause_;      /* qualifications */
  List *from_clause_;       /* optional from clause for more tables */
  List *returning_list_;    /* list of expressions to return */
  WithClause *with_clause_; /* WITH clause */
};

using TransactionStmtKind = enum TransactionStmtKind {
  TRANS_STMT_BEGIN,
  TRANS_STMT_START, /* semantically identical to BEGIN */
  TRANS_STMT_COMMIT,
  TRANS_STMT_ROLLBACK,
  TRANS_STMT_SAVEPOINT,
  TRANS_STMT_RELEASE,
  TRANS_STMT_ROLLBACK_TO,
  TRANS_STMT_PREPARE,
  TRANS_STMT_COMMIT_PREPARED,
  TRANS_STMT_ROLLBACK_PREPARED
};

using TransactionStmt = struct TransactionStmt {
  NodeTag type_;
  TransactionStmtKind kind_; /* see above */
  List *options_;            /* for BEGIN/START and savepoint commands */
  char *gid_;                /* for two-phase-commit related commands */
};

using ObjectType = enum ObjectType {
  OBJECT_AGGREGATE,
  OBJECT_AMOP,
  OBJECT_AMPROC,
  OBJECT_ATTRIBUTE, /* type's attribute, when distinct from column */
  OBJECT_CAST,
  OBJECT_COLUMN,
  OBJECT_COLLATION,
  OBJECT_CONVERSION,
  OBJECT_DATABASE,
  OBJECT_DEFAULT,
  OBJECT_DEFACL,
  OBJECT_DOMAIN,
  OBJECT_DOMCONSTRAINT,
  OBJECT_EVENT_TRIGGER,
  OBJECT_EXTENSION,
  OBJECT_FDW,
  OBJECT_FOREIGN_SERVER,
  OBJECT_FOREIGN_TABLE,
  OBJECT_FUNCTION,
  OBJECT_INDEX,
  OBJECT_LANGUAGE,
  OBJECT_LARGEOBJECT,
  OBJECT_MATVIEW,
  OBJECT_OPCLASS,
  OBJECT_OPERATOR,
  OBJECT_OPFAMILY,
  OBJECT_POLICY,
  OBJECT_ROLE,
  OBJECT_RULE,
  OBJECT_SCHEMA,
  OBJECT_SEQUENCE,
  OBJECT_TABCONSTRAINT,
  OBJECT_TABLE,
  OBJECT_TABLESPACE,
  OBJECT_TRANSFORM,
  OBJECT_TRIGGER,
  OBJECT_TSCONFIGURATION,
  OBJECT_TSDICTIONARY,
  OBJECT_TSPARSER,
  OBJECT_TSTEMPLATE,
  OBJECT_TYPE,
  OBJECT_USER_MAPPING,
  OBJECT_VIEW
};

using DropBehaviour = enum DropBehavior {
  DROP_RESTRICT, /* drop fails if any dependent objects */
  DROP_CASCADE   /* remove dependent objects too */
};

using DropStmt = struct DropStmt {
  NodeTag type_;
  List *objects_;          /* list of sublists of names (as Values) */
  List *arguments_;        /* list of sublists of arguments (as Values) */
  ObjectType remove_type_; /* object type */
  DropBehavior behavior_;  /* RESTRICT or CASCADE behavior */
  bool missing_ok_;        /* skip error if object is missing? */
  bool concurrent_;        /* drop index concurrently? */
};

using DropDatabaseStmt = struct DropDatabaseStmt {
  NodeTag type_;
  char *dbname_;    /* name of database to drop */
  bool missing_ok_; /* skip error if object is missing? */
};

using TruncateStmt = struct TruncateStmt {
  NodeTag type_;
  List *relations_;       /* relations (RangeVars) to be truncated */
  bool restart_seqs_;     /* restart owned sequences? */
  DropBehavior behavior_; /* RESTRICT or CASCADE behavior */
};

using ExecuteStmt = struct ExecuteStmt {
  NodeTag type_;
  char *name_;   /* The name of the plan to execute */
  List *params_; /* Values to assign to parameters */
};

using PrepareStmt = struct PrepareStmt {
  NodeTag type_;
  char *name_;     /* Name of plan, arbitrary */
  List *argtypes_; /* Types of parameters (List of TypeName) */
  Node *query_;    /* The query itself (as a raw parsetree) */
};

using DefElemAction = enum DefElemAction {
  DEFELEM_UNSPEC, /* no action given */
  DEFELEM_SET,
  DEFELEM_ADD,
  DEFELEM_DROP
};

using DefElem = struct DefElem {
  NodeTag type_;
  char *defnamespace_; /* NULL if unqualified name */
  char *defname_;
  Node *arg_;               /* a (Value *) or a (TypeName *) */
  DefElemAction defaction_; /* unspecified action, or SET/ADD/DROP */
  int location_;            /* parse location, or -1 if none/unknown */
};

using CopyStmt = struct CopyStmt {
  NodeTag type_;
  RangeVar *relation_; /* the relation to copy */
  Node *query_;        /* the SELECT query to copy */
  List *attlist_;      // List of column names (as Strings), or NIL
  // for all columns
  bool is_from_;    /* TO or FROM */
  bool is_program_; /* is 'filename' a program to popen? */
  char *filename_;  /* filename, or NULL for STDIN/STDOUT */
  List *options_;   /* List of DefElem nodes */
};

using CreateDatabaseStmt = struct CreateDatabaseStmt {
  NodeTag type_;
  char *dbname_;  /* name of database to create */
  List *options_; /* List of DefElem nodes */
};

using CreateSchemaStmt = struct CreateSchemaStmt {
  NodeTag type_;
  char *schemaname_;   /* the name of the schema to create */
  Node *authrole_;     /* the owner of the created schema */
  List *schema_elts_;  /* schema components (list of parsenodes) */
  bool if_not_exists_; /* just do nothing if schema already exists? */
};

using RoleSpecType = enum RoleSpecType {
  ROLESPEC_CSTRING,      /* role name is stored as a C string */
  ROLESPEC_CURRENT_USER, /* role spec is CURRENT_USER */
  ROLESPEC_SESSION_USER, /* role spec is SESSION_USER */
  ROLESPEC_PUBLIC        /* role name is "public" */
};

using RoleSpec = struct RoleSpec {
  NodeTag type_;
  RoleSpecType roletype_; /* Type of this rolespec */
  char *rolename_;        /* filled only for ROLESPEC_CSTRING */
  int location_;          /* token location, or -1 if unknown */
};

using ViewCheckOption = enum ViewCheckOption { NO_CHECK_OPTION, LOCAL_CHECK_OPTION, CASCADED_CHECK_OPTION };

using ViewStmt = struct ViewStmt {
  NodeTag type_;
  RangeVar *view_;                    /* the view to be created */
  List *aliases_;                     /* target column names */
  Node *query_;                       /* the SELECT query */
  bool replace_;                      /* replace an existing view? */
  List *options_;                     /* options from WITH clause */
  ViewCheckOption with_check_option_; /* WITH CHECK OPTION */
};

using ParamRef = struct ParamRef {
  NodeTag type_;
  int number_;   /* the number of the parameter */
  int location_; /* token location, or -1 if unknown */
};

using VacuumOption = enum VacuumOption {
  VACOPT_VACUUM = 1 << 0,   /* do VACUUM */
  VACOPT_ANALYZE = 1 << 1,  /* do ANALYZE */
  VACOPT_VERBOSE = 1 << 2,  /* print progress info */
  VACOPT_FREEZE = 1 << 3,   /* FREEZE option */
  VACOPT_FULL = 1 << 4,     /* FULL (non-concurrent) vacuum */
  VACOPT_NOWAIT = 1 << 5,   /* don't wait to get lock (autovacuum only) */
  VACOPT_SKIPTOAST = 1 << 6 /* don't process the TOAST table, if any */
};

using VacuumStmt = struct VacuumStmt {
  NodeTag type_;
  int options_;        /* OR of VacuumOption flags */
  RangeVar *relation_; /* single table to process, or NULL */
  List *va_cols_;      /* list of column names, or NIL for all */
};

enum VariableSetKind {
  VAR_SET_VALUE,   /* SET var = value */
  VAR_SET_DEFAULT, /* SET var TO DEFAULT */
  VAR_SET_CURRENT, /* SET var FROM CURRENT */
  VAR_SET_MULTI,   /* special case for SET TRANSACTION ... */
  VAR_RESET,       /* RESET var */
  VAR_RESET_ALL    /* RESET ALL */
};

using VariableSetStmt = struct VariableSetStmt {
  NodeTag type_;
  VariableSetKind kind_;
  char *name_;    /* variable to be set */
  List *args_;    /* List of A_Const nodes */
  bool is_local_; /* SET LOCAL? */
};

using VariableShowStmt = struct VariableShowStmt {
  NodeTag type_;
  char *name_;
};

/// **********  For UDFs *********** ///

using CreateFunctionStmt = struct CreateFunctionStmt {
  NodeTag type_;
  bool replace_;          /* T => replace if already exists */
  List *funcname_;        /* qualified name of function to create */
  List *parameters_;      /* a list of FunctionParameter */
  TypeName *return_type_; /* the return type */
  List *options_;         /* a list of DefElem */
  List *with_clause_;     /* a list of DefElem */
};

using FunctionParameterMode = enum FunctionParameterMode {
  /* the assigned enum values appear in pg_proc, don't change 'em! */
  FUNC_PARAM_IN = 'i',       /* input only */
  FUNC_PARAM_OUT = 'o',      /* output only */
  FUNC_PARAM_INOUT = 'b',    /* both */
  FUNC_PARAM_VARIADIC = 'v', /* variadic (always input) */
  FUNC_PARAM_TABLE = 't'     /* table function output column */
};

using FunctionParameter = struct FunctionParameter {
  NodeTag type_;
  char *name_;                 /* parameter name, or NULL if not given */
  TypeName *arg_type_;         /* TypeName for parameter type */
  FunctionParameterMode mode_; /* IN/OUT/etc */
  Node *defexpr_;              /* raw default expr, or NULL if not given */
};
