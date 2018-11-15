
Porting notes for libpg_query (from Peloton)

third_party/libpg_query is ported from the Peloton sources, with a few
small changes to the include files (other than formatting).

The changes are to content and location of include files in
src/include/parser


nodes.h - now wraps libpg_query/nodes.h (rather than including a modified
	copy)

parsenodes.h - no change.
	   include/parser/parsenodes.h is a modified (reduced?) version of 
	   libpg_query/src/postgres/include/nodes/parsenodes.h

pg_list.h - removed from include directory, as contents are identical
	  (except for formatting) to libpg_query version.
	  Modify
	  #include "parser/pg_list.h"
	  to
	  #include "libpg_query/pg_list.h"

pg_query.h - removed from include directory, as contents are identical
	  (except for formatting) to libpg_query version.
	  Modify
	  #include "parser/pg_query.h"
	  to
	  #include "libpg_query/pg_query.h"

pg_trigger.h - no change.
	   include/parser/pg_trigger.h is a reduced version of
	   libpg_query/src/postgres/include/catalog/pg_trigger.h

