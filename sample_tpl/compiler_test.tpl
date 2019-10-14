/// Inserter API
// Initialization
var inserter : Inserter
@inserterInit(&inserter, execCtx, table_oid) // oid
@inserterInitBind(&inserter, execCtx, "table_name") // name

// Getting and filling table PR
var pr : *ProjectedRow = @inserterGetTablePR(&inserter)
@prSetInt(pr, 0, ...)
@prSetDateNull(pr, 1, ...)

// Inserting in table
var tuple_slot: *Tupleslot = @inserterTableInsert(&inserter)

// Getting and filling each index PR
var index_pr: *ProjectedRow = @inserterGetIndexPR(&inserter, index_oid1)
@prSetInt(index_pr, 1, @prGetInt(table_pr, 0))
@inserterIndexInsert(&inserter, index_oid1) // explicitly pass in tuple slot?


/// Selector API
// Initialization
var selector: Selector
var col_oids : [2]uint32
col_oids[0] = 1
col_oids[1] = 2
@selectorInit(&selector, execCtx, table_oid, col_oids)
@selectorInitBind(&selector, execCtx, "table_name", col_oids)

// Select
var pr : *ProjectedRow = @selectorSelect(&select, tuple_slot) // Assume slot is received from the an index iterator.


/// Updater API
// Initialization
var updater : Updater
var col_oids : [2]uint32
col_oids[0] = 1
col_oids[1] = 2
@updaterInit(&selector, execCtx, table_oid, col_oids)
@updaterInitBind(&selector, execCtx, "table_name", col_oids)

// Getting and filling table PR
var pr : *ProjectedRow = @updaterGetTablePR(&inserter)
@prSetInt(pr, 0, ...)
@prSetDateNull(pr, 1, ...)

// Updating in table
@updaterTableUpdate(&inserter, slot) // Assume slot is received from the an index iterator.

// Delete + Insert in indexes
var index_pr: *ProjectedRow = @updaterGetIndexPR(&inserter, index_oid1)
@updaterIndexDelete(&inserter, index_oid1)
@updaterIndexInsert(&inserter, index_oid1, slot) // explicitely pass in tuple slot?



/// Deleter API
var deleter : Deleter
@deleterInit(&deleter, execCtx, table_oid)
@deleterInitBind(&deleter, execCtx, "table_name")

// Get table PR
var pr : *ProjectedRow = @deleterTableSelect(&select, tuple_slot) // // Assume slot is received from the an index iterator.

// Delete in all indexes
var index_pr : *ProjectorRow = @deleterGetIndexPR(&deleter, index_oid1)
@prSetInt(pr, 0, ...)
@deleterIndexDelete(index_pr)

// Delete in table
@deleterTableDelete(&select, tuple_slot)










