// Problem: the tuple slot that will be inserted into must already have been allocated/reallocated before this is run
// Problem: @todo: FIX! where is the projected row defined/set? Otherwise, nothing is actually being inserted...
// Inserts tuples into a specific tuple slot of an empty table
// Does not return anything

fun main(execCtx: *ExecutionContext, insert_slot: *TupleSlot) -> nil {
  // Init storage_interface
  var col_oids: [1]uint32
  col_oids[0] = 1 // colA
  var storage_interface: StorageInterface
  @storageInterfaceInitBind(&storage_interface, execCtx, "empty_table", col_oids, true)

  // Insert into the empty table
  @tableInsertInto(&storage_interface, insert_slot)

  // Unbind/Free the storage_interface
  @storageInterfaceFree(&storage_interface)
}
