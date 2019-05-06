struct Row {
  a: int32
  b: int32
}

fun compareFn(lhs: *Row, rhs: *Row) -> int32 {
  if (lhs.a < rhs.a) {
    return -1
  } else {
    return 1
  }
}

fun main() -> int32 {
  var alloc: RegionAlloc
  @regionInit(&alloc)

  var sorter: Sorter
  @sorterInit(&sorter, &alloc, compareFn, @sizeOf(Row))

  for (row in test_1) {
    if (row.colA < 500) {
      var elem = @ptrCast(*Row, @sorterInsert(&sorter))
      elem.a = row.colA
      elem.b = row.colB
    }
  }

  @sorterSort(&sorter)

  @sorterFree(&sorter)
  @regionFree(&alloc)

  return 0
}
