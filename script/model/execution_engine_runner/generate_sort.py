#!/usr/bin/env python3

# Generate the key type for the sort
def GenerateSortKey(col_num):
    print("struct SortRow{} {{".format(col_num))
    for i in range(col_num):
        print("  c{} : Integer".format(i + 1))
    print("}")
    print()

def GenerateKeyCheck(col_num):
    print("fun compareFn{}(lhs: *SortRow{}, rhs: *SortRow{}) -> int32 {{".format(col_num, col_num, col_num))
    for i in range(1, col_num + 1):
        print("  if (lhs.c{} < rhs.c{}) {{".format(i, i))
        print("    return -1")
        print("  }")
        print("  if (lhs.c{} > rhs.c{}) {{".format(i, i))
        print("    return 1")
        print("  }")
    print("  return 0")
    print("}\n")

def GenerateBuildSide(col_num, pattern):
    fun_name = "build{}{}".format(col_num, pattern)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartTimer(execCtx)")

    print("  var sorter = &state.sorter{}".format(col_num)) # join hash table

    # table iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    if pattern == "full":
        for i in range(col_num - 1):
            print("  col_oids[{}] = 5".format(i))
        print("  col_oids[{}] = 1".format(col_num - 1))
    elif pattern == 'skew':
        for i in range(col_num):
            print("  col_oids[{}] = 5".format(i))
    elif pattern == 'uniform':
        for i in range(col_num):
            print("  col_oids[{}] = 1".format(i))


    print("  @tableIterInitBind(&tvi, execCtx, \"integer5\", col_oids)")

    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var vec = @tableIterGetPCI(&tvi)")
    print("    for (; @pciHasNext(vec); @pciAdvance(vec)) {")

    print("      var row = @ptrCast(*SortRow{}, @sorterInsert(sorter))".format(col_num))
    for i in range(col_num):
        print("      row.c{} = @pciGetInt(vec, {})".format(i + 1, i))

    print("    }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    print("  @sorterSort(sorter)")

    print("  @execCtxEndTimer(execCtx, @stringToSql(\"{}\"))".format(fun_name))
    print("}")

    print()

    return fun_name

def GenerateProbeSide(col_num, pattern):
    fun_name = "probe{}{}".format(col_num, pattern)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartTimer(execCtx)")

    print("  var sort_iter: SorterIterator")

    print("  for (@sorterIterInit(&sort_iter, &state.sorter{});".format(col_num))
    print("    @sorterIterHasNext(&sort_iter);")
    print("    @sorterIterNext(&sort_iter)) {")
    print("    var row = @ptrCast(*SortRow{}, @sorterIterGetRow(&sort_iter))".format(col_num))
    #print("    state.ret_val = state.ret_val + 1")
    print("  }")
    print("  @sorterIterClose(&sort_iter)")

    print("  @execCtxEndTimer(execCtx, @stringToSql(\"{}\"))".format(fun_name))
    print("}")

    print()

    return fun_name


def GenerateState(col_nums):
    print("struct State {")
    for i in col_nums:
        print("  sorter{}: Sorter".format(i))
    print("  ret_val : int32")
    print("}\n")

def GenerateTearDown(col_nums):
    print("fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        print("  @sorterFree(&state.sorter{})".format(i))
    print("}\n")


def GenerateSetup(col_nums):
    print("fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        print("  @sorterInit(&state.sorter{}, @execCtxGetMem(execCtx), compareFn{}, @sizeOf(SortRow{}))".format(i, i,
                                                                                                                i))
    print("  state.ret_val = 0")
    print("}\n")

def GenerateMainFun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")
    print("  setUpState(execCtx, &state)")

    n = len(fun_names)

    for i, fun_name in enumerate(fun_names):
        print("  {}(execCtx, &state)".format(fun_name))

    print("  tearDownState(execCtx, &state)")
    print("  return state.ret_val")
    print("}")


def GenerateAll():
    col_nums = range(1, 6)
    dist_patterns = ['full', 'uniform', 'skew']
    fun_names = []

    for col_num in col_nums:
        GenerateSortKey(col_num)

    GenerateState(col_nums)

    for col_num in col_nums:
        GenerateKeyCheck(col_num)

    GenerateSetup(col_nums)
    GenerateTearDown(col_nums)

    for pattern in dist_patterns:
        for col_num in col_nums:
            fun_names.append(GenerateBuildSide(col_num, pattern))
        for col_num in col_nums:
            fun_names.append(GenerateProbeSide(col_num, pattern))

    GenerateMainFun(fun_names)

if __name__ == '__main__':
    GenerateAll()
