#!/usr/bin/env python3


def generate_sort_key(col_num):
    # Generate the key type for the sort
    print("struct SortRow{} {{".format(col_num))
    for i in range(col_num):
        print("  c{} : Integer".format(i + 1))
    print("}")
    print()


def generate_key_check(col_num):
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


def generate_build_side(col_num, row_num, cardinality):
    fun_name = "buildCol{}Row{}Car{}".format(col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx, 3)")

    print("  var sorter = &state.sorter{}".format(col_num))  # sort buffer

    # table iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(0, col_num):
        print("  col_oids[{}] = {}".format(i, 15 - i))

    print("  @tableIterInitBind(&tvi, execCtx, \"INTEGERCol31Row{}Car{}\", col_oids)".format(row_num, cardinality))

    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var vec = @tableIterGetPCI(&tvi)")
    print("    for (; @pciHasNext(vec); @pciAdvance(vec)) {")

    print("      var row = @ptrCast(*SortRow{}, @sorterInsert(sorter))".format(col_num))
    for i in range(col_num):
        print("      row.c{} = @pciGetInt(vec, {})".format(i + 1, col_num - 1))

    print("    }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    print("  @sorterSort(sorter)")

    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"SORT_BUILD, {}, {}, {}\"))".format(
        row_num, col_num * 4, cardinality))
    print("}")

    print()

    return fun_name


def generate_probe_side(col_num, row_num, cardinality):
    fun_name = "probeCol{}Row{}Car{}".format(col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx, 3)")

    print("  var sort_iter: SorterIterator")

    print("  for (@sorterIterInit(&sort_iter, &state.sorter{});".format(col_num))
    print("    @sorterIterHasNext(&sort_iter);")
    print("    @sorterIterNext(&sort_iter)) {")
    print("    var row = @ptrCast(*SortRow{}, @sorterIterGetRow(&sort_iter))".format(col_num))
    print("    state.ret_val = state.ret_val + 1")
    print("  }")
    print("  @sorterIterClose(&sort_iter)")

    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"SORT_ITERATE, {}, {}, {}\"))".format(
        row_num, col_num * 4, cardinality))
    print("}")

    print()

    return fun_name


def generate_state(col_nums):
    print("struct State {")
    for i in col_nums:
        print("  sorter{}: Sorter".format(i))
    print("  ret_val : int32")
    print("}\n")


def generate_tear_down(col_nums):
    print("fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        print("  @sorterFree(&state.sorter{})".format(i))
    print("}\n")


def generate_setup(col_nums):
    print("fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        print("  @sorterInit(&state.sorter{}, @execCtxGetMem(execCtx), compareFn{}, @sizeOf(SortRow{}))".format(i, i,
                                                                                                                i))
    print("  state.ret_val = 0")
    print("}\n")


def generate_main_fun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")

    for fun_name in fun_names:
        if "build" in fun_name:
            print("\n  setUpState(execCtx, &state)")
        print("  {}(execCtx, &state)".format(fun_name))

        if "probe" in fun_name:
            print("  tearDownState(execCtx, &state)")

    print("\n  return state.ret_val")
    print("}")


def generate_all():
    col_nums = range(1, 16, 2)
    row_nums = [1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000]
    fun_names = []

    for col_num in col_nums:
        generate_sort_key(col_num)

    generate_state(col_nums)

    for col_num in col_nums:
        generate_key_check(col_num)

    generate_setup(col_nums)
    generate_tear_down(col_nums)

    for col_num in col_nums:
        for row_num in row_nums:
            cardinalities = [1]
            while cardinalities[-1] < row_num:
                cardinalities.append(cardinalities[-1] * 2)
            cardinalities[-1] = row_num
            for cardinality in cardinalities:
                fun_names.append(generate_build_side(col_num, row_num, cardinality))
                fun_names.append(generate_probe_side(col_num, row_num, cardinality))

    generate_main_fun(fun_names)


if __name__ == '__main__':
    generate_all()
