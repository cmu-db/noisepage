#!/usr/bin/env python3


def generate_state():
    print("struct State {")
    print("  placeholder : int64")
    print("}\n")


def generate_scan_fun(col_num, row_num, cardinality):
    fun_name = "scanCol{}Row{}Car{}".format(col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx)")

    # construct the iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(col_num):
        print("  col_oids[{}] = {}".format(i, i + 1))  # oids for each column
    print("  @tableIterInitBind(&tvi, execCtx, \"IntegerCol5Row{}Car{}\", col_oids)".format(row_num, cardinality))

    # iterate the table
    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var pci = @tableIterGetPCI(&tvi)")
    print("      for (; @pciHasNext(pci); @pciAdvance(pci)) {")
    for i in range(0, col_num):
        print("        var val{} = @pciGetInt(pci, {})".format(i, i))
    print("      }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    # Every integer column has four bytes
    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"scan, {}, {}, {}\"))".format(row_num, col_num * 4,
                                                                                             cardinality))
    print("}")

    print()

    return fun_name


def generate_main_fun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")
    for fun_name in fun_names:
        print("  {}(execCtx, &state)".format(fun_name))
    print("  return 0")
    print("}")


def generate_all():
    col_nums = range(1, 6)
    fun_names = []
    row_nums = [1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
                200000, 500000, 1000000]
    cardinalities = [1, 2, 5, 10, 50, 100]

    generate_state()

    for col_num in col_nums:
        for row_num in row_nums:
            for cardinality in cardinalities:
                fun_names.append(generate_scan_fun(col_num, row_num, cardinality))

    generate_main_fun(fun_names)


if __name__ == '__main__':
    generate_all()
