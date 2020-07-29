#!/usr/bin/env python3

import argparse


def generate_state():
    print("struct State {")
    print("  placeholder : int64")
    print("}\n")


def generate_output_struct(col_num):
    print("struct outputStruct {")
    for i in range(0, col_num):
        print(" c{} : Integer".format(i))
    print("}\n")


def generate_scan_fun(col_num, row_num, cardinality, include_output):
    fun_name = "scanCol{}Row{}Car{}".format(col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx, 3)")

    # construct the iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(col_num):
        print("  col_oids[{}] = {}".format(i, i + 1))  # oids for each column
    print("  @tableIterInitBind(&tvi, execCtx, \"INTEGERCol31Row{}Car{}\", col_oids)".format(row_num, cardinality))

    # iterate the table
    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var vpi = @tableIterGetVPI(&tvi)")
    print("      for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {")
    if include_output == 0:
        for i in range(0, col_num):
            print("        var val{} = @vpiGetInt(vpi, {})".format(i, i))
    else:
        print("        var out = @ptrCast(*outputStruct, @outputAlloc(execCtx))")
        for i in range(0, col_num):
            print("        out.c{} = @vpiGetInt(vpi, {})".format(i, i))
    print("      }")
    print("  }")
    if include_output == 1:
        print("  @outputFinalize(execCtx)")
    print("  @tableIterClose(&tvi)")

    # Every integer column has four bytes
    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"SEQ_SCAN, {}, {}, {}\"))".format(
        row_num, col_num * 4, cardinality))
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


def generate_all(include_output):
    fun_names = []
    col_nums = range(1, 16, 2)
    row_nums = [1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000]

    generate_state()

    if include_output == 1:
        generate_output_struct(16)

    for col_num in col_nums:
        for row_num in row_nums:
            cardinalities = [1]
            while cardinalities[-1] < row_num:
                cardinalities.append(cardinalities[-1] * 2)
            cardinalities[-1] = row_num
            for cardinality in cardinalities:
                fun_names.append(generate_scan_fun(col_num, row_num, cardinality, include_output))

    generate_main_fun(fun_names)


if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Mini Trainer')
    aparser.add_argument('--include_output', type=int, default=0,
                         help='Whether to write to the OutputBuffer (1 represents yes)')
    args = aparser.parse_args()
    generate_all(args.include_output)
