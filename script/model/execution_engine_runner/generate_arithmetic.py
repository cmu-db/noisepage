#!/usr/bin/env python3

def DataTypeToGetName(type):
    if type == 'Integer':
        get_name = "Int"
    if type == 'Real':
        get_name = "Double"

    return get_name

def GetArithmeticValue(type):
    if type == 'Integer':
        value = 54321
    if type == 'Real':
        value = 54321.0

    return value

def OperationToSymbol(operation):
    if operation == "add":
        symbol = "+"
    if operation == "minus":
        symbol = "-"
    if operation == "multiply":
        symbol = "*"
    if operation == "divide":
        symbol = "/"
    if operation == "greater":
        symbol = "<"
    if operation == "equal":
        symbol = "=="

    return symbol

def GenerateState():
    print("struct State {")
    print("  Integer_placeholder : Integer")
    print("  Real_placeholder : Real")
    print("}\n")

def GenerageScanFun(col_num, row_num, cardinality, data_type):
    get_name = DataTypeToGetName(data_type)

    fun_name = "scanCol{}Row{}Car{}".format(col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx)")

    # construct the iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(col_num):
        print("  col_oids[{}] = {}".format(i, i + 1)) # oids for each column
    print("  @tableIterInitBind(&tvi, execCtx, \"{}Col5Row{}Car{}\", col_oids)".format(data_type, row_num, cardinality))

    # iterate the table
    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var pci = @tableIterGetPCI(&tvi)")
    print("      for (; @pciHasNext(pci); @pciAdvance(pci)) {")
    for i in range(0, col_num):
        print("        var val{} = @pciGet{}(pci, {})".format(i, get_name, i))
    print("      }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    # Every integer column has four bytes
    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"basescan\"))".format())
    print("}")

    print()

    return fun_name

def GenerageArithmeticFun(col_num, row_num, cardinality, data_type, operation):
    get_name = DataTypeToGetName(data_type)
    arithmetic_value = GetArithmeticValue(data_type)
    fun_name = "{}{}Col{}Row{}Car{}".format(data_type, operation, col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx)")

    # construct the iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(col_num):
        print("  col_oids[{}] = {}".format(i, i + 1)) # oids for each column
    print("  @tableIterInitBind(&tvi, execCtx, \"{}Col5Row{}Car{}\", col_oids)".format(data_type, row_num, cardinality))

    # iterate the table
    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var pci = @tableIterGetPCI(&tvi)")
    print("      for (; @pciHasNext(pci); @pciAdvance(pci)) {")
    for i in range(0, col_num):
        print("        var val{} = @pciGet{}(pci, {})".format(i, get_name, i))
        if operation == "greater" or operation == "equal":
            print("        if (val{} {} {}) {{".format(i, OperationToSymbol(operation), arithmetic_value))
            print("          state.{}_placeholder = val{}".format(data_type, i))
            print("        }")
        else:
            print("        state.{}_placeholder = val{} {} {}".format(data_type, i, OperationToSymbol(operation),
                                                                      arithmetic_value))
    print("      }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    # Every integer column has four bytes
    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"{}{}, {}\"))".format(data_type.lower(),
                                                                                     operation, row_num))
    print("}")

    print()

    return fun_name

def GenerateMainFun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")
    for fun_name in fun_names:
        print("  {}(execCtx, &state)".format(fun_name))
    print("  return 0")
    print("}")

def GenerateAll():
    col_nums = [1]
    fun_names = []
    row_nums = [1000000]
    cardinalities = [100]
    data_types = ["Integer", "Real"]
    operations = ["add", "minus", "multiply", "divide", "greater", "equal"]

    GenerateState()

    for col_num in col_nums:
        for row_num in row_nums:
            for cardinality in cardinalities:
                for data_type in data_types:
                    for operation in operations:
                        fun_names.append(GenerageScanFun(col_num, row_num, cardinality, data_type))
                        fun_names.append(GenerageArithmeticFun(col_num, row_num, cardinality, data_type, operation))

    GenerateMainFun(fun_names)

if __name__ == '__main__':
    GenerateAll()
