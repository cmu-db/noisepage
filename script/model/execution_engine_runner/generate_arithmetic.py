#!/usr/bin/env python3

def GetValueSuffix(type):
    if type == 'Integer':
        suffix = ""
    if type == 'Real':
        suffix = ".0"

    return suffix

def OperationToSymbol(operation):
    if operation == "add":
        symbol = "-" # This is on purpose since "+" on floats does not return the correct type
    if operation == "sub":
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
    print("  Integer_placeholder : int64")
    print("  Real_placeholder : float64")
    print("}\n")

def GenerageArithmeticFun(row_num, data_type, operation):
    suffix = GetValueSuffix(data_type)
    fun_name = "{}{}Row{}".format(data_type, operation, row_num)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx)")

    step_size = 100

    print("  for (var i = 0; i < {}; i = i + {}) {{".format(row_num, step_size))
    print("    var a = state.{}_placeholder - 1000000000{}".format(data_type, suffix))
    for i in range(step_size):
        if operation == "greater":
            if i != 0:
                print("    if (a {} 5{}) {{".format(OperationToSymbol(operation), suffix))
                print("      a = 10{}".format(suffix))
                print("    } else {")
                print("      a = 1{}".format(suffix))
                print("    }")
        else:
            print("    a = a {} 3{}".format(OperationToSymbol(operation), suffix))
    print("    state.{}_placeholder = a".format(data_type))
    print("  }")

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
    print("  return state.Integer_placeholder")
    print("}")

def GenerateAll():
    fun_names = []
    row_nums = [10000, 50000, 10000, 500000, 1000000]
    data_types = ["Integer", "Real"]
    operations = ["add", "multiply", "divide", "greater"]

    GenerateState()

    for row_num in row_nums:
        for data_type in data_types:
            for operation in operations:
                fun_names.append(GenerageArithmeticFun(row_num, data_type, operation))

    GenerateMainFun(fun_names)

if __name__ == '__main__':
    GenerateAll()
