#!/usr/bin/env python3


def get_value_suffix(value_type):
    suffix = None
    if value_type == 'OP_INTEGER':
        suffix = ""
    if value_type == 'OP_DECIMAL':
        suffix = ".0"

    return suffix


def operation_to_symbol(operation):
    symbol = None
    if operation == "PLUS_OR_MINUS":
        symbol = "-"  # This is on purpose since "+" on floats does not return the correct type
    if operation == "MULTIPLY":
        symbol = "*"
    if operation == "DIVIDE":
        symbol = "/"
    if operation == "COMPARE":
        symbol = "<"

    return symbol


def generate_state():
    print("struct State {")
    print("  OP_INTEGER_placeholder : int64")
    print("  OP_DECIMAL_placeholder : float64")
    print("}\n")


def generage_arithmetic_fun(row_num, data_type, operation):
    suffix = get_value_suffix(data_type)
    fun_name = "{}{}Row{}".format(data_type, operation, row_num)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx, 3)")

    step_size = 100

    print("  for (var i = 0; i < {}; i = i + {}) {{".format(row_num, step_size))
    print("    var a = state.{}_placeholder - 1000000000{}".format(data_type, suffix))
    for i in range(step_size):
        if operation == "COMPARE":
            if i != 0:
                print("    if (a {} 5{}) {{".format(operation_to_symbol(operation), suffix))
                print("      a = 10{}".format(suffix))
                print("    } else {")
                print("      a = 1{}".format(suffix))
                print("    }")
        else:
            print("    a = a {} 3{}".format(operation_to_symbol(operation), suffix))
    print("    state.{}_placeholder = a".format(data_type))
    print("  }")

    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"{}_{}, {}\"))".format(
        data_type, operation, row_num))
    print("}")

    print()

    return fun_name


def generate_main_fun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")
    for fun_name in fun_names:
        print("  {}(execCtx, &state)".format(fun_name))
    print("  return state.OP_INTEGER_placeholder")
    print("}")


def generate_all():
    fun_names = []
    row_nums = list(range(10000, 100000, 10000)) + list(range(100000, 1000000, 100000))
    data_types = ["OP_INTEGER", "OP_DECIMAL"]
    operations = ["PLUS_OR_MINUS", "MULTIPLY", "DIVIDE", "COMPARE"]

    generate_state()

    for row_num in row_nums:
        for data_type in data_types:
            for operation in operations:
                fun_names.append(generage_arithmetic_fun(row_num, data_type, operation))

    generate_main_fun(fun_names)


if __name__ == '__main__':
    generate_all()
