#!/usr/bin/env python3


def get_type_info(value_type):
    # The name we should use after "@vpiGet...", like "@vpiGetInt"
    get_name = None
    type_size = None
    if value_type == 'Integer':
        get_name = "Int"
        type_size = 4
    if value_type == 'Real':
        get_name = "Double"
        type_size = 8

    return get_name, type_size


def tpl_type_to_noisepage_type(value_type):
    if value_type == 'Integer':
        return "INTEGER"
    if value_type == 'Real':
        return "DECIMAL"


def generate_build_key(col_num, value_type):
    # Generate the key type for the join
    print("struct BuildKey{}{} {{".format(value_type, col_num))
    for i in range(col_num):
        print("  c{} : {}".format(i + 1, value_type))
    print("}")
    print()


def generate_build_row(col_num, value_type):
    # Generate the join struct (key and value)
    print("struct BuildRow{}{} {{".format(value_type, col_num))
    print("  key: BuildKey{}{}".format(value_type, col_num))
    print("}")
    print()


def generate_key_check(col_num, value_type):
    get_name, _ = get_type_info(value_type)

    print("fun keyCheck{}{}(row: *BuildRow{}{}, vpi: *VectorProjectionIterator) -> bool {{".format(value_type, col_num,
                                                                                                   value_type, col_num))
    print("  return @sqlToBool(@vpiGet{}(vpi, {}) == row.key.c1)".format(get_name, col_num - 1), end="")
    for i in range(1, col_num):
        print(" and @sqlToBool(@vpiGet{}(vpi, {}) == row.key.c{})".format(get_name, col_num - 1, i + 1), end="")
    print("\n}\n")


def generate_build_side(col_num, row_num, cardinality, value_type):
    get_name, type_size = get_type_info(value_type)

    fun_name = "build{}Col{}Row{}Car{}".format(value_type, col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx, 3)")

    print("  var ht = &state.table{}{}".format(value_type, col_num))  # join hash table

    # table iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(0, col_num):
        print("  col_oids[{}] = {}".format(i, 15 - i))

    print("  @tableIterInitBind(&tvi, execCtx, \"{}Col31Row{}Car{}\", col_oids)".format(tpl_type_to_noisepage_type(
        value_type), row_num, cardinality))

    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var vec = @tableIterGetVPI(&tvi)")
    print("    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {")

    # calculate the join key
    print("      var hash_val = @hash(@vpiGet{}(vec, {})".format(get_name, col_num - 1), end="")
    for i in range(1, col_num):
        print(", @vpiGet{}(vec, {})".format(get_name, col_num - 1), end="")
    print(")")
    # insert into the join table
    print("      var agg = @ptrCast(*BuildRow{}{}, @aggHTLookup(ht, hash_val, keyCheck{}{}, vec))".format(value_type,
                                                                                                          col_num,
                                                                                                          value_type,
                                                                                                          col_num))
    print("      if (agg == nil) {")
    # construct the agg element
    print("        agg = @ptrCast(*BuildRow{}{}, @aggHTInsert(ht, hash_val))".format(value_type, col_num))
    print("        agg.key.c1 = @vpiGet{}(vec, {})".format(get_name, col_num - 1))
    for i in range(1, col_num):
        print("        agg.key.c{} = @vpiGet{}(vec, {})".format(i + 1, get_name, col_num - 1))
    print("      }")

    print("    }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"AGG_BUILD, {}, {}, {}\"))".format(
        row_num, col_num * type_size, cardinality))
    print("}")

    print()

    return fun_name


def generate_probe_side(col_num, row_num, cardinality, value_type):
    _, type_size = get_type_info(value_type)

    fun_name = "probe{}Col{}Row{}Car{}".format(value_type, col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartResourceTracker(execCtx, 3)")

    # construct the iterator
    print("  var agg_ht_iter: AHTIterator")
    print("  var iter = &agg_ht_iter")

    # iterate through the agg table
    print("  for (@aggHTIterInit(iter, &state.table{}{}); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {{".format(
        value_type, col_num))
    print("    var agg = @ptrCast(*BuildRow{}{}, @aggHTIterGetRow(iter))".format(value_type, col_num))
    print("    state.num_matches = state.num_matches + 1")
    print("  }")
    print("  @aggHTIterClose(iter)")

    print("  @execCtxEndResourceTracker(execCtx, @stringToSql(\"AGG_ITERATE, {}, {}, {}\"))".format(
        cardinality, col_num * type_size, cardinality))
    print("}")

    print()

    return fun_name


def generate_state(col_nums, types):
    print("struct State {")
    for i in col_nums:
        for value_type in types:
            print("  table{}{} : AggregationHashTable".format(value_type, i))
    print("  num_matches : int64")
    print("}\n")


def generate_tear_down(col_nums, types):
    print("fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        for value_type in types:
            print("  @aggHTFree(&state.table{}{})".format(value_type, i))
    print("}\n")


def generate_setup(col_nums, types):
    print("fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        for value_type in types:
            print("  @aggHTInit(&state.table{}{}, @execCtxGetMem(execCtx), @sizeOf(BuildRow{}{}))".format(value_type, i,
                                                                                                          value_type,
                                                                                                          i))

    print("  state.num_matches = 0")
    print("}\n")


def generate_main_fun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")

    for i, fun_name in enumerate(fun_names):
        if "build" in fun_name:
            print("\n  setUpState(execCtx, &state)")
        print("  {}(execCtx, &state)".format(fun_name))

        if "probe" in fun_name:
            print("  tearDownState(execCtx, &state)")

    print("\n  return state.num_matches")
    print("}")


def generate_all():
    fun_names = []
    col_nums = range(1, 16, 2)
    row_nums = [1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000]
    # Specified by the type name in tpl
    types = ['Integer']
    # types = ['Integer', "Real"]

    for col_num in col_nums:
        for value_type in types:
            generate_build_key(col_num, value_type)
            generate_build_row(col_num, value_type)
            generate_key_check(col_num, value_type)

    generate_state(col_nums, types)
    generate_setup(col_nums, types)
    generate_tear_down(col_nums, types)

    for col_num in col_nums:
        for row_num in row_nums:
            cardinalities = [1]
            while cardinalities[-1] < row_num:
                cardinalities.append(cardinalities[-1] * 2)
            cardinalities[-1] = row_num
            for cardinality in cardinalities:
                for value_type in types:
                    fun_names.append(generate_build_side(col_num, row_num, cardinality, value_type))
                    fun_names.append(generate_probe_side(col_num, row_num, cardinality, value_type))

    generate_main_fun(fun_names)


if __name__ == '__main__':
    generate_all()
