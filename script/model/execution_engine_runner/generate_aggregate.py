#!/usr/bin/env python3

def GetTypeInfo(type):
    if type == 'Integer':
        get_name = "Int"
        type_feature = "1, 0, 0, 0"
    if type == 'Real':
        get_name = "Double"
        type_feature = "0, 1, 0, 0"

    return get_name, type_feature

# Generate the key type for the join
def GenerateBuildKey(col_num, type):
    print("struct BuildKey{}{} {{".format(type, col_num))
    for i in range(col_num):
        print("  c{} : {}".format(i + 1, type))
    print("}")
    print()

# Generate the join struct (key and value)
def GenerateBuildRow(col_num, type):
    print("struct BuildRow{}{} {{".format(type, col_num))
    print("  key: BuildKey{}{}".format(type, col_num))
    print("}")
    print()

def GenerateKeyCheck(col_num, type):
    get_name, _ = GetTypeInfo(type)

    print("fun keyCheck{}{}(row: *BuildRow{}{}, pci: *ProjectedColumnsIterator) -> bool {{".format(type, col_num,
                                                                                                 type, col_num))
    print("  return @sqlToBool(@pciGet{}(pci, {}) == row.key.c1)".format(get_name, col_num - 1), end="")
    for i in range(1, col_num):
        print(" and @sqlToBool(@pciGet{}(pci, {}) == row.key.c{})".format(get_name, col_num - 1, i + 1), end="")
    print("\n}\n")

def GenerateBuildSide(col_num, row_num, cardinality, type):
    get_name, type_feature = GetTypeInfo(type)

    fun_name = "build{}Col{}Row{}Car{}".format(type, col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartTimer(execCtx)")

    print("  var ht = &state.table{}{}".format(type, col_num)) # join hash table

    # table iterator
    print("  var tvi: TableVectorIterator")
    print("  var col_oids : [{}]uint32".format(col_num))
    for i in range(0, col_num):
        print("  col_oids[{}] = {}".format(i, 5 - i))

    print("  @tableIterInitBind(&tvi, execCtx, \"{}Col5Row{}Car{}\", col_oids)".format(type, row_num, cardinality))

    print("  for (@tableIterAdvance(&tvi)) {")
    print("    var vec = @tableIterGetPCI(&tvi)")
    print("    for (; @pciHasNext(vec); @pciAdvance(vec)) {")

    # calculate the join key
    print("      var hash_val = @hash(@pciGet{}(vec, {})".format(get_name, col_num - 1), end="")
    for i in range(1, col_num):
        print(", @pciGet{}(vec, {})".format(get_name, col_num - 1), end="")
    print(")")
    # insert into the join table
    print("      var agg = @ptrCast(*BuildRow{}{}, @aggHTLookup(ht, hash_val, keyCheck{}{}, vec))".format(type,
            col_num, type, col_num))
    print("      if (agg == nil) {")
    # construct the agg element
    print("        agg = @ptrCast(*BuildRow{}{}, @aggHTInsert(ht, hash_val))".format(type, col_num))
    print("        agg.key.c1 = @pciGet{}(vec, {})".format(get_name, col_num - 1))
    for i in range(1, col_num):
        print("        agg.key.c{} = @pciGet{}(vec, {})".format(i + 1, get_name, col_num - 1))
    print("      }")

    print("    }")
    print("  }")
    print("  @tableIterClose(&tvi)")

    print("  @execCtxEndTimer(execCtx, @stringToSql(\"aggbuild, {}, {}, {}, {}\"))".format(row_num, col_num,
                                                                                          cardinality, type_feature))
    print("}")

    print()

    return fun_name

def GenerateProbeSide(col_num, row_num, cardinality, type):
    _, type_feature = GetTypeInfo(type)

    fun_name = "probe{}Col{}Row{}Car{}".format(type, col_num, row_num, cardinality)
    print("fun {}(execCtx: *ExecutionContext, state: *State) -> nil {{".format(fun_name))
    print("  @execCtxStartTimer(execCtx)")

    # construct the iterator
    print("  var agg_ht_iter: AggregationHashTableIterator")
    print("  var iter = &agg_ht_iter")

    # iterate through the agg table
    print("  for (@aggHTIterInit(iter, &state.table{}{}); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {{".format(
        type, col_num))
    print("    var agg = @ptrCast(*BuildRow{}{}, @aggHTIterGetRow(iter))".format(type, col_num))
    print("    state.num_matches = state.num_matches + 1")
    print("  }")
    print("  @aggHTIterClose(iter)")

    print("  @execCtxEndTimer(execCtx, @stringToSql(\"aggprobe, {}, {}, {}, {}\"))".format(row_num, col_num,
                                                                                          cardinality, type_feature))
    print("}")

    print()

    return fun_name


def GenerateState(col_nums, types):
    print("struct State {")
    for i in col_nums:
        for type in types:
            print("  table{}{} : AggregationHashTable".format(type, i))
    print("  num_matches : int64")
    print("}\n")

def GenerateTearDown(col_nums, types):
    print("fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        for type in types:
            print("  @aggHTFree(&state.table{}{})".format(type, i))
    print("}\n")


def GenerateSetup(col_nums, types):
    print("fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {")
    for i in col_nums:
        for type in types:
            print("  @aggHTInit(&state.table{}{}, @execCtxGetMem(execCtx), @sizeOf(BuildRow{}{}))".format(type, i,
                                                                                                          type, i))

    print("  state.num_matches = 0")
    print("}\n")

def GenerateMainFun(fun_names):
    print("fun main(execCtx: *ExecutionContext) -> int32 {")
    print("  var state: State")

    n = len(fun_names)

    for i, fun_name in enumerate(fun_names):
        if "build" in fun_name:
            print("\n  setUpState(execCtx, &state)")
        print("  {}(execCtx, &state)".format(fun_name))

        if "probe" in fun_name:
            print("  tearDownState(execCtx, &state)")

    print("\n  return state.num_matches")
    print("}")


def GenerateAll():
    #agg_types = ['IntegerSumAggregate', 'CountStarAggregate', 'IntegerAvgAggregate', 'IntegerMinAggregate',
    #             'IntegerMaxAggregate']
    fun_names = []
    col_nums = range(1, 6)
    row_nums = [1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
                200000, 500000, 1000000]
    #row_nums = [1000000]
    cardinalities = [1, 2, 5, 10, 50, 100]
    types = ['Integer']
    #types = ['Integer', "Real"]

    for col_num in col_nums:
        for type in types:
            GenerateBuildKey(col_num, type)
            GenerateBuildRow(col_num, type)
            GenerateKeyCheck(col_num, type)

    GenerateState(col_nums, types)
    GenerateSetup(col_nums, types)
    GenerateTearDown(col_nums, types)

    for col_num in col_nums:
        for row_num in row_nums:
            for cardinality in cardinalities:
                for type in types:
                    fun_names.append(GenerateBuildSide(col_num, row_num, cardinality, type))
                    fun_names.append(GenerateProbeSide(col_num, row_num, cardinality, type))

    GenerateMainFun(fun_names)

if __name__ == '__main__':
    GenerateAll()
