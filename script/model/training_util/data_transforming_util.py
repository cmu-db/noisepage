import numpy as np

from info import data_info
from type import OpUnit, Target


def _tuple_num_linear_train_transform(x, y):
    # Linearly transform down the target according to the tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    return y / tuple_num[:, np.newaxis]


def _tuple_num_linear_predict_transform(x, y):
    # Linearly transform up the target according to the tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    return y * tuple_num[:, np.newaxis]


# Transform the target linearly according to the tuple num
_tuple_num_linear_transformer = (_tuple_num_linear_train_transform, _tuple_num_linear_predict_transform)


def _tuple_num_memory_cardinality_linear_train_transform(x, y):
    # Linearly transform down the target according to the tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    new_y = y / tuple_num[:, np.newaxis]
    # Transform the memory consumption based on the cardinality
    cardinality = np.copy(x[:, data_info.CARDINALITY_INDEX])
    new_y[:, data_info.TARGET_CSV_INDEX[Target.MEMORY_B]] *= tuple_num
    # Having a 250 offset since below roughly that the memory consumption is constant (while fixing other features)
    new_y[:, data_info.TARGET_CSV_INDEX[Target.MEMORY_B]] /= cardinality + 250
    return new_y


def _tuple_num_memory_cardinality_linear_predict_transform(x, y):
    # Linearly transform up the target according to the tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    new_y = y * tuple_num[:, np.newaxis]
    # Transform the memory consumption based on the cardinality
    cardinality = np.copy(x[:, data_info.CARDINALITY_INDEX])
    new_y[:, data_info.TARGET_CSV_INDEX[Target.MEMORY_B]] /= tuple_num
    new_y[:, data_info.TARGET_CSV_INDEX[Target.MEMORY_B]] *= cardinality + 250
    return new_y


# Transform the target linearly according to the tuple num
_tuple_num_memory_cardinality_linear_transformer = (_tuple_num_memory_cardinality_linear_train_transform,
                                                    _tuple_num_memory_cardinality_linear_predict_transform)


def _tuple_num_linear_log_train_transform(x, y):
    # Transform down the target according to the linear-log (nlogn) tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    return y / (tuple_num * np.log2(tuple_num) + 1e-6)[:, np.newaxis]


def _tuple_num_linear_log_predict_transform(x, y):
    # Transform up the target according to the linear-log (nlogn) tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    return y * (tuple_num * np.log2(tuple_num) + 1e-6)[:, np.newaxis]


# Transform the target in a linear-log way (nlogn) according to the tuple num
_tuple_num_linear_log_transformer = (_tuple_num_linear_log_train_transform, _tuple_num_linear_log_predict_transform)


def _tuple_num_log_train_transform(x, y):
    # Transform down the target according to the log tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    return y / (np.log2(tuple_num) + 1e-6)[:, np.newaxis]


def _tuple_num_log_predict_transform(x, y):
    # Transform up the target according to the log tuple num value in the input
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    return y * (np.log2(tuple_num) + 1e-6)[:, np.newaxis]


# Transform the target in a log scale (logn) according to the tuple num
_tuple_num_log_transformer = (_tuple_num_log_train_transform, _tuple_num_log_predict_transform)

# Map the opunit to the output (y) transformer it needs for mini-model training
OPUNIT_Y_TRANSFORMER_MAP = {
    OpUnit.GC: None,
    OpUnit.LOG_SERIALIZER_TASK: None,
    OpUnit.DISK_LOG_CONSUMER_TASK: None,
    OpUnit.TXN_BEGIN: None,
    OpUnit.TXN_COMMIT: None,

    # Execution engine opunits
    OpUnit.SEQ_SCAN: _tuple_num_linear_transformer,
    OpUnit.HASHJOIN_BUILD: _tuple_num_linear_transformer,
    OpUnit.HASHJOIN_PROBE: _tuple_num_linear_transformer,
    OpUnit.AGG_ITERATE: _tuple_num_linear_transformer,
    OpUnit.SORT_ITERATE: _tuple_num_linear_transformer,
    OpUnit.INSERT: _tuple_num_linear_transformer,
    OpUnit.UPDATE: _tuple_num_linear_transformer,
    OpUnit.DELETE: _tuple_num_linear_transformer,
    OpUnit.OP_INTEGER_PLUS_OR_MINUS: _tuple_num_linear_transformer,
    OpUnit.OP_INTEGER_MULTIPLY: _tuple_num_linear_transformer,
    OpUnit.OP_INTEGER_DIVIDE: _tuple_num_linear_transformer,
    OpUnit.OP_INTEGER_COMPARE: _tuple_num_linear_transformer,
    OpUnit.OP_DECIMAL_PLUS_OR_MINUS: _tuple_num_linear_transformer,
    OpUnit.OP_DECIMAL_MULTIPLY: _tuple_num_linear_transformer,
    OpUnit.OP_DECIMAL_DIVIDE: _tuple_num_linear_transformer,
    OpUnit.OP_DECIMAL_COMPARE: _tuple_num_linear_transformer,
    OpUnit.OUTPUT: _tuple_num_linear_transformer,

    OpUnit.IDX_SCAN: _tuple_num_log_transformer,
    OpUnit.SORT_BUILD: _tuple_num_linear_log_transformer,
    OpUnit.CREATE_INDEX: _tuple_num_linear_log_transformer,

    OpUnit.AGG_BUILD: _tuple_num_memory_cardinality_linear_transformer,
}


def _tuple_num_cardinality_linear_train_transform(x):
    # Linearly divide the cardinality by the tuple num
    tuple_num = np.copy(x[:, data_info.TUPLE_NUM_INDEX])
    new_x = x * 1.0
    new_x[:, data_info.CARDINALITY_INDEX] /= tuple_num + 1
    return new_x


# Map the opunit to the input (x) transformer it needs for mini-model training
OPUNIT_X_TRANSFORMER_MAP = {
    OpUnit.GC: None,
    OpUnit.LOG_SERIALIZER_TASK: None,
    OpUnit.DISK_LOG_CONSUMER_TASK: None,
    OpUnit.TXN_BEGIN: None,
    OpUnit.TXN_COMMIT: None,

    # Execution engine opunits
    OpUnit.SEQ_SCAN: None,
    OpUnit.AGG_ITERATE: None,
    OpUnit.SORT_ITERATE: None,
    OpUnit.INSERT: None,
    OpUnit.UPDATE: None,
    OpUnit.DELETE: None,
    OpUnit.OP_INTEGER_PLUS_OR_MINUS: None,
    OpUnit.OP_INTEGER_MULTIPLY: None,
    OpUnit.OP_INTEGER_DIVIDE: None,
    OpUnit.OP_INTEGER_COMPARE: None,
    OpUnit.OP_DECIMAL_PLUS_OR_MINUS: None,
    OpUnit.OP_DECIMAL_MULTIPLY: None,
    OpUnit.OP_DECIMAL_DIVIDE: None,
    OpUnit.OP_DECIMAL_COMPARE: None,
    OpUnit.OUTPUT: None,
    OpUnit.IDX_SCAN: None,
    OpUnit.CREATE_INDEX: None,

    # Transform the opunits for which the ratio between the tuple num and the cardinality impacts the behavior
    OpUnit.HASHJOIN_BUILD: _tuple_num_cardinality_linear_train_transform,
    OpUnit.HASHJOIN_PROBE: _tuple_num_cardinality_linear_train_transform,
    OpUnit.AGG_BUILD: _tuple_num_cardinality_linear_train_transform,
    OpUnit.SORT_BUILD: _tuple_num_cardinality_linear_train_transform,
}
