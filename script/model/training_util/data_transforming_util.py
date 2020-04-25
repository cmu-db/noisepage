import numpy as np

from info import data_info
from type import OpUnit


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

# Map the opunit to the transformer it needs for mini-model training
OPUNIT_MODELING_TRANSFORMER_MAP = {
    OpUnit.GC_DEALLOC: None,
    OpUnit.GC_UNLINK: None,
    OpUnit.LOG_SERIAL: None,
    OpUnit.LOG_CONSUME: None,
    OpUnit.TXN_BEGIN: None,
    OpUnit.TXN_COMMIT: None,

    # Execution engine opunits
    OpUnit.SEQ_SCAN: _tuple_num_linear_transformer,
    OpUnit.IDX_SCAN: _tuple_num_linear_transformer,
    OpUnit.HASHJOIN_BUILD: _tuple_num_linear_transformer,
    OpUnit.HASHJOIN_PROBE: _tuple_num_linear_transformer,
    OpUnit.AGG_BUILD: _tuple_num_linear_transformer,
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

    OpUnit.SORT_BUILD: _tuple_num_linear_log_transformer,
}
