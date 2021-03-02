import numpy as np

from ..info import data_info
from ..type import OpUnit, Target, ExecutionFeature

_TRANSFORM_EPSILON = 1


def _num_rows_linear_train_transform(x, y):
    # Linearly transform down the target according to the num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    return y / tuple_num[:, np.newaxis]


def _num_rows_linear_predict_transform(x, y):
    # Linearly transform up the target according to the num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    return y * tuple_num[:, np.newaxis]


# Transform the target linearly according to the num_rows
_num_rows_linear_transformer = (_num_rows_linear_train_transform, _num_rows_linear_predict_transform)


def _num_rows_memory_cardinality_linear_train_transform(x, y):
    # Linearly transform down the target according to the num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]]) + _TRANSFORM_EPSILON
    new_y = y / tuple_num[:, np.newaxis]
    # Transform the memory consumption based on the cardinality
    cardinality = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]])
    new_y[:, data_info.instance.target_csv_index[Target.MEMORY_B]] *= tuple_num
    # Having a 250 offset since below roughly that the memory consumption is constant (while fixing other features)
    new_y[:, data_info.instance.target_csv_index[Target.MEMORY_B]] /= cardinality + 250
    return new_y


def _num_rows_memory_cardinality_linear_predict_transform(x, y):
    # Linearly transform up the target according to the num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]]) + _TRANSFORM_EPSILON
    new_y = y * tuple_num[:, np.newaxis]
    # Transform the memory consumption based on the cardinality
    cardinality = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]])
    new_y[:, data_info.instance.target_csv_index[Target.MEMORY_B]] /= tuple_num
    new_y[:, data_info.instance.target_csv_index[Target.MEMORY_B]] *= cardinality + 250
    return new_y


# Transform the target linearly according to the num_rows
_num_rows_memory_cardinality_linear_transformer = (_num_rows_memory_cardinality_linear_train_transform,
                                                   _num_rows_memory_cardinality_linear_predict_transform)


def _num_rows_log_cardinality_linear_train_transform(x, y):
    # Transform down the target in log scale according to the num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    new_y = y / (np.log2(tuple_num) + _TRANSFORM_EPSILON)[:, np.newaxis]
    # Transform linearly again based on the cardinality
    cardinality = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]])
    return new_y / cardinality[:, np.newaxis]


def _num_rows_log_cardinality_linear_predict_transform(x, y):
    # Transform up the target in log scale according to the num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    new_y = y * (np.log2(tuple_num) + _TRANSFORM_EPSILON)[:, np.newaxis]
    # Transform linearly again based on the cardinality
    cardinality = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]])
    return new_y * cardinality[:, np.newaxis]


# Transform the target linearly according to the num_rows
_num_rows_log_cardinality_linear_transformer = (_num_rows_log_cardinality_linear_train_transform,
                                                _num_rows_log_cardinality_linear_predict_transform)


def _num_rows_linear_log_train_transform(x, y):
    # Transform down the target according to the linear-log (nlogn) num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    return y / (tuple_num * np.log2(tuple_num) + _TRANSFORM_EPSILON)[:, np.newaxis]


def _num_rows_linear_log_predict_transform(x, y):
    # Transform up the target according to the linear-log (nlogn) num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    return y * (tuple_num * np.log2(tuple_num) + _TRANSFORM_EPSILON)[:, np.newaxis]


# Transform the target in a linear-log way (nlogn) according to the num_rows
_num_rows_linear_log_transformer = (_num_rows_linear_log_train_transform, _num_rows_linear_log_predict_transform)


def _num_rows_log_train_transform(x, y):
    # Transform down the target according to the log num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    return y / (np.log2(tuple_num) + _TRANSFORM_EPSILON)[:, np.newaxis]


def _num_rows_log_predict_transform(x, y):
    # Transform up the target according to the log num_rows value in the input
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    return y * (np.log2(tuple_num) + _TRANSFORM_EPSILON)[:, np.newaxis]


# Transform the target in a log scale (logn) according to the num_rows
_num_rows_log_transformer = (_num_rows_log_train_transform, _num_rows_log_predict_transform)


def _cardinality_linear_train_transform(x, y):
    # Transform down the target according to the cardinality in the input
    cardinality = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]])
    return y / (cardinality + _TRANSFORM_EPSILON)[:, np.newaxis]


def _cardinality_linear_predict_transform(x, y):
    # Transform up the target according to the cardinality in the input
    cardinality = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]])
    return y * (cardinality + _TRANSFORM_EPSILON)[:, np.newaxis]


# Transform the target in a linear scale (cardinality)
_cardinality_linear_transformer = (_cardinality_linear_train_transform,
                                   _cardinality_linear_predict_transform)

# Map the opunit to the output (y) transformer it needs for mini-model training
OPUNIT_Y_TRANSFORMER_MAP = {
    OpUnit.GC: None,
    OpUnit.LOG_SERIALIZER_TASK: None,
    OpUnit.DISK_LOG_CONSUMER_TASK: None,
    OpUnit.TXN_BEGIN: None,
    OpUnit.TXN_COMMIT: None,

    # Execution engine opunits
    OpUnit.SEQ_SCAN: _num_rows_linear_transformer,
    OpUnit.HASHJOIN_BUILD: _num_rows_linear_transformer,
    OpUnit.HASHJOIN_PROBE: _num_rows_linear_transformer,
    OpUnit.AGG_ITERATE: _num_rows_linear_transformer,
    OpUnit.SORT_ITERATE: _num_rows_linear_transformer,
    OpUnit.INSERT: _num_rows_linear_transformer,
    OpUnit.UPDATE: _num_rows_linear_transformer,
    OpUnit.DELETE: _num_rows_linear_transformer,
    OpUnit.OP_INTEGER_PLUS_OR_MINUS: _num_rows_linear_transformer,
    OpUnit.OP_INTEGER_MULTIPLY: _num_rows_linear_transformer,
    OpUnit.OP_INTEGER_DIVIDE: _num_rows_linear_transformer,
    OpUnit.OP_INTEGER_COMPARE: _num_rows_linear_transformer,
    OpUnit.OP_REAL_PLUS_OR_MINUS: _num_rows_linear_transformer,
    OpUnit.OP_REAL_MULTIPLY: _num_rows_linear_transformer,
    OpUnit.OP_REAL_DIVIDE: _num_rows_linear_transformer,
    OpUnit.OP_REAL_COMPARE: _num_rows_linear_transformer,
    OpUnit.OP_VARCHAR_COMPARE: _num_rows_linear_transformer,
    OpUnit.OUTPUT: _num_rows_linear_transformer,

    # OpUnit.IDX_SCAN: _num_rows_log_transformer,
    # Another possible alternative
    OpUnit.IDX_SCAN: _num_rows_log_cardinality_linear_transformer,
    OpUnit.SORT_BUILD: _num_rows_linear_log_transformer,
    OpUnit.CREATE_INDEX: _num_rows_linear_log_transformer,
    OpUnit.CREATE_INDEX_MAIN: _num_rows_linear_log_transformer,

    OpUnit.AGG_BUILD: _num_rows_memory_cardinality_linear_transformer,
    OpUnit.SORT_TOPK_BUILD: _num_rows_memory_cardinality_linear_transformer,

    OpUnit.PARALLEL_MERGE_HASHJOIN: _num_rows_linear_transformer,
    OpUnit.PARALLEL_MERGE_AGGBUILD: _num_rows_linear_transformer,
    OpUnit.PARALLEL_SORT_STEP: _num_rows_linear_log_transformer,
    OpUnit.PARALLEL_SORT_MERGE_STEP: _num_rows_linear_log_transformer,

    OpUnit.INDEX_INSERT: _num_rows_log_cardinality_linear_transformer,
    OpUnit.INDEX_DELETE: _cardinality_linear_transformer,
}


def _num_rows_cardinality_linear_train_transform(x):
    # Linearly divide the cardinality by the num_rows
    tuple_num = np.copy(x[:, data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]])
    new_x = x * 1.0
    new_x[:, data_info.instance.input_csv_index[ExecutionFeature.EST_CARDINALITIES]] /= tuple_num + _TRANSFORM_EPSILON
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
    OpUnit.OP_REAL_PLUS_OR_MINUS: None,
    OpUnit.OP_REAL_MULTIPLY: None,
    OpUnit.OP_REAL_DIVIDE: None,
    OpUnit.OP_REAL_COMPARE: None,
    OpUnit.OP_VARCHAR_COMPARE: None,
    OpUnit.OUTPUT: None,
    OpUnit.IDX_SCAN: None,
    OpUnit.CREATE_INDEX: None,
    OpUnit.CREATE_INDEX_MAIN: None,

    # Transform the opunits for which the ratio between the num_rows and the cardinality impacts the behavior
    OpUnit.HASHJOIN_BUILD: _num_rows_cardinality_linear_train_transform,
    OpUnit.HASHJOIN_PROBE: _num_rows_cardinality_linear_train_transform,
    OpUnit.AGG_BUILD: _num_rows_cardinality_linear_train_transform,
    OpUnit.SORT_BUILD: _num_rows_cardinality_linear_train_transform,
    OpUnit.SORT_TOPK_BUILD: _num_rows_cardinality_linear_train_transform,

    OpUnit.PARALLEL_MERGE_HASHJOIN: None,
    OpUnit.PARALLEL_MERGE_AGGBUILD: None,
    OpUnit.PARALLEL_SORT_STEP: None,
    OpUnit.PARALLEL_SORT_MERGE_STEP: None,
    OpUnit.INDEX_INSERT: None,
    OpUnit.INDEX_DELETE: None,
}
