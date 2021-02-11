from util import io_util
from info import data_info


def create_metrics_and_prediction_files(metrics_path, prediction_path, test_only):
    """Create the prediction result files

    :param metrics_path: the file to store the prediction metrics
    :param prediction_path: the file to store the raw predictions
    :param test_only: whether the metrics file is for test only
    :return:
    """
    # First write the header to the result files
    io_util.create_csv_file(metrics_path, ["Method"] + _get_result_labels(test_only))
    io_util.create_csv_file(prediction_path, None)


def record_predictions(pred_results, prediction_path):
    """Record the raw prediction results

    :param pred_results: the data
    :param prediction_path: the file path to score
    :return:
    """
    num_data = pred_results[0].shape[0]
    for i in range(num_data):
        result_list = (list(pred_results[0][i]) + [""] + list(pred_results[1][i]) + [""]
                       + list(pred_results[2][i]))
        io_util.write_csv_result(prediction_path, "", result_list)


def _get_result_labels(test_only):
    labels = []
    if test_only:
        datasets = ["Test"]
    else:
        datasets = ["Train", "Test"]
    for dataset in datasets:
        for target in data_info.instance.MINI_MODEL_TARGET_LIST:
            labels.append(dataset + " " + target.name)
        labels.append("")

    return labels
