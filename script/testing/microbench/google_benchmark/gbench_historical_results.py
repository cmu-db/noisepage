from statistics import pstdev


class GBenchHistoricalResults(object):
    """ Collection of past microbenchmark results. This provides aggregate
        functions to perform on all historical results.
    """

    def __init__(self, test_suite, test_name):
        self.test_suite = test_suite
        self.test_name = test_name
        self.time_unit = None
        self.gbench_results = []
        self.total_time = 0
        self.total_throughput = 0
        return

    def add_gbench_test_result(self, gbench_test_result):
        """ add a result, ensuring we have a valid input, consistent
            with results being accumulated
        """
        assert self.test_suite == gbench_test_result.suite_name
        assert self.test_name == gbench_test_result.test_name

        if self.time_unit:
            assert self.time_unit == gbench_test_result.time_unit
        else:
            self.time_unit = gbench_test_result.time_unit

        self.total_time = gbench_test_result.get_time_secs()
        self.total_throughput += gbench_test_result.items_per_second

        self.gbench_results.append(gbench_test_result)
        return

    def get_num_results(self):
        return len(self.gbench_results)

    def get_mean_time(self):
        if self.get_num_results() <= 0:
            raise ValueError('Must have at least 1 historical result to calculate mean time')
        return self.total_time / self.get_num_results()

    def get_stdev_time(self):
        if self.get_num_results() <= 0:
            raise ValueError('Must have at least 1 historical result to calculate stdev time')
        return pstdev(map(self.gbench_results, lambda res: res.get_time_secs()))

    def get_mean_throughput(self):
        if self.get_num_results() <= 0:
            raise ValueError('Must have at least 1 historical result to calculate mean throughput')
        return self.total_throughput / self.get_num_results()

    def get_stdev_throughput(self):
        if self.get_num_results() <= 0:
            raise ValueError('Must have at least 1 historical result to calculate stdev throughput')
        return pstdev(map(lambda res: res.items_per_second, self.gbench_results))
