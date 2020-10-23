class GBenchTestResult(object):
    """ A single Google Benchmark result """

    def __init__(self, result_dict):
        """result_dict: single test dict, from json Google Benchmark <file>.json

        sample input
        ----------------
        "name": "DataTableBenchmark/SimpleInsert/real_time",
        "iterations": 5,
        "real_time": 1.2099044392001815e+03,
        "cpu_time": 1.2098839266000000e+03,
        "time_unit": "ms",
        "items_per_second": 8.2651155545892473e+06
        """

        for key, value in result_dict.items():
            if key == 'name':
                self._parse_test_name(value)
                self._parse_time_type(value)
            setattr(self, key, value)

    def _parse_test_name(self, name):
        """ split name into suite_name and test_name """
        parts = name.split('/')
        self.suite_name = parts[0]
        self.test_name = parts[1]
        return

    def _parse_time_type(self, name):
        """ Determine which of the reported times is the correct one to use """
        parts = name.split('/')
        # Check whether the last section of the benchmark contains the
        # string "manual_time". If it does, then we will use the "real_time"
        # measurement. All other benchmarks will just use "cpu_time"
        if parts[-1] == "manual_time":
            self.time_type = "real_time"

        # But it also means that the last section *could be* the arguments for a single
        # benchmark invocation. This means that we need to make sure that we include them in
        # the test name only if there are more than two parts in the fullname, otherwise they will
        # just overwrite each others results (which is a bad thing!)
        else:
            self.time_type = "cpu_time"
            if len(parts) == 3:
                self.test_name += "({})".format(parts[-1])

        return

    def get_time_secs(self):
        """ return execution time, converted to seconds """
        time_unit_scale = {"ms": 10**3,
                           "us": 10**6,
                           "ns": 10**9}
        time_value = getattr(self, self.time_type)
        return float(time_value) / time_unit_scale[self.time_unit]

    def __str__(self):
        ret_strs = []
        for attr, value in self.__dict__.items():
            ret_strs.append("{KEY} : {VAL}\n".format(KEY=attr, VAL=value))
        return ''.join(ret_strs)
