from abc import abstractmethod, ABC

from oltpbench.test_case_oltp import TestCaseOLTPBench


class DataLoader(ABC):

    @abstractmethod
    def prepare(self):
        """
        Prepares data loader to start loading in data
        """
        pass

    @abstractmethod
    def load(self):
        """
        Starts loading data into NoisePage
        """
        pass


class OLTPBenchDataLoader(DataLoader, TestCaseOLTPBench):

    def __init__(self, args):
        super().__init__()
        # We do not care about the actual test, we just want to load the data
        super().__init__(self, args)
        self.db_execute = False

    def prepare(self):
        pass

    def load(self):
        pass