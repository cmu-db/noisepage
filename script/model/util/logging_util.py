import logging


def init_logging(loglevel):
    """Initialize the logging module

    :param loglevel: the logging level to set
    """
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', level=numeric_level)