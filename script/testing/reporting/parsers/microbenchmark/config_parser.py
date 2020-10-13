def parse_parameters(config):
    """ Compile the microbenchmark parameters used when running the test """
    return {
        'threads': config.num_threads,
        'min_runtime': config.min_time
    }


def parse_wal_device(config):
    """ Determine which label to use for the WAL when publishing results """
    if 'ramdisk' in config.logfile_path:
        return 'RAM disk'
    return 'HDD'
