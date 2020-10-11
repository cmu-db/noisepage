def parse_parameters(config):
    return {
        'threads': config.num_threads,
        'min_time': config.min_time
    }

def parse_wal_device(config):
    if 'ramdisk' in config.logfile_path:
        return 'RAM disk'
    return 'HDD'