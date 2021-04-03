from .log_throughput import primary_log_throughput


def start_primary():
    pass


# -wal_enable=true -wal_file_path=wal.log

def start_replica():
    pass


def main():
    # TODO
    primary_log_throughput()


if __name__ == '__main__':
    main()

"""
Look at self_driving stuff


Primary Log Throughput Test(TestOLTPBench):
    run_pre_suit:
        #  start server
        super().run_pre_suite()

Replica Log Throughput Test(TestServer):
    run_pre_suit:
        # start replica
        super().run_pre_suite()
        
Log Throughput Test Case
    __init__:
        super
    
    run_pre_test:
        super
        get current log record id
        
    
        
    run_post_test:
        super
        calculate log throughput
"""
