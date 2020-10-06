# Configuration file for OLTPBench
To run a OLTPBench test, you should run the `run_oltpbench.py --config-file=config.json`. 

In the configuration file, those information are required:
1. A list of test cases in `testcases`
2. The benchmarks and options in each test case, required by the oltpbench's workload descriptor file

The `run_oltpbench` script will run all test cases in the configuration file sequentially. 

The `loop` key in the configuration file is used to duplicate the test case with different options.

The `server_args` filed in the configuration specify the server commandline args.
## Example:

<pre>
{
    "type": "oltpbenchmark",
    "server_args":{
        "connection_thread_count": 32,
        "wal_file_path": "/mnt/ramdisk/wal.log"    
    },
    "testcases": [
        {
            "base": {
                "benchmark": "tatp",
                "weights": "2,35,10,35,2,14,2",
                "query_mode": "extended",
                "scale_factor": 1,
                "terminals": 1,
                "loader_threads": 4,
                "client_time": 60
            },
            "loop": [
                {"terminals":1, "db_create":true,"db_load":true},
                {"terminals":2, "db_restart":false,"db_create":false,"db_load":false},
                {"terminals":4, "db_restart":false,"db_create":false,"db_load":false},
            ]
        },
        {
            "base": {
                "benchmark": "tatp",
                "weights": "2,35,10,35,2,14,2",
                "query_mode": "extended",
                "terminals": 8,
                "loader_threads": 4,
                "client_time": 600
            }
        }
    ]
}
</pre>