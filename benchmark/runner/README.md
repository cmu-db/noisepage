# TPCHRunner Instruction
The class TPCHRunner contanins two benchmark, TPC-H benchmark from [TPC.ORG](http://www.tpc.org/tpch/), 
and Star Schema Benchmark, a TPC-H based benchmark described [here](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF).

*How to start:*

1. Clone this repo and compile tpch_runner in the release mode:
from root directory,
```
$ mkdir release
$ cd release
$ cmake -DCMAKE_BUILD_TYPE=Release ..
$ make tpch_runner -j4
```
2. Get table generator for TPC-H and SSB

[TPC-H](https://github.com/malin1993ml/tpl_tables.git)
[SSB](https://github.com/wuwenw/SSB_Table_Generator.git)

Make sure generated tables align with the path in TPCHRunner, i.e.
```
const std::string tpch_table_root_ = "../../../tpl_tables/tables/";
const std::string ssb_dir_ = "../../../ssb_tables/ssb_tables/tables/";
```
For TPCH, use 
```
$ bash gen_tpch.sh <scale_factor> 
```
to generate tables

For SSB, using 1 as the <scale_factor> for example,
```
$ cd ssb-dbgen
$ make clean && make
$ ./dbgen -s 1 -T a
$ python3 convert.py
```
csv files are generated under ../ssb_tables. Note that this will only compile in Linux system.

3. To switch to SSB, change
```
 tpch::Workload::BenchmarkType type = tpch::Workload::BenchmarkType::TPCH;
``` 
to 
```
 tpch::Workload::BenchmarkType type = tpch::Workload::BenchmarkType::SSB;
```