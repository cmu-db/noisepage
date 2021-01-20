<a href="https://noise.page/">
<img src="https://noise.page/logos/noisepage-horizontal.svg" alt="NoisePage Logo" height="200">
</a>

-----------------

[![Jenkins Status](http://jenkins.db.cs.cmu.edu:8080/job/terrier/job/master/badge/icon)](http://jenkins.db.cs.cmu.edu:8080/job/terrier/)
[![codecov](https://codecov.io/gh/cmu-db/noisepage/branch/master/graph/badge.svg)](https://codecov.io/gh/cmu-db/noisepage)

[NoisePage](https://noise.page) is a relational database management system developed by the [Carnegie Mellon Database Group](https://db.cs.cmu.edu). The research goal of the NoisePage project is to develop high-performance system components that support autonomous operation and optimization as a first-class design principle.

## Key Features
* Integrated machine learning components to forecast, model, and plan the system's behavior.
* Postgres compatible wire-protocol, SQL, and catalogs.
* [Apache Arrow](https://arrow.apache.org/) compatible in-memory columnar storage.
* Lock-free multi-version concurrency control.
* Just-in-time query compilation using the LLVM.
* Vectorized execution using relaxed-operator fusion (ROF).
* 100% Open-Source (MIT License)

## Quickstart
The NoisePage project is built and tested on **Ubuntu 20.04**. No other environments are officially supported.

```
git clone https://github.com/cmu-db/noisepage.git
cd noisepage
sudo ./script/installation/packages.sh
mkdir build
cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_JEMALLOC=ON -DNOISEPAGE_UNITY_BUILD=ON ..
ninja noisepage
./bin/noisepage
```

You can now connect to NoisePage over the Postgres client `psql`.
```
psql -h localhost -U noisepage -p 15721
```

Additional Notes:
- If you have less than 16 GB of RAM, use `-DNOISEPAGE_UNITY_BUILD=OFF` in the `cmake` commands above.
- If you know what you're doing, install the prerequisite packages from `./script/installation/packages.sh` manually.


## For Developers

Please see the [docs](https://github.com/cmu-db/noisepage/tree/master/docs/).


## Contributing

If you are a current student at CMU,
  - See the [New Student Guide](https://github.com/cmu-db/noisepage/tree/master/docs/). 
  - Consider enrolling in one of the [database courses](https://db.cs.cmu.edu/courses/).

Contributions from non-CMU students are also welcome!
