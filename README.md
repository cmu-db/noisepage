# NoisePage

[![Jenkins Status](http://jenkins.db.cs.cmu.edu:8080/job/terrier/job/master/badge/icon)](http://jenkins.db.cs.cmu.edu:8080/job/terrier/)
[![codecov](https://codecov.io/gh/cmu-db/terrier/branch/master/graph/badge.svg)](https://codecov.io/gh/cmu-db/terrier)
[![pullreminders](https://pullreminders.com/badge.svg)](https://pullreminders.com?ref=badge)

Welcome to NoisePage!

## Quickstart

Notes:
- You need to be on Ubuntu 20.04 or macOS 10.14+. Nothing else is officially supported.
- If you have less than 8 GB of RAM, use `-DNOISEPAGE_UNITY_BUILD=OFF` in the `cmake` commands below.
- If you know what you're doing, install the prerequisite packages from `./script/installation/packages.sh` manually.

```
git clone https://github.com/cmu-db/noisepage.git
sudo ./script/installation/packages.sh
mkdir build
cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_JEMALLOC=ON -DNOISEPAGE_UNITY_BUILD=ON
ninja noisepage
./bin/noisepage
```

You can now connect to NoisePage over the Postgres client `psql`.
```
psql -h localhost -U terrier -p 15721
```

## For developers

Please see the [docs](https://github.com/cmu-db/noisepage/tree/master/docs/).

## Frequently Asked Questions

1. **Is this Carnegie Mellon's new database system project that is replacing [Peloton](https://github.com/cmu-db/peloton)?**

   Yes.
   
2. **Is the new name of the DBMS "terrier"?**

   No. We have not announced the new name yet but it will not be "terrier". That is the name of [Andy's dog](http://home.bt.com/news/animals/baffled-jack-russell-gets-the-wrong-end-of-the-stick-as-it-tries-to-squeeze-through-dog-flap-11363989001132).
   
3. **When will you announce the new system?**

   We hope to have our first release in 2020.

4. **Will the new system still be "[self-driving](http://www.cs.cmu.edu/~pavlo/blog/2018/04/what-is-a-self-driving-database-management-system.html)"?**

   Yes, our goal is to have this new system to support autonomous operation and optimization as a first-class design principle.

5. **Will the new system still be PostgreSQL compatiable?**
   
   Yes. The DBMS supports the PostgresSQL network protocol (both simple and extended) and emulates PostgresSQL's catalog layout. We are working on support for PL/pgSQL UDFs.

6. **How can I get involved?**
   
   See the [New Student Guide](https://github.com/cmu-db/terrier/wiki/New-Student-Guide). If you are a current student at CMU, then you should consider enrolling in one of the [database courses](https://db.cs.cmu.edu/courses/). Non-CMU students are also welcome to contribute.
