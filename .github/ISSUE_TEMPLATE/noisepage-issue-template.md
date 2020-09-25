---
name: NoisePage Issue Template
about: Please follow this template when creating an issue for NoisePage!
title: ''
labels: ''
assignees: ''

---

Welcome to the issue tracker for **NoisePage**! We're excited that you're interested in improving our system. Below, please **choose either a Feature Request or Bug Report** and replace the sample text below to describe the issue! Additionally, please choose the appropriate labels on the Github panel. If you wish to and are able to solve the issue, feel free to assign yourself; otherwise we will manage this later!

# Feature Request
## Summary
Please provide a short summary of the feature you would like implemented.

## Solution
If possible, include a description of the desired solution you have in mind. Ideally, a series of steps outlining what is required and a plan to implement would be the most helpful for our developers! An [example](https://github.com/cmu-db/noisepage/issues/879) from an issue by @mbutrovich follows:

This is an issue to track and discuss toolchain updates that we may want to explore this summer.
- [ ] Ubuntu 20.04 LTS: this will bump GCC support to 9.3, and also bump any dependencies like TBB
- [ ] LLVM 10 for runtime code generation
- [ ] LLVM 10 for code quality (clang-format, clang-tidy): this may require code changes for new clang-tidy checks
   third_party folder (xxHash, spdlog, json, etc.)

Feel free to suggest more or raise concerns with the existing items. When we're convinced of the list, we should make an overall project with issues for each item in the list to spread out the tasks and track progress.

### Alternatives
If you are aware of any alternatives to the solution you presented, please describe them here!

___

# Bug Report
**Note**: Before filing a bug report, please make sure to check whether the bug has already been filed. If it has, please do not re-file the report, our developers are already hard at work fixing it!

## Summary
Please provide a short summary of the bug observed.

## Environment
To address the bug, especially if it environment specific, we need to know what kind of configuration you are running on. Please include the following:

**OS**: Ubuntu 18.04 (LTS) or macOS 10.14+ (specify version).

**Compiler**: GCC 7.0+ or Clang 8.0+. 

**Note:** we do not support any other toolchains at the moment, so please do not create bug reports if you are building on other environments. Please see the [wiki page](https://github.com/cmu-db/noisepage/wiki/System-Setup) on our supported systems.

## Steps to Reproduce
Whenever possible, retrace how you came about the problem and describe so here. An [example](https://github.com/cmu-db/noisepage/issues/1117) from an issue by @jkosh44 follows:
1. Compile with the following args: -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON
2. Run terrier with parallel execution turned off terrier -parallel_execution=false
3. Run a built in function with an empty string (ex: SELECT STARTS_WITH('HELLO', '');)

If you have an understanding of why the bug occurred, that would be awesome to include as well! In this case, be as descriptive as you can and provide references to lines of code in the code whenever possible! An [example](https://github.com/cmu-db/noisepage/issues/1158) from an issue by @tanujnay112 follows:

`Insert into select` currently doesn't insert anything and does not generate code to do so for a few reasons.
1. InsertTranslator doesn't call prepare on its children if such children exist.
2. The InsertPlanNode needs to have DerivedValueExpressions in its values_ vector to signify tuples that are derived from its children for insertion. This should be done in plan_generator.cpp. Otherwise, there is nothing in the values_ vector during INSERT INTO SELECT and the insert translator generates nothing.
3. Also, in order for the children of the insert node to pass up tuples the InsertSelect node needs to request the required columns from the descendant scan node [here](https://github.com/cmu-db/terrier/blob/d60db2a543eed2c0463d2fae02eb4bc39628b4f8/src/optimizer/input_column_deriver.cpp#L233). Currently this just passes down required columns from above which is incorrect. You can't request columns from an Insert node. Also, at this point we must generate the required columns to request from the children. In the event that we have a INSERT INTO yyy SELECT * FROM xxx;, we can easily just query all the columns oids in xxx and request those columns. The tableoid of xxx is stored in the child of the concerned InsertSelect node.
4. The information stored in InsertSelect is incomplete. While the vanilla Insert physical operator node has this that allows us to know which columns we are inserting into, the InsertSelect operator has no equivalent even though this information is necessary.

### Expected Behavior
Fill in the behavior expected from our system, as well as the reference you used.

Useful things to include here are snippets of past correct functionality (if something broke), references to documentation from PostgreSQL, or references to other database implementations. An [example](https://github.com/cmu-db/noisepage/issues/1150) from an issue by @lmwnshn follows:

```SQL
postgres=# create table foo (a int);
CREATE TABLE
postgres=# insert into foo values (1);
INSERT 0 1
postgres=# insert into foo values (1);
INSERT 0 1
postgres=# insert into foo values (2);
INSERT 0 1
postgres=# select a, sum(a) from foo group by a;
 a | sum
---+-----
 2 |   2
 1 |   2
(2 rows)
postgres=# select a, sum(a) from foo group by a order by a;
 a | sum
---+-----
 1 |   2
 2 |   2
(2 rows)
```

### Actual Behavior
Fill in the behavior you actually observed for our system.

Useful things to include here are code snippets showing the command run and the output observed and screenshots of issues. An [example](https://github.com/cmu-db/noisepage/issues/1150) from an issue by @lmwnshn follows:
```SQL
terrier=# create table foo (a int);
CREATE TABLE
terrier=# insert into foo values (1);
INSERT 0 1
terrier=# insert into foo values (1);
INSERT 0 1
terrier=# insert into foo values (2);
INSERT 0 1
terrier=# select a, sum(a) from foo group by a;
 a | ?column?
---+----------
 1 |        2
 2 |        2
(2 rows)
terrier=# select a, sum(a) from foo group by a order by a;
 a | ?column?
---+----------
(0 rows)
```
