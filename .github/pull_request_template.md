Welcome to the PR tracker for **NoisePage**! We're excited that you're interested in improving our system.

**Before continuing with opening a PR**, please read through the **[Pull Request Process](https://github.com/cmu-db/noisepage/wiki/Pull-Request-Process)** page on our wiki. PRs that do not follow our guidelines will be immediately closed. In general, you should avoid creating a PR until you are reasonably confident the tests should pass, having tested locally first.

Please choose the appropriate labels on the Github panel and feel free to assign yourself. Additionally, if your PR solves an open issue, please link the issue on the Github panel. However, please **DO NOT** assign any reviewers to your PR. We will decide who best to assign for a review.


# Heading
Please choose an appropriate heading for your PR, relevant to the changes you have made. For example, if your PR addresses the `LIMIT` clause on `IndexScan`s, an appropriate PR name would be `Index Scan Limit`.

## Description
Please create a description of the issue your PR solves, and how you went about implementing your solution. An [example](https://github.com/cmu-db/noisepage/issues/879) from a PR by @thepinetree follows:

Limit clauses are currently not propagated to the `IndexScanPlanNode` in the optimizer and as a result, the execution engine can't take advantage of the limit during operation. Instead, this is done in-post, with a `LimitPlanNode` doing so after the index scan is completed.

This PR adds functionality for the limit value to be pushed down to an index scan, and is used in TPC-C. Limits values will be pushed down to their child `LogicalGet` via transformation rule and converted to values in the `PhysicalIndexScan` which are then set in the `IndexScanPlanNode`. To appropriately act on the `Limit` value, we also add infrastructure for optional properties for a child to satisfy, which is tracked only in an `Optimizer` node. The PR also moves the `OrderByOrderingType` from the optimizer to the catalog as a precursor to further changes to involve the sort direction of columns in creating/scanning an index.

### Remaining Tasks
Again, you should only create PR once you are reasonably confident you are near completion. However, if there are some tasks still remaining before the PR is ready to merge, please create a checklist to track active progress. An [example](https://github.com/cmu-db/noisepage/issues/1031) from a PR by @thepinetree follows:

:pushpin: TODOs:
- [x] ~~Stash limit in OptimizerContext for pushdown (INVALID)~~
- [x] Move ordering type to catalog
- [x] Add transformation rule for limit pushdown
- [x] Add optional property support
- [x] Fix memory leaks
- [ ] Add GitHub issues for OrderingType investigation, physical prune stage, and TPL break statement

## Performance
If your PR has the potential to greatly affect the performance of the system, please address these by benchmarking your changes with respect to master, or profiling the performance. You may do this in one of the following ways:
1. Inline a table outlining performance results. An [example](https://github.com/cmu-db/noisepage/pull/1109) from a PR @gonzalezjo follows:

    | Machine Type 	| Terminals 	| Scale Factor 	| Socket Type 	| Transactions / Second 	|
    |--------------	|-----------	|--------------	|-------------	|-----------------------	|
    | Bare metal   	| 10        	| 10           	| UNIX        	| 8346 (+34%)           	|
    | Bare metal   	| 10        	| 10           	| INET        	| 6214                  	|
    | Bare metal   	| 1         	| 1            	| UNIX        	| 914 (+37%)            	|
    | Bare metal   	| 1         	| 1            	| INET        	| 668                   	|
    | VMware       	| 10        	| 10           	| UNIX        	| 3728 (+24%)           	|
    | VMware       	| 10        	| 10           	| INET        	| 3005                  	|
    | VMware       	| 1         	| 1            	| UNIX        	| 289 (+28%)            	|
    | VMware       	| 1         	| 1            	| INET        	| 226                   	|

2. Create a Google Sheets document noting baseline performance and scalability, as is done [here](https://docs.google.com/spreadsheets/d/1eng7O98KaG0fJn6SVavquPr2zipzp5Ju9FtQylHPV2o/edit?usp=sharing) in an example from @mbutrovich
3. Create an SVG of profiling results per the [EC2 profiling instructions](https://github.com/cmu-db/noisepage/wiki/Profiling-on-EC2), as is done [here](https://drive.google.com/file/d/1xSn1o7RyazbvnyKjlxMets1H7gQlQ2WO/view?usp=sharing) in an example from @thepinetree

## Further Work
If your PR unlocked the potential for further improvement, please note them here and create additional issues! Do the same if you discovered bugs in the process of development. An [example](https://github.com/cmu-db/noisepage/pull/1109) from a PR by @gonzalezjo follows:

### Investigating loopback and TCP overhead

This is probably a dead end, but less of a dead end than libevent/epoll stuff.

I'd like to work on improving our INET socket overhead, but at this point, that might not be doable without being a kernel engineer. Nothing I tried measurably reduced loopback and/or INET overhead, and I tried a lot. Still, I think it's worth digging some more.

One question I have is how much of the speedup comes from avoiding TCP, and how much comes from using a glorified pipe instead of loopback. This would be interesting to know, but I have no idea how I'd measure, and I'm not convinced that I'd be able to make anything useful from the answer.

---
Here's an empty template to format yourself!
# Heading

## Description

## Remaining tasks

- [ ] Foo
- [ ] Bar
- [ ] Baz

## Performance

## Further work