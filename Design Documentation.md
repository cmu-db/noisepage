# Project Design Document

# Supporting Nested QuerySupporting Nested Query


## Overview
In this project, our initial goal is to support four types of nested queries mentioned in Kim's paper, including type-A, type-N, type-J and type-JA. Currently both type-N and type-J queries are supported in the optimizer with the UnnestMarkJoinToInnerJoin rule. However, executing them correctly requires the support of in operation. For  type-A and type-JA queries that involves aggregation operators, materialization techniques are required in order to complete the optimization process. Furthermore, we are determined to support views by rewriting views into nested queries.


## Scope
We mainly focus on the BottomUp rewriting stage and the stage of group optimization in the optimizer. To support views, we may also touch the parser and the executor.


## Glossary (Optional)


**Logical Operator**: high-level operators that specify data transformations without specifying the physical execution algorithms to be used
**Query Tree**: a tree representation of a query using logical operators to specify the order in which operators are to be applied
**Physical Operator**: specific algorithms that implement particular database operations
**Execution Plan**: a tree of physical operators that implement the logical operators in a query tree
**Expression**: an operator with zero or more input expressions. The operator can be either logical or physical.


## Architectural Design
The optimizer takes a query tree as input, performs rewriting and optimization and generate the best plan as output to the executor. The rewriting stage includes two parts: the BottomUp rewriting and the TopDown rewriting. The former is for unnesting subqueries and the latter is for pushing predicates down. After the query being rewritten, each node (group) of the query tree  will be explored for its logically equivallent expressions. And the optimizer will compute the costs of all groups. In other words, all possible plans for this query will be generated. At last, the best plan, the one with the lowest cost, will be chosen and passed to the executor.
The role of an optimizer inside a DBMS is shown in the figure below.
![%~{(Q%OHV})09K_9YNF%IWI.png](https://cdn.nlark.com/yuque/0/2020/png/350676/1586317785565-3e0de603-8a58-4c68-830b-0dd8d0ae43e3.png#align=left&display=inline&height=316&name=%25~%7B%28Q%25OHV%7D%2909K_9YNF%25IWI.png&originHeight=316&originWidth=592&size=23039&status=done&style=none&width=592)


## Design Rationale
Our goal is to support all four types of nested queries in this project, and we divide it into several parts:

1. Support type-A and type-JA using CTE
1. Implement operator IN
1. Support views (125% goal)

To achieve the goals above, we come up with some plans accordingly:

1. Materialize the result of subqueries and integrate it into LogicalFilter
1. Rewrite operator COMPARE_IN using semi join by adding new rewrite rules
1. Rewrite views into subqueries by adding new rules



## Testing Plan
We have written several simple queries of different types. Our inital goal is to support these first.
Our plan is to write more nested queries for testing (10 queries per type).
Besides, nine complex queries from TPC-H can be used for tests. 
During the test, we can compare the generated plan with the plan from Postgres and HyPer([HyPer Web Interface](https://hyper-db.de/interface.html#)).
For the new rules we write, we will add some unit tests in the directory `test/optimizer` to see if them work.


## Trade-offs and Potential Problems


> Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).



## Future Work


> Write down future work to fix known problems or otherwise improve the component.



## References
[1] Kim, Won. "On optimizing an SQL-like nested query." ACM Transactions on Database Systems (TODS) 7.3 (1982): 443-469.
[2] Xu Y. Efficiency in the Columbia database query optimizer[D]. Portland State University, 1998.
