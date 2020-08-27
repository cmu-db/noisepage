# Project Design Document

# Supporting Nested Query


## Overview
In this project, our initial goal is to support four types of nested queries mentioned in Kim's paper, including type-A, type-N, type-J and type-JA. Currently both type-N and type-J queries are supported in the optimizer with the UnnestMarkJoinToInnerJoin rule. However, executing them correctly requires the support of in operation. For  type-A and type-JA queries that involves aggregation operators, unnesting algorithms are required in order to complete the optimization process. Furthermore, we are determined to support views by rewriting views into nested queries in the future.


## Scope
This feature mainly relies on the TopDownRewrite and BottomUpRewrite rewriting stage and the stage of group optimization in the optimizer. To support views, we may also touch the parser and the executor.
The components we added are as follows:

- RewritePushExplicitFilterThroughJoin in rewrite_rules.cpp, a logical-level rewriting rule belonging to the PREDICATE_PUSH_DOWN rule set. It is used in the TopDownRewrite stage of OptimizerLoop.
- LeftSemiHashJoin in physical_operators.cpp, which is a new physical operator for executing SemiJoin. 
- LogicalSemiJoinToPhysicalSemiLeftHashJoin in implementation_rules.cpp. It is utilized by OptimizeExpression::Execute(). 
- UnnestSingleJoinToInnerJoin and DependentSingleJoinToInnerJoin Unnesting rules in unnesting_rules.cpp. They are called in the BottomUpRewrite stage of OptimizerLoop.



## Glossary (Optional)


**Logical Operator**: high-level operators that specify data transformations without specifying the physical execution algorithms to be used
**Query Tree**: a tree representation of a query using logical operators to specify the order in which operators are to be applied
**Physical Operator**: specific algorithms that implement particular database operations
**Execution Plan**: a tree of plan nodes that implement the logical operators in a query tree
**Expression**: an operator with zero or more input expressions. The operator can be either logical or physical.


## Architectural Design
The optimizer takes a query tree as input, performs rewriting and optimization and generate the best plan as output to the executor. The rewriting stage includes two parts: the BottomUp rewriting and the TopDown rewriting. The former is for unnesting subqueries and the latter is for pushing predicates down. After the query being rewritten, each node (group) of the query tree  will be explored for its logically equivallent expressions. And the optimizer will compute the costs of all groups. In other words, all possible plans for this query will be generated. At last, the best plan, the one with the lowest cost, will be chosen and passed to the executor.
The role of an optimizer inside a DBMS is shown in the figure below.
![%~{(Q%OHV})09K_9YNF%IWI.png](https://cdn.nlark.com/yuque/0/2020/png/350676/1586317785565-3e0de603-8a58-4c68-830b-0dd8d0ae43e3.png#align=left&display=inline&height=316&margin=%5Bobject%20Object%5D&name=%25~%7B%28Q%25OHV%7D%2909K_9YNF%25IWI.png&originHeight=316&originWidth=592&size=23039&status=done&style=none&width=592)
To support the four kinds of nested queries we mentioned above, we mainly added transformation rules and new physical operators. Our approach is inspired by Kim's paperTo be more specific:
For **Type A** queries, terrier did not support SingleJoin and thus could not utilize the AggregateAndGroupBy results directly. Since terrier implements bottom-up query execution, a combination of Filter operator and SingleJoin in this case is equivalent to a InnerJoin with additional predicates. Therefore, we defined a new UnnestSingleJoinToInnerJoin rule with pattern SingleJoin -> AggregateAndGroupBy. This rule converts SingleJoin operator into InnerJoin accordingly.
![image.png](https://cdn.nlark.com/yuque/0/2020/png/1252453/1588746419433-8e16bb13-c044-48b1-bb0a-427d658018cd.png#align=left&display=inline&height=398&margin=%5Bobject%20Object%5D&name=image.png&originHeight=632&originWidth=1156&size=80724&status=done&style=none&width=728)


For **Type N** queries, we unnest the query by transforming Markjoin into Innerjoin through the rule - UnnestMarkJoinToInnerJoin. Since COMPARE_IN expression in Filter3 is not supported in terrier, another rule - RewritePushExplicitFilterThroughJoin is applied to combine the filter operator and the InnerJoin operator into a SemiJoin operator. For implementation, we defined a new LogicalSemiJoin node type and modified RewritePushExplicitFilterThroughJoin. The initial goal of RewritePushExplicitFilterThroughJoin is to push LogicalFilter operators through LogicalInnerJoin operators. We added an additional case that if the LogicalFilter operator contains a COMPARE_IN expression, the LogicalFilter operator is merged into join operator. The COMPARE_IN operation is deleted directly.
![image.png](https://cdn.nlark.com/yuque/0/2020/png/1252453/1588041184814-6c616381-2976-41aa-82cf-9ce7dc696709.png#align=left&display=inline&height=350&margin=%5Bobject%20Object%5D&name=image.png&originHeight=472&originWidth=991&size=50631&status=done&style=none&width=735)
For **Type J **queries, it is very similar to Type N except that instead of deleting COMPARE_IN expression, it is replaced with a COMPARE_EQUAL predicate in the SemiJoin operator. 
![image.png](https://cdn.nlark.com/yuque/0/2020/png/1252453/1588746552704-a7fc96bc-c34c-444a-a883-06ea80d624dc.png#align=left&display=inline&height=388&margin=%5Bobject%20Object%5D&name=image.png&originHeight=775&originWidth=1628&size=114150&status=done&style=none&width=814)
For **Type JA** queries, we implemented a new DependentSingeJoinToInnerJoin rule that matches the pattern: SingleJoin->Filter->AggregateAndGroupBy. Terrier did not support Type JA queries because the Filter operator may have correlated predicate that refers the outer relation. To deal with this problem, we have to pull Filter operator through SingleJoin operator. Another problem is that expressions in Filter node may not appear in AggregateAndGroupBy operator. In that case, new groupbys are added to the AggregateAndGroupBy operator accordingly. The final result should be something like Filter1 -> InnerJoin -> Filter2 -> AggregateAndGroupBy.
![image.png](https://cdn.nlark.com/yuque/0/2020/png/1252453/1588746505653-735e8f07-3525-44ea-b34f-c197401f85bf.png#align=left&display=inline&height=376&margin=%5Bobject%20Object%5D&name=image.png&originHeight=783&originWidth=1552&size=117808&status=done&style=none&width=746)

## Design Rationale
Our goal is to support all four types of nested queries in this project, and we divide it into several parts:

1. Support nested queries in optimizer
1. Support nested predicates in execution engine 
1. Support views (125% goal)

To achieve the goals above, we come up with some plans accordingly:

1. Transforming 4 basic types of nesting into joins with new transformation rules 
1. Supporting COMPARE_IN operation by adding new rewrite rules and a new operator node - LogicalSemiJoin
1. Storing views in system catalog and rewrite them into subqueries during execution



## Testing Plan
Correctness is tested with a mix of C++ code and java code

C++ code is for a specific functionality. e.g. Check if the transformation rules are applied successfully
Junit test is for an end-to-end test. 10 test cases for each type
During the test, we also compared the generated plan with the plan from Postgres and HyPer([HyPer Web Interface](https://hyper-db.de/interface.html#)).


## Trade-offs and Potential Problems
For supporting typeA nesting that involves aggregation function in subqueries, we avoided materialization using new transformation rules. We kepted the Aggregation operator node under an InnerJoin operator node. The way we chose is simpler to implement. However, implementing materialization techniques may be beneficial for performance.  
## Future Work

1. Support views by transforming them into nested queries

1. Add materialization techniques for typeA nesting

1. Support typeD nested queries by implementing set operations
## References
[1] Kim, Won. "On optimizing an SQL-like nested query." ACM Transactions on Database Systems (TODS) 7.3 (1982): 443-469.
[2] Xu Y. Efficiency in the Columbia database query optimizer[D]. Portland State University, 1998.
