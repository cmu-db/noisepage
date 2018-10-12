# Plan Node Classes

### TODOs (Gus)

* **ALL PLAN NODES**
    * Some plan nodes have a function that `GetOutputColumns(std::vector<oid_t> &columns)`. We need to verify if `oid_t` here is actually a column id or an offset
    * Does `attribute_info.h` need to be changed? Some files rely on this

* Abstract Plan Node
    * Figure out how to handle the replacement of VisitParameters. We need somewhere to store expression parameters if we want the PlanNode objects to be stateless. This applies to all plan nodes with expressions

* Aggregate Plan Node
    * Needs AttributeInfo, expressions, planner, schema, AggregateType, Codegen objects

* Analyze Plan Node
    * Needs datatable object. I imagine with the new storage layer, and thus new way to analyze, we need to rewrite this class.

* Create Function Plan Node
    * Might need to change if we changed how we handle UDF in terrier.
    * Needs parser objects (`parser::CreateFunctionStatement`)
    * Copy function looks like a hack

* Hash Plan Node
    * Needs expressions

* Plan Node Defs
    * Needs replacement for `INVALID_TYPE_ID`


