# Plan Node Classes

### TODOs (Gus)

* **ALL PLAN NODES**
    * Some plan nodes have a function that `GetOutputColumns(std::vector<oid_t> &columns)`. We need to verify if `oid_t` here is actually a column id or an offset
    * Does `attribute_info.h` need to be changed? Some files rely on this

* Do we need the following:
    * CSV Scan Plan Node
    * Export External File Plan Node
    * Hybrid Scan Plan Node

* Abstract Join Plan Node
    * Someone wrote "Fuck Off" in the constructor lol
    * Needs expressions
    * Needs schema
    * Has a `HandleSubplanBinding` function
    * Needs `ProjectInfo.h`. Idk what this does

* Abstract Plan Node
    * Figure out how to handle the replacement of VisitParameters. We need somewhere to store expression parameters if we want the PlanNode objects to be stateless. This applies to all plan nodes with expressions

* Aggregate Plan Node
    * Needs AttributeInfo, expressions, planner, schema, AggregateType, Codegen objects

* Analyze Plan Node
    * Needs datatable object. I imagine with the new storage layer, and thus new way to analyze, we need to rewrite this class.

* Attribute Info
    * Needs `codegen::type::Type`

* Create Function Plan Node
    * Might need to change if we changed how we handle UDF in terrier.
    * Needs parser objects (`parser::CreateFunctionStatement`)
    * Copy function looks like a hack

* Hash Plan Node
    * Needs expressions

* Plan Node Defs
    * Needs replacement for `INVALID_TYPE_ID`

* Create Plan Node
    * This is a mess and someone who built the new storage engine should prob rewrite it or bring it in manually

* CSV Scan Plan Node
    * Is this needed?

* Delete Plan Node
    * Needs DataTable object
    * Has a `SetParametersValue` function
    * Same as `CreatePlanNode`, someone who built the new storage engine should rewrite this

* Export External File Plan Node
    * Is this needed?

* Hash Join Plan Node
    * Needs expressions
    * Has `HandleSubplanBinding` function
    * Copy function needs Schema

* Hybrid Scan Plan Node
    * Is this needed?

