# SettingsManager

## Overview

> What motivates this to be implemented? What will this component achieve? 

The Settings Manager is a place for all configurations of the system. It offers programmatic interfaces for defining, accessing and modifying configuration parameters for all parts of the system. Although there is a `pg_settings` table in the catalog that does similar things, the internal parts don't want to access its configurations with troublesome SQL statements. Therefore, we provide `set` and `get` methods to them. The access and modify methods are designed to be able to detect wrong parameter names at compilation time. 

It is responsible for loading the configurations from the default values, `gflags`, and the config file. It also supports callback triggers: you can define a callback function for each configurable parameter. When the parameter is changed, the Settings Manager will call that function for you. For example, when someone scales down the number of GC threads from 8 to 6, the Settings Manager will tell the Garbage Collector to shut down 2 threads via the callback tied to that parameter. Besides, the Settings Manager supports action context, which records information relevant to the setting value deployment. The caller of `set` methods can pass in a callback function to analyze the action context.

Having a Settings Manager in the system eliminates all hardcoding stuff and its compilation time saves a lot of debugging efforts. In addition, it will be a critical part for a self-driving database system where quite a lot of parameters will be modified online.

## Scope

> Which parts of the system will this feature rely on or modify? Write down specifics so people involved can review the design doc

The Settings Manager relies on the following parts:

- The main database object `DBMain`. The callbacks should be defined in the main database object because only the main object has all the pointers to the different parts of the system. The Settings Manager will invoke them when the corresponding parameter is changed. Only settings manager have access to the main database object.
- The catalog and the Transaction Manager. Currently, we are maintaining a physical `pg_settings` table in the catalog. Therefore, in order to keep consistent, we have to update `pg_settings` every time a parameter is changed, and updating the catalog needs to be done in a transaction. 

## Glossary (Optional)

> If you are introducing new concepts or giving unintuitive names to components, write them down here.

## Architectural Design

> Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

The Settings Manager is a relatively independent component in the system. It mainly serves as a storage component except when it wants to invoke callbacks.

For parameter definitions, they are parsed by GFlags framework in the main() function and populated to a map inside `DBMain` object. The Settings Manager will check the setting values in DBMain. After that, it will serve `get` and `set` calls from any part of the system.

For callbacks, when a parameter is changed, the Settings Manager will invoke its associated callback function in the main database object. The change can be done either synchronously or asynchronously. If it's invoked in an asynchronous manner: the Settings Manager will not wait until the change is enforced. Instead, it will expect to return immediately. Then, there is a callback in the Settings Manager itself to receive reports from the parts. The information is passed via an ActionContext object.

## Design Rationale

> Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

### Compilation Time Check

 In order to enforce compilation time check, we include all the parameter names in `settings.h` into an `enum` called `Param`. In this way, users can only access the parameters in the `enum`. 

We don't use reflections because it is very troublesome in C++ and it is not compilation time check. A wrong parameter name will not be discovered until run time.

Writing the `enum` is made much easier thanks to the macro based design of the Settings Manager, which is explained below. 

### Macro based Parameter Definition

We provide some macros for the developers to define their parameters. They include `SETTING_int`, `SETTING_bool`, `SETTING_double` and `SETTING_string`. The definition of the macros is in `settings_macro.h`.

Although macros are confusing and not straightforward, it offers more benefits than disadvantages in the Settings Manager. The biggest benefit is that the users can now specify each parameter only once. Without macros, when a user wants to define a new parameter, he must do it for three times: append it in the `Params` `enum`, register it in `gflags`, and initialize it in the Settings Manager. It is clear that this exposes too many internal details of the Settings Manager to the users. 

In the current design, users only need to define his parameter in `settings.h` only once, and the Settings Manager will do the `enum`, `gflags` and initialization stuff for them by simply `#include settings.h` and interpret `SETTING_xxx` in different ways.

### Internal and External Map

Although there is a physical `pg_settings` table in the catalogs where we can find all the information about a parameter, we finally decided that there should be a map that contains the values of parameters. Why? Because:

- The most important reason is that accessing the catalog needs to be done in a transaction. This can cause infinite loops. A typical situation is, on system bootstrapping, the Settings Manager needs to open a transaction to append the values into the `pg_settings` table. However, as long as there is a configuration needed by the Transaction Manager (a very likely one can be the default isolation level), it will call `get` to ask for that parameter, and the Settings Manager will open a new transaction to read the parameter from the catalog, which results in an infinite loop. In order to reduce the interaction with other parts of the system, we decided to maintain a map inside the Settings Manager. In this way, it can serve all requests without consulting anyone else.
- It is much easier to operate on a map than the catalog. 
- Since the number of parameters is low, an extra map won't introduce much overhead.

- Although this essentially turned the Settings Manager as a cache layer for the `pg_settings` table, its consistency is easy to maintain (just update the catalog in `set` methods).

This value map is an external map actually stored in DBMain. We also have another internal callback map, which maps a setting to its callback. It should be immutable during runtime,

## Testing Plan

> How should the component be tested?

The following tests should be written:

- Are the default values loaded successfully?
- Do `gflags` and config file successfully overwrite default values?
- Can mutable parameters be correctly changed? If it has a callback function, is that called?
- Can the Settings Manager check if the new value is between `min_value` and `max_value`?
- Can the Settings Manager refuse to change immutable parameters?

## Trade-offs and Potential Problems

> Write down any conscious trade-off you made that can be problematic in the future or any problems discovered during the design process that remain unaddressed (technical debts).

By adding the internal map, the system is storing 3 copies of each parameter value: one in its component (each component is very likely to maintain a local copy, like in the Garbage Collector there is very likely to be a `num_threads` member variable), one in the map, and one in the catalog. However, this is necessary as stated above. The real unnecessary copy is actually the `pg_settings` table. In fact, Postgresql does not have this physical table. It also stores the values in an internal map. When someone calls `SELECT * FROM pg_settings`, it generates a view from the internal map. However, Terrier does not support generating a view from functions yet. Therefore, we must maintain the physical table.

Another note is that changing parameters in Postgresql is transactional, but not in Terrier. Because we think it's weird and meaningless to let different transactions see different configurations. Also, supporting transactional modification adds quite a lot of complexity to implementation.

## Future Work

> Write down future work to fix known problems or otherwise improve the component.

If Terrier supports building views from functions, we can safely remove the physical `pg_settings` table to reduce redundancy.