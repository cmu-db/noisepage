# Metric Collection Infrastructure

## Overview

>We plan to build an infrastructure for collecting internal statistics during execution in the CMU DBMS. This infrastructure allows data to be collected at multiple levels including database, index, table, and tuple levels. Examples of data that can be collected are CPU usage, memory usage, and transaction latency. The collected statistics are stored in SQL tables, which can be accessed from the catalog and persisted like other application defined tables. Statistics collected are expected to be used to achieve autonomous self-driving.


## Scope
>The metric collection infrastructure is mostly separated from the other part of the system. The interaction is occurred at the point of initialing metrics, triggering callbacks, and persisting metric data. Specifically, the metric collection infrastructure requires support from the following subsystems: 
> + **Settings Manager:** The settings manager needs to provide an interface for selecting levels and types of metrics to be collected as well as data collection behaviour (e.g. collection frequency, update frequency). The metric collection infrastructure then can collect statistics accordingly.
> + **Catalog:** The catalog needs to provide a way for the metric collection infrastructure to create and lookup internal SQL tables that store the aggregated statistics.
> + **Other:** Callbacks should be added at the point the concerned events happen, so the metric collection infrastructure can update the related metrics.

## Architectural Design
>Each thread collects data independently during execution while a designated thread aggregates data periodically and writes to SQL tables. In addition, each thread stores the data it collected to SQL tables before leaving the current execution context and starting a new task.

>To realize above functions, we construct a metric collection framework that is extensible and relatively independent of the database management system. Specifically, our framework has multi-level data structures: different event types such as transaction commit, different metric classes such as metric handler (class Metric) and the actual data holders (class RawData) in metric classes. As the following figure demonstrates: 1) Metrics are registered to corresponding events in initialization. 2) When an event is triggered at the running time, related metric handlers are called to apply for latch to access the data. 3) The actual data holders update the values of the variables. 

>At the same time, a background aggregator regularly contacts worker threads to swap out their collected data in all data holders and then aggregate them. The aggregated data will be updated and persisted in metric catalog tables.

![data_flow](https://raw.githubusercontent.com/wenxuanqiu/terrier/15-721-project2-stats/src/include/stats/data_flow.png)

## Design Rationale
>We choose the current design for several reasons:
> + The metric collection is an upstream task, and the requirements may change according to downstream tasks. Therefore, the framework should be extensible, maintainable and easy for other developers to use. In our design, if we want to collect some metrics related to an event, it is convenient to add an API directly where the event is triggered. Moreover, it is easy to add new metric types and event types.
> + The framework is easy to test as it is relatively independent of the main database management system. There is no need to insert much code and will not hinder the development of other parts.
> + The structure is clear and correct. To guarantee that there is only one instance of each metric at any time, we have a wrapper of data holder which functions like a latch.

> We have considered other implementations. For example, the most direct way is to construct several data holders for different statistics and insert update code in database management system where some statistics are changed. Though the method is simple, it is difficult to maintain and extend. 

## Testing Plan
> There are two kinds of tests:
> + **Correctness**: Firstly, to test the correct operation of the whole framework, we set specific test metric and guarantee that the framework runs without error and returns right data. Secondly, we write unit tests for different metrics to make sure every data type is correctly collected.
> + **Performance**: Metric collection could be enabled/disabled through setting manager. In this way, we could test the performance of our collection framework by comparing the total performance of the database management system with and without metric collection. To further analyze the efficiency, we will test performance impact of different collected metrics.

## Trade-offs and Potential Problems
>The main trade-off is between performance and extensibility/easy to use. To make it easy to add collection code in the database management system and add new metrics, we use the current multi-level framework. It may harm the total performance of metric collection. Moreover, to conveniently calculate or analyze some statistics, we put several statistics in one metric such as the number of committed transactions and the number of aborted transactions. It may also make performance worse since the granularity of latch is increased.

## Future Work
>There is plenty of room for future research. For instance, some metrics are difficult to be accurately collected, such as memory usage whose exact value is obscured by varchar data. We could do research on methods of better estimation or set a hyper-parameter to trade off between accuracy and performance.
