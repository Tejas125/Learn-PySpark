# Apache Spark Documentation

# Day 1 – Spark Fundamentals & Internals

---

# 1. Problems with Hadoop MapReduce

Hadoop MapReduce was one of the first large-scale distributed data processing frameworks. Although it solved the problem of processing massive datasets, it has several limitations.

## Limitations

* Disk-based processing (reads/writes data to HDFS after every Map and Reduce phase)
* High latency due to frequent disk I/O
* Not suitable for iterative algorithms (Machine Learning)
* Poor performance for interactive analytics
* Complex Java programming model
* Difficult debugging
* Multiple MapReduce jobs required for complex workflows

## Example

A data pipeline performing filtering, joining, and aggregation requires multiple MapReduce jobs, resulting in repeated disk reads and writes.

---

# 2. What is Apache Spark?

Apache Spark is an open-source distributed computing framework designed for fast, large-scale data processing.

It processes data in-memory, making it significantly faster than Hadoop MapReduce.

Spark supports

* Batch Processing
* Stream Processing
* Machine Learning
* Graph Processing
* SQL Analytics

---

# 3. Spark Features & Ecosystem

## Features

* In-memory processing
* Fault tolerance
* Lazy evaluation
* High scalability
* Supports Java, Scala, Python, R
* Easy APIs
* Optimized execution engine

## Spark Ecosystem

| Component       | Purpose                     |
| --------------- | --------------------------- |
| Spark Core      | Basic execution engine      |
| Spark SQL       | Structured data processing  |
| Spark Streaming | Real-time stream processing |
| MLlib           | Machine Learning            |
| GraphX          | Graph processing            |

---

# 4. RDD (Resilient Distributed Dataset)

RDD is Spark's fundamental distributed collection of data.

It is

* Immutable
* Distributed
* Fault tolerant
* Partitioned
* Lazily evaluated

## Properties

* Immutable
* Distributed across nodes
* Supports transformations and actions
* Fault recovery using lineage
* Parallel processing

Example

```
val numbers = sc.parallelize(Array(1,2,3,4))
```

---

# 5. Data Partitioning in Spark

Partitioning divides data into smaller chunks processed in parallel.

## Benefits

* Parallel execution
* Load balancing
* Reduced execution time

## Types

### Hash Partitioning

Data distributed based on hash value.

### Range Partitioning

Data divided based on value ranges.

Example

```
8 Partitions

P1
P2
P3
P4
P5
P6
P7
P8
```

Each executor processes one or more partitions.

---

# 6. Transformations and Actions

## Transformations

Create a new RDD/DataFrame.

Examples

* map()
* filter()
* flatMap()
* groupBy()
* join()
* distinct()

Transformations are lazy.

Example

```
df.filter($"salary">50000)
```

---

## Actions

Trigger execution.

Examples

* show()
* count()
* collect()
* first()
* reduce()
* save()

Example

```
df.count()
```

---

# 7. Narrow vs Wide Transformations

## Narrow Transformation

Each partition depends on one parent partition.

Examples

* map
* filter
* union

Advantages

* Fast
* No shuffle

Example

```
P1 → P1
P2 → P2
P3 → P3
```

---

## Wide Transformation

Requires data shuffle between partitions.

Examples

* groupBy
* reduceByKey
* join
* distinct

Advantages

* Enables aggregations

Disadvantages

* Expensive
* Network communication

Example

```
P1 \
P2 -----> Shuffle -----> New Partitions
P3 /
```

---

# 8. Read/Write Operations

Spark supports many storage systems.

## Reading

```
spark.read.csv()

spark.read.json()

spark.read.parquet()

spark.read.jdbc()

spark.read.format("delta")
```

## Writing

```
df.write.csv()

df.write.parquet()

df.write.jdbc()

df.write.saveAsTable()
```

Supported Storage

* HDFS
* S3
* Azure Data Lake
* Snowflake
* Delta Lake
* SQL Databases

---

# 9. Lazy Evaluation

Spark does not execute transformations immediately.

Instead, it builds a logical execution plan.

Execution starts only when an Action is called.

Example

```
df.filter(...)
  .select(...)
  .groupBy(...)
```

Nothing executes until

```
show()

count()

collect()
```

Benefits

* Optimization
* Reduced computation
* Better execution planning

---

# 10. Lineage Graph (DAG)

Spark records every transformation.

This record is called Lineage.

If a partition is lost, Spark rebuilds it using the lineage graph.

Example

```
Read CSV
     ↓
Filter
     ↓
Map
     ↓
GroupBy
     ↓
Count
```

This forms a Directed Acyclic Graph (DAG).

Benefits

* Fault tolerance
* No data replication required

---

# 11. Spark Web UI Walkthrough

Spark Web UI helps monitor applications.

Tabs

* Jobs
* Stages
* Storage
* Environment
* Executors
* SQL
* Streaming (if applicable)

Useful Information

* Execution time
* Shuffle size
* Failed tasks
* Memory usage
* DAG Visualization

Default Port

```
4040
```

---

# 12. Creation of Job, Stage and Tasks

Execution Flow

```
Action

↓

Job

↓

Stages

↓

Tasks

↓

Executors
```

## Job

Created whenever an Action is executed.

Example

```
count()
```

creates one Job.

---

## Stage

A Job consists of one or more stages.

Stages are separated by Shuffle operations.

---

## Task

Each partition creates one Task.

Example

100 partitions

↓

100 Tasks

---

# 13. Spark Architecture & Components

```
                Driver Program
                      |
             Spark Session
                      |
              DAG Scheduler
                      |
            Task Scheduler
                      |
----------------------------------------------
|              Cluster Manager               |
----------------------------------------------
      |             |              |
 Executor 1     Executor 2     Executor 3
      |             |              |
   Task 1        Task 2        Task 3
```

## Components

### Driver

* Creates SparkSession
* Builds DAG
* Schedules jobs
* Collects results

### Cluster Manager

Allocates cluster resources.

Examples

* Standalone
* YARN
* Kubernetes
* Mesos

### Executor

Runs tasks.

Responsible for

* Reading data
* Processing data
* Caching
* Returning results

---

# 14. Standalone and YARN Cluster Manager

## Standalone

Spark's own cluster manager.

Advantages

* Easy setup
* Lightweight

Used mainly for

* Development
* Small clusters

---

## YARN

Resource manager in Hadoop.

Advantages

* Better resource utilization
* Security
* Multi-tenant support
* Enterprise deployment

Common in production environments.

---

# 15. Deployment Modes

## Client Mode

```
Developer Machine

↓

Driver

↓

Cluster
```

Driver runs on client machine.

Good for development.

---

## Cluster Mode

```
Cluster

↓

Driver

↓

Executors
```

Driver runs inside the cluster.

Recommended for production.

---

# 16. Spark Job Internals

Complete Execution Flow

```
Application Submitted

↓

SparkSession Created

↓

Read Data

↓

Transformations

↓

Logical Plan

↓

Catalyst Optimizer (DataFrames)

↓

Physical Plan

↓

DAG Scheduler

↓

Job

↓

Stages

↓

Tasks

↓

Executors

↓

Result Returned
```

Execution Steps

1. User submits Spark application.
2. Driver creates SparkSession.
3. Transformations build the logical plan.
4. Action triggers execution.
5. DAG Scheduler creates stages.
6. Task Scheduler assigns tasks to executors.
7. Executors process partitions in parallel.
8. Results are returned to the Driver.
9. Driver sends results to the user or stores them in the target destination.

---

# Day 1 Summary

After completing Day 1, you should understand:

* Why Spark is faster than Hadoop MapReduce
* Spark architecture and ecosystem
* RDD fundamentals
* Data partitioning
* Transformations and actions
* Narrow vs Wide transformations
* Lazy evaluation
* DAG and lineage
* Spark Web UI
* Jobs, Stages, and Tasks
* Cluster managers
* Deployment modes
* End-to-end Spark job execution flow
