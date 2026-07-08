PySpark fundamentals 

1. Reading data efficiently

- APIs: spark.read.csv(), spark.read.json(), spark.read.parquet()
- Notes: Parquet is columnar and performant for analytics; JSON/CSV are flexible but may need schema inference or explicit schema for speed.
- Example:

```python
df = spark.read.parquet('/data/events/')
df = spark.read.csv('/data/users.csv', header=True, inferSchema=True)
```

2. Core transformations

- RDD/DataFrame operations: map, flatMap (RDD), filter, union, select, join
- Notes: Transformations are lazy; they build a DAG executed on an action.
- Example:

```python
filtered = df.filter(df.age > 18)
names = df.select('name')
unioned = df1.unionByName(df2)
```

3. Aggregations at scale

- APIs: groupBy, agg, count, sum, avg
- Notes: Use grouping keys carefully to avoid skew; consider aggregations with map-side combine (Spark does this automatically for many aggregations).
- Example:

```python
summary = df.groupBy('country').agg(F.count('*').alias('cnt'), F.avg('score').alias('avg'))
```

4. Column manipulations

- withColumn(), withColumnRenamed(), expr(), when/otherwise
- Notes: Use expressions and built-in functions from pyspark.sql.functions for performant column ops.
- Example:

```python
from pyspark.sql import functions as F
df2 = df.withColumn('score2', F.col('score') * 1.1).withColumnRenamed('id', 'user_id')
df2 = df2.withColumn('label', F.when(F.col('score') > 50, 1).otherwise(0))
```

Quick best practices

- Persist only when reused often (df.cache(), df.persist()).
- Provide explicit schema for large CSV/JSON reads.
- Monitor and fix data skew (salting, repartitioning).
- Prefer built-in functions over UDFs; if needed, prefer pandas_udf for performance.