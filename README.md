# Custom spark mllib extension with a python wrapper

Version: Spark 2.2.1

For a more detailed explanation check: https://raufer.github.io/2018-02-08-custom-spark-models-with-python-wrappers/

----

For a clean packaging:

```
sbt clean assembly
```

To create an environment for the `python` subfolder:
```
pipenv install --three
```

To run the tests:
```
sbt test
```
or
```
pipenv run nosetests tests/
```

From scala:
```scala
val df = Seq(
  (1.0, 0.0),
  (5.0, 2.0),
  (0.0, 0.0),
  (7.0, 2.0),
  (4.0, 1.0),
  (8.0, 3.0),
  (10.0, 3.0)
).toDF("input", "expected")

val nBins = 4

val bucketizer = new Bucketizer()
  .setInputCol("input")
  .setOutputCol("bin")
  .setNumberBins(nBins)

val model = bucketizer.fit(df)

model.transform(df).show()

```
```
+-----+--------+---+
|input|expected|bin|
+-----+--------+---+
|  1.0|       0|  0|
|  5.0|       2|  2|
|  0.0|       0|  0|
|  7.0|       2|  2|
|  4.0|       1|  1|
|  8.0|       3|  3|
| 10.0|       4|  4|
+-----+--------+---+
```

From python:

```python
from custom_spark_ml.feature.bucketizer import Bucketizer, BucketizerModel

data = [
    (1.0, 0),
    (5.0, 2),
    (0.0, 0),
    (7.0, 2),
    (4.0, 1),
    (8.0, 3),
    (10.0, 4)
]

df = spark.createDataFrame(data, ["input", "expected"])

nBins = 4

bucketizer = Bucketizer() \
    .setInputCol("input") \
    .setOutputCol("bin") \
    .setNumberBins(nBins)

model = bucketizer.fit(df)

model.transform(df).show()

```
```
+-----+--------+---+
|input|expected|bin|
+-----+--------+---+
|  1.0|       0|  0|
|  5.0|       2|  2|
|  0.0|       0|  0|
|  7.0|       2|  2|
|  4.0|       1|  1|
|  8.0|       3|  3|
| 10.0|       4|  4|
+-----+--------+---+
```
