import logging

import pyspark
from pyspark import SparkConf

def quiet_py4j(level=logging.WARN):
    """
    Turn down spark verbosity for testing environmnets
    """
    logger = logging.getLogger('py4j')
    logger.setLevel(level)


def _default_spark_configuration():
    """
    Default configuration for spark's unit testing
    Custom jars are added for the spark.ml custom extensions
    """
    mljar = '../target/scala-2.11/spark-mllib-custom-models-assembly-0.1.jar'

    conf = SparkConf()
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    conf.set("spark.jars", mljar)
    conf.set("spark.app.name", "sparkTestApp")
    return conf


def bootstrap_test_spark_session(conf=None):
    """Setup spark testing context"""
    conf = conf or _default_spark_configuration()
    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


quiet_py4j()
spark = bootstrap_test_spark_session()
