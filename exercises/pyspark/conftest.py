import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("data_test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
