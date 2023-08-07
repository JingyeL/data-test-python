import shutil
import data_join as joiner
import unittest


def test_data_join(spark_session):
    assert spark_session is not None


def test_data_join(spark_session):
    df_user = spark_session.read.csv(
        "exercises/pyspark/03_data_join/users.csv", header=True, inferSchema=True
    )
    df_purchase = spark_session.read.csv(
        "exercises/pyspark/03_data_join/purchases.csv", header=True, inferSchema=True
    )
    joiner.data_join(df_user, df_purchase, "output.json")

    # try:
    df_actual = spark_session.read.json("output.json")
    assert df_actual.count() == 3
    df_actual = df_actual.select("user_id", "name", "total_amount")
    df_actual.filter("user_id == 1 and name =='John'").collect()[0][2] == 75.0
    df_actual.filter("user_id == 2 and name =='Alice'").collect()[0][2] == 20.0
    df_actual.filter("user_id == 3 and name =='Bob'").collect()[0][2] == 30.0
    # finally:
    #     shutil.rmtree("output.json")
