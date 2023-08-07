import shutil
import data_transformation as trasnform


def test_data_transformation(spark_session):
    assert spark_session is not None


def test_filter_data_by_click(spark_session):
    df = spark_session.read.csv(
        "exercises/pyspark/01_data_transformation/data.csv",
        header=True,
        inferSchema=True,
    )
    assert trasnform.filter_data(df, "click").count() == 3
    assert trasnform.filter_data(df, "view").count() == 1


def test_add_avg_duration(spark_session):
    df = spark_session.read.csv(
        "exercises/pyspark/01_data_transformation/data.csv",
        header=True,
        inferSchema=True,
    )
    df_actual = trasnform.add_avg_duration(df)
    assert df_actual.count() == 3
    assert df_actual.filter(df_actual.user_id == 1).collect()[0]["avg_duration"] == 6.5
    assert df_actual.filter(df_actual.user_id == 3).collect()[0]["avg_duration"] == 15
    assert df_actual.filter(df_actual.user_id == 2).collect()[0]["avg_duration"] == 10


def test_data_transformation(spark_session):
    df = spark_session.read.csv(
        "exercises/pyspark/01_data_transformation/data.csv",
        header=True,
        inferSchema=True,
    )
    trasnform.data_transformation(df, "output.parquet")

    try:
        df_actual = spark_session.read.parquet("output.parquet")
        assert df_actual.count() == 2
        assert (
            df_actual.filter(df_actual.user_id == 1).collect()[0]["avg_duration"] == 6.5
        )
        assert (
            df_actual.filter(df_actual.user_id == 2).collect()[0]["avg_duration"] == 10
        )

    finally:
        shutil.rmtree("output.parquet")
