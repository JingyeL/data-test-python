import shutil
import data_aggregation as agg


def test_data_aggregation(spark_session):
    assert spark_session is not None


def test_data_aggregation(spark_session):
    df = spark_session.read.json(
        "exercises/pyspark/02_data_aggregation/data.json", multiLine=True
    )
    agg.data_aggregation(df, "output.csv")

    try:
        df_actual = spark_session.read.csv("output.csv", header=True, inferSchema=True)
        assert df_actual.count() == 2
        assert df_actual.columns == ["event_date", "total_events"]

        assert df_actual.collect()[0][1] == 18
        assert (
            df_actual.filter(df_actual.event_date == "2022-01-01").collect()[0][1] == 18
        )
        assert (
            df_actual.filter(df_actual.event_date == "2022-01-02").collect()[0][1] == 17
        )
    finally:
        shutil.rmtree("output.csv")
