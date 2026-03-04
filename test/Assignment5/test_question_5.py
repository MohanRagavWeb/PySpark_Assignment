from src.Assignment5.util import create_spark, create_dataframes, average_salary

def test_avg_salary():
    spark = create_spark()
    employee_df, _, _ = create_dataframes(spark)
    avg_df = average_salary(employee_df)
    result = {row['department']: round(row['avg_salary'], 1) for row in avg_df.collect()}
    assert result["D101"] == 8933.3
    spark.stop()