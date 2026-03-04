from util import (
    create_spark, create_dataframes, average_salary, employee_name_starts_with_m,
    add_bonus_column, reorder_columns, join_departments,
    replace_state_with_country, lowercase_and_add_load_date, save_as_external_tables
)

if __name__ == "__main__":
    spark = create_spark()
    employee_df, department_df, country_df = create_dataframes(spark)

    # 2. Average salary
    avg_df = average_salary(employee_df)
    avg_df.show()

    # 3. Employee names starting with 'm'
    emp_m_df = employee_name_starts_with_m(employee_df, department_df)
    emp_m_df.show()

    # 4. Add bonus
    employee_df = add_bonus_column(employee_df)

    # 5. Reorder
    employee_df = reorder_columns(employee_df)

    # 6. Joins
    inner_df, left_df, right_df = join_departments(employee_df, department_df)
    print("Inner Join:")
    inner_df.show()
    print("Left Join:")
    left_df.show()
    print("Right Join:")
    right_df.show()

    # 7. Replace state with country name
    emp_with_country = replace_state_with_country(employee_df, country_df)
    emp_with_country.show()

    # 8. Lowercase + load_date
    final_df = lowercase_and_add_load_date(emp_with_country)
    final_df.show()

    # 9. Save external tables
    save_as_external_tables(final_df)

    spark.stop()