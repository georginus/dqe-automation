"""
Description: Data Quality checks for all datasets.
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest

# Проверка для facility_name_min_time_spent_per_visit_date
@pytest.mark.parquet_data
def test_check_dataset_is_not_empty(target_data_facility_name_min_time_spent_per_visit_date, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data_facility_name_min_time_spent_per_visit_date)

# Проверка для facility_type_avg_time_spent_per_visit_date
@pytest.mark.parquet_data
def test_check_dataset_is_not_empty_type(target_data_facility_type_avg_time_spent_per_visit_date, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data_facility_type_avg_time_spent_per_visit_date)

# Проверка совпадения количества строк (count) для facility_name_min_time_spent_per_visit_date
@pytest.mark.parquet_data
def test_check_count(target_data_facility_name_min_time_spent_per_visit_date, db_connection, data_quality_library):
    source_query = """
    SELECT
        f.facility_name,
        DATE(v.visit_timestamp) AS visit_date,
        MIN(v.duration_minutes) AS min_time_spent
    FROM public.visits v
    JOIN public.facilities f ON v.facility_id = f.id
    GROUP BY f.facility_name, DATE(v.visit_timestamp)
    """
    source_data = db_connection.get_data_sql(source_query)
    data_quality_library.check_count(source_data, target_data_facility_name_min_time_spent_per_visit_date)

