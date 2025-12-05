"""
Description: Data Quality checks for all datasets.
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest

@pytest.mark.parquet_data
def test_check_dataset_is_not_empty(target_data_facility_name_min_time_spent_per_visit_date):
    df = target_data_facility_name_min_time_spent_per_visit_date
    assert not df.empty, "Dataset is empty! DataFrame for facility_name_min_time_spent_per_visit_date is empty."

@pytest.mark.parquet_data
def test_check_dataset_is_not_empty_type(target_data_facility_type_avg_time_spent_per_visit_date):
    df = target_data_facility_type_avg_time_spent_per_visit_date
    assert not df.empty, "Dataset is empty! DataFrame for facility_type_avg_time_spent_per_visit_date is empty."

@pytest.mark.parquet_data
def test_check_count(target_data_facility_name_min_time_spent_per_visit_date, db_connection):
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
    target_data = target_data_facility_name_min_time_spent_per_visit_date
    assert len(source_data) == len(target_data), (
        f"Row count mismatch: source={len(source_data)}, target={len(target_data)}"
    )

@pytest.mark.parquet_data
def test_check_not_null_values(target_data_facility_name_min_time_spent_per_visit_date):
    df = target_data_facility_name_min_time_spent_per_visit_date
    for col in ['facility_name', 'visit_date', 'min_time_spent']:
        assert col in df.columns, f"Column '{col}' is missing in DataFrame!"
        nulls = df[df[col].isnull()]
        assert nulls.empty, f"Column '{col}' contains null values!"

@pytest.mark.parquet_data
def test_check_uniqueness(target_data_facility_name_min_time_spent_per_visit_date):
    df = target_data_facility_name_min_time_spent_per_visit_date
    duplicates = df[df.duplicated(subset=['facility_name', 'visit_date', 'min_time_spent'], keep=False)]
    assert duplicates.empty, f"Found duplicate rows: {duplicates}"
