import pytest
import pandas as pd


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    SELECT
        f.facility_name,
        DATE(v.visit_timestamp) AS visit_date,
        MIN(v.duration_minutes) AS min_time_spent
    FROM public.visits v
    JOIN public.facilities f ON v.facility_id = f.id
    GROUP BY f.facility_name, DATE(v.visit_timestamp)
    """
    data = db_connection.get_data_sql(source_query)
    return pd.DataFrame(data)

@pytest.fixture(scope='module')
def target_data(parquet_reader):
    target_path = r"C:\Users\Heorhi_Chychenkau\PycharmProjects\dqe-automation\parquet_data\facility_name_min_time_spent_per_visit_date"
    return parquet_reader.process(target_path, include_subfolders=True)

# Smoke test: dataset is not empty
@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)

# Completeness test: all source rows are present in target
@pytest.mark.parquet_data
@pytest.mark.completeness
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_completeness(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(
        source_data, target_data, key_columns=['facility_name', 'visit_date', 'min_time_spent']
    )

# Count test: row count matches
@pytest.mark.parquet_data
@pytest.mark.completeness
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)

# Quality test: no duplicates
@pytest.mark.parquet_data
@pytest.mark.quality
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_uniqueness(target_data, data_quality_library):
    data_quality_library.check_duplicates(
        target_data, column_names=['facility_name', 'visit_date', 'min_time_spent']
    )

# Quality test: no nulls in key columns
@pytest.mark.parquet_data
@pytest.mark.quality
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(
        target_data, ['facility_name', 'visit_date', 'min_time_spent']
    )