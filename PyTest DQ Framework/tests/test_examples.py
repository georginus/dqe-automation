import pytest

# 1. facility_name_min_time_spent_per_visit_date

@pytest.mark.parquet_data
def test_check_dataset_is_not_empty_facility_name(target_data_facility_name_min_time_spent_per_visit_date):
    df = target_data_facility_name_min_time_spent_per_visit_date
    assert not df.empty, "Dataset is empty! DataFrame for facility_name_min_time_spent_per_visit_date is empty."

@pytest.mark.parquet_data
def test_check_not_null_values_facility_name(target_data_facility_name_min_time_spent_per_visit_date):
    df = target_data_facility_name_min_time_spent_per_visit_date
    for col in ['facility_name', 'visit_date', 'min_time_spent']:
        assert col in df.columns, f"Column '{col}' is missing in DataFrame!"
        nulls = df[df[col].isnull()]
        assert nulls.empty, f"Column '{col}' contains null values!"

@pytest.mark.parquet_data
def test_check_uniqueness_facility_name(target_data_facility_name_min_time_spent_per_visit_date):
    df = target_data_facility_name_min_time_spent_per_visit_date
    duplicates = df[df.duplicated(subset=['facility_name', 'visit_date', 'min_time_spent'], keep=False)]
    assert duplicates.empty, f"Found duplicate rows: {duplicates}"

@pytest.mark.parquet_data
def test_check_count_facility_name(target_data_facility_name_min_time_spent_per_visit_date, db_connection):
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

# 2. facility_type_avg_time_spent_per_visit_date

@pytest.mark.parquet_data
def test_check_dataset_is_not_empty_facility_type(target_data_facility_type_avg_time_spent_per_visit_date):
    df = target_data_facility_type_avg_time_spent_per_visit_date
    assert not df.empty, "Dataset is empty! DataFrame for facility_type_avg_time_spent_per_visit_date is empty."

@pytest.mark.parquet_data
def test_check_not_null_values_facility_type(target_data_facility_type_avg_time_spent_per_visit_date):
    df = target_data_facility_type_avg_time_spent_per_visit_date
    for col in ['facility_type', 'visit_date', 'avg_time_spent']:
        assert col in df.columns, f"Column '{col}' is missing in DataFrame!"
        nulls = df[df[col].isnull()]
        assert nulls.empty, f"Column '{col}' contains null values!"

@pytest.mark.parquet_data
def test_check_uniqueness_facility_type(target_data_facility_type_avg_time_spent_per_visit_date):
    df = target_data_facility_type_avg_time_spent_per_visit_date
    duplicates = df[df.duplicated(subset=['facility_type', 'visit_date', 'avg_time_spent'], keep=False)]
    assert duplicates.empty, f"Found duplicate rows: {duplicates}"

@pytest.mark.parquet_data
def test_check_count_facility_type(target_data_facility_type_avg_time_spent_per_visit_date, db_connection):
    source_query = """
    SELECT
        f.facility_type,
        DATE(v.visit_timestamp) AS visit_date,
        AVG(v.duration_minutes) AS avg_time_spent
    FROM public.visits v
    JOIN public.facilities f ON v.facility_id = f.id
    GROUP BY f.facility_type, DATE(v.visit_timestamp)
    """
    source_data = db_connection.get_data_sql(source_query)
    target_data = target_data_facility_type_avg_time_spent_per_visit_date
    assert len(source_data) == len(target_data), (
        f"Row count mismatch: source={len(source_data)}, target={len(target_data)}"
    )

# 3. patient_sum_treatment_cost_per_facility_type

@pytest.mark.parquet_data
def test_check_dataset_is_not_empty_patient_sum(target_data_patient_sum_treatment_cost_per_facility_type):
    df = target_data_patient_sum_treatment_cost_per_facility_type
    assert not df.empty, "Dataset is empty! DataFrame for patient_sum_treatment_cost_per_facility_type is empty."

@pytest.mark.parquet_data
def test_check_not_null_values_patient_sum(target_data_patient_sum_treatment_cost_per_facility_type):
    df = target_data_patient_sum_treatment_cost_per_facility_type
    for col in ['patient_id', 'facility_type', 'sum_treatment_cost']:
        assert col in df.columns, f"Column '{col}' is missing in DataFrame!"
        nulls = df[df[col].isnull()]
        assert nulls.empty, f"Column '{col}' contains null values!"

@pytest.mark.parquet_data
def test_check_uniqueness_patient_sum(target_data_patient_sum_treatment_cost_per_facility_type):
    df = target_data_patient_sum_treatment_cost_per_facility_type
    duplicates = df[df.duplicated(subset=['patient_id', 'facility_type', 'sum_treatment_cost'], keep=False)]
    assert duplicates.empty, f"Found duplicate rows: {duplicates}"

@pytest.mark.parquet_data
def test_check_count_patient_sum(target_data_patient_sum_treatment_cost_per_facility_type, db_connection):
    source_query = """
    SELECT
        v.patient_id,
        f.facility_type,
        SUM(t.treatment_cost) AS sum_treatment_cost
    FROM public.treatments t
    JOIN public.visits v ON t.visit_id = v.id
    JOIN public.facilities f ON v.facility_id = f.id
    GROUP BY v.patient_id, f.facility_type
    """
    source_data = db_connection.get_data_sql(source_query)
    target_data = target_data_patient_sum_treatment_cost_per_facility_type
    assert len(source_data) == len(target_data), (
        f"Row count mismatch: source={len(source_data)}, target={len(target_data)}"
    )
