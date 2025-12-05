import os
import pytest
import pandas as pd
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="localhost", help="Database host")
    parser.addoption("--db_name", action="store", default="mydatabase", help="Database name")
    parser.addoption("--db_port", action="store", default="5432", help="Database port")
    parser.addoption("--db_user", action="store", help="Database user (required)")
    parser.addoption("--db_password", action="store", help="Database password (required)")

def pytest_configure(config):
    required_options = ["db_user", "db_password"]
    for opt in required_options:
        if not config.getoption(f"--{opt}"):
            raise pytest.UsageError(f"Missing required option: --{opt}")

@pytest.fixture(scope='session')
def db_connection(request):
    db_host = request.config.getoption("--db_host")
    db_name = request.config.getoption("--db_name")
    db_port = request.config.getoption("--db_port")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")
    try:
        with PostgresConnectorContextManager(

            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_name=db_name,
            db_port=db_port
        ) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {e}")

@pytest.fixture(scope='session')
def parquet_reader():
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(f"Failed to initialize ParquetReader: {e}")
    finally:
        del reader

@pytest.fixture(scope='module')
def target_data_facility_name_min_time_spent_per_visit_date(parquet_reader):
    target_path = os.getenv(
        "PARQUET_PATH_FACILITY_NAME",
        "/parquet_data/facility_name_min_time_spent_per_visit_date"
    )
    df = parquet_reader.process(target_path, include_subfolders=True)
    if not df.empty and 'visit_date' in df.columns:
        df['visit_date'] = pd.to_datetime(df['visit_date'])
    return df

@pytest.fixture(scope='module')
def target_data_facility_type_avg_time_spent_per_visit_date(parquet_reader):
    target_path = os.getenv(
        "PARQUET_PATH_FACILITY_TYPE",
        "/parquet_data/facility_type_avg_time_spent_per_visit_date"
    )
    df = parquet_reader.process(target_path, include_subfolders=True)
    if not df.empty and 'visit_date' in df.columns:
        df['visit_date'] = pd.to_datetime(df['visit_date'])
    return df

@pytest.fixture(scope='module')
def target_data_patient_sum_treatment_cost_per_facility_type(parquet_reader):
    target_path = os.getenv(
        "PARQUET_PATH_PATIENT_SUM",
        "/parquet_data/patient_sum_treatment_cost_per_facility_type"
    )
    df = parquet_reader.process(target_path, include_subfolders=True)
    return df

@pytest.fixture(scope='session')
def data_quality_library():
    try:
        dql = DataQualityLibrary()
        yield dql
    except Exception as e:
        pytest.fail(f"Failed to initialize DataQualityLibrary: {e}")
    finally:
        del dql
