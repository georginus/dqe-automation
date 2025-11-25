import pytest
import csv


# Fixture to read the CSV file
@pytest.fixture(scope="session")
def read_csv_file():
    def _read(path_to_file):
        with open(path_to_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            return list(reader)
    return _read


# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def validate_schema():
    def _validate(actual_schema, expected_schema):
        assert actual_schema == expected_schema, (
            f"Expected schema {expected_schema}, but actual is {actual_schema}"
        )
    return _validate


# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(session, config, items):
    for item in items:
        # Если у теста нет ни одного маркера (кроме стандартных служебных)
        if not item.own_markers:
            item.add_marker("unmarked")