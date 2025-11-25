import csv
import os
from collections import defaultdict
import pytest
import re


def test_file_not_empty(read_csv_file):
    # file_path = "../../src/data/data.csv"
    # assert os.path.getsize(file_path) > 0, "File is empty!"
    rows = read_csv_file("../../src/data/data.csv")
    assert len(rows) > 0


@pytest.mark.validate_csv
def test_validate_schema(read_csv_file, validate_schema):
    rows = read_csv_file("../../src/data/data.csv")
    actual_schema = rows[0]
    expected_schema = ["id", "name", "age", "email", "is_active"]
    validate_schema(actual_schema, expected_schema)


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Test temporary skipped")
def test_age_column_valid():
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=2):
            try:
                age = int(row["age"])
            except (ValueError, KeyError):
                pytest.fail(f"Incorrect value 'age' in the row {row_num}: {row.get('age')}")
            assert 0 <= age <= 100, f"Age is out of range 0-100 in the row {row_num}: {age}"


@pytest.mark.validate_csv
@pytest.mark.parametrize("age", [60, 40, 41, 53, 37, 30, 101])
@pytest.mark.xfail(reason="Age 101 is out of range 0-100")
def test_age_range(age):
    assert 0 <= age <= 100


@pytest.mark.validate_csv
@pytest.mark.custom
def test_email_column_valid():
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))
    email_pattern = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=2):
            email = row.get("email", "")
            assert email_pattern.match(email), f"Email is incorrect in the row {row_num}: {email}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Test fails if duplicates found")
def test_duplicates():
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    data_rows = rows[1:]

    row_map = defaultdict(list)
    for idx, row in enumerate(data_rows, start=2):
        row_tuple = tuple(row)
        row_map[row_tuple].append(idx)

    duplicates = {row: lines for row, lines in row_map.items() if len(lines) > 1}

    assert not duplicates, (
            "File contains duplicates:\n" +
            "\n".join(
                f"Row: {row} — rows number: {lines}"
                for row, lines in duplicates.items()
            )
    )


@pytest.mark.validate_csv
@pytest.mark.parametrize("id_value, expected_is_active", [
    pytest.param("1", "False"),
    pytest.param("2", "True", marks=pytest.mark.xfail(reason="Known issue for id=2")),
])
def test_active_players(id_value, expected_is_active):
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get("id") == id_value:
                assert row.get("is_active") == expected_is_active, (
                    f"For id={id_value} expected is_active={expected_is_active}, "
                    f"but actual is_active={row.get('is_active')}"
                )
                break
        else:
            pytest.fail(f"Row with id={id_value} is absent in the file")


@pytest.mark.validate_csv
def test_active_player():
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get("id") == "2":
                assert row.get("is_active") == "True", (
                    f"For id=2 expected is_active=True, but actual is_active={row.get('is_active')}"
                )
                break
        else:
            pytest.fail("Row with id=2 is absent in the file")
