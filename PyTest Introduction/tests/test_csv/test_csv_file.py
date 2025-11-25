import csv
import os
from collections import defaultdict

import pytest
import re


def test_file_not_empty():
    file_path = "../../src/data/data.csv"
    assert os.path.getsize(file_path) > 0, "File is empty!"


@pytest.mark.validate_csv
def test_validate_schema():
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))
    expected_header = ["id", "name", "age", "email", "is_active"]

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        assert header == expected_header, f"Expected schema: {expected_header}, Actual header: {header}"


@pytest.mark.validate_csv
# @pytest.mark.skip(reason="Test skipped")
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
def test_email_column_valid():
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))
    email_pattern = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=2):  # start=2, т.к. первая строка — заголовок
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
            "File contains duplicated:\n" +
            "\n".join(
                f"Row: {row} — rows number: {lines}"
                for row, lines in duplicates.items()
            )
    )


@pytest.mark.validate_csv
@pytest.mark.parametrize("id_value, expected_is_active", [
    ("1", "False"),
    ("2", "True"),
])
def test_active_players(id_value, expected_is_active):
    test_dir = os.path.dirname(__file__)
    file_path = os.path.abspath(os.path.join(test_dir, "../../src/data/data.csv"))

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        found = False
        for row in reader:
            if row.get("id") == id_value:
                found = True
                assert row.get("is_active") == expected_is_active, (
                    f"For id={id_value} expected is_active={expected_is_active}, "
                    f"but actual is_active={row.get('is_active')}"
                )
        assert found, f"Row with id={id_value} is absent in the file"


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
