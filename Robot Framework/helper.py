from robot.api.deco import keyword
import pandas as pd
import json
import re
import os



@keyword
def read_html_table(table_html):
    # Reads HTML table string into a Pandas DataFrame
    dfs = pd.read_html(table_html)
    return dfs[0] if dfs else pd.DataFrame()


@keyword
def parse_plotly_table_from_script(script_text):
    # Найти массив данных между первой [ и соответствующей ]
    arr_start = script_text.find('[')
    depth = 0
    end = -1
    for i in range(arr_start, len(script_text)):
        if script_text[i] == '[':
            depth += 1
        elif script_text[i] == ']':
            depth -= 1
            if depth == 0:
                end = i
                break
    arr_text = script_text[arr_start:end+1]
    # Преобразовать в валидный JSON
    arr_text = re.sub(r"(\w+):", r'"\1":', arr_text)  # Ключи в кавычки
    arr_text = arr_text.replace("'", '"')
    arr = json.loads(arr_text)
    table = next((x for x in arr if x.get("type") == "table"), None)
    if not table:
        return pd.DataFrame()
    headers = table["header"]["values"]
    values = table["cells"]["values"]

    rows = []
    for i in range(len(values[0])):
        row = [values[j][i] for j in range(len(values))]
        rows.append(row)
    df = pd.DataFrame(rows, columns=headers)
    return df


@keyword
def convert_table_data_to_dataframe(table_data):
    # table_data: list of lists, first row is header
    if not table_data or len(table_data) < 2:
        return pd.DataFrame()
    header = table_data[0]
    rows = table_data[1:]
    df = pd.DataFrame(rows, columns=header)
    # Приведение типов (например, float)
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass
    return df


@keyword
def prepare_parquet_for_comparison(folder, filter_date=None):
    # Recursively find all .parquet files in all subfolders
    parquet_files = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(root, file))
    if not parquet_files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

    # Привести названия столбцов к нужным
    rename_map = {
        'facility_type': 'Facility Type',
        'visit_date': 'Visit Date',
        'avg_time_spent': 'Average Time Spent',
        'min_time_spent': 'Average Time Spent',  # если у тебя min_time_spent
    }
    df = df.rename(columns=rename_map)

    # Преобразовать дату к строке
    df['Visit Date'] = pd.to_datetime(df['Visit Date']).dt.strftime('%Y-%m-%d')

    # Фильтрация по дате, если нужно
    if filter_date:
        df = df[df['Visit Date'] == filter_date]

    # Округлить значения float
    if 'Average Time Spent' in df.columns:
        df['Average Time Spent'] = pd.to_numeric(df['Average Time Spent'], errors='coerce').round(2)

    # Сортировка для корректного сравнения
    df = df.sort_values(by=['Facility Type', 'Visit Date', 'Average Time Spent']).reset_index(drop=True)

    return df


@keyword
def compare_dataframes(df1, df2):
    # Compares two DataFrames for exact match and returns differences if any
    # Sort columns for reliable comparison
    df1_sorted = df1.sort_index(axis=1)
    df2_sorted = df2.sort_index(axis=1)
    if df1_sorted.equals(df2_sorted):
        return True, None
    else:
        # Show differences
        diff1 = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)
        diff2 = pd.concat([df2_sorted, df1_sorted]).drop_duplicates(keep=False)
        diff = pd.concat([diff1, diff2]).drop_duplicates()
        return False, diff
