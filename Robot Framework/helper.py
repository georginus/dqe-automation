from robot.api.deco import keyword
import pandas as pd
import json
import re
import os

@keyword
def parse_plotly_table_from_script(script_text):
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
    arr_text = script_text[arr_start:end + 1]
    arr_text = re.sub(r"(\w+):", r'"\1":', arr_text)
    arr_text = arr_text.replace("'", '"')
    arr = json.loads(arr_text)
    table = next((x for x in arr if x.get("type") == "table"), None)
    if not table:
        print("HTML table not found in script!")
        return pd.DataFrame()
    headers = table["header"]["values"]
    values = table["cells"]["values"]
    rows = []
    for i in range(len(values[0])):
        row = [values[j][i] for j in range(len(values))]
        rows.append(row)
    df = pd.DataFrame(rows, columns=headers)
    if "Average Time Spent" in df.columns:
        df["Average Time Spent"] = pd.to_numeric(df["Average Time Spent"], errors="coerce").round(2)
    df["Visit Date"] = df["Visit Date"].astype(str).str.strip()
    print("df_html (from HTML):")
    print(df)
    df.to_csv("debug_df_html.csv", index=False)
    return df

@keyword
def filter_dataframe_by_date(df, filter_date):
    if 'Visit Date' in df.columns:
        df['Visit Date'] = df['Visit Date'].astype(str).str.strip()
        filtered = df[df['Visit Date'] == filter_date].reset_index(drop=True)
        print(f"df_html after filtering by date {filter_date}:")
        print(filtered)
        filtered.to_csv("debug_df_html_filtered.csv", index=False)
        return filtered
    print("No 'Visit Date' column in df_html!")
    return df

@keyword
def prepare_parquet_for_comparison(folder, filter_date=None):
    parquet_files = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(root, file))
    if not parquet_files:
        print("Нет parquet-файлов!")
        pd.DataFrame().to_csv("debug_df_parquet.csv", index=False)
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

    rename_map = {
        'facility_type': 'Facility Type',
        'visit_date': 'Visit Date',
        'avg_time_spent': 'Average Time Spent',
        'min_time_spent': 'Average Time Spent',
    }
    df = df.rename(columns=rename_map)
    print("Столбцы после переименования:", df.columns)
    print("Первые строки после переименования:")
    print(df.head())
    print("Типы данных после переименования:")
    print(df.dtypes)

    if 'Visit Date' in df.columns:
        # Преобразуем к строке и убираем пробелы
        df['Visit Date'] = df['Visit Date'].astype(str).str.strip()
        # Преобразуем к формату YYYY-MM-DD
        df['Visit Date'] = pd.to_datetime(df['Visit Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        print("Все значения Visit Date после преобразования:", df['Visit Date'].unique())
        print("Сравниваем с датой:", repr(filter_date))
        for val in df['Visit Date'].unique():
            print(f"'{val}' == '{filter_date}' ? {val == filter_date}")

    if filter_date and 'Visit Date' in df.columns:
        filtered = df[df['Visit Date'] == filter_date].reset_index(drop=True)
        print("df_parquet после фильтрации по дате:")
        print(filtered)
        filtered.to_csv("debug_df_parquet.csv", index=False)
        df = filtered
    else:
        df.to_csv("debug_df_parquet.csv", index=False)

    if 'Average Time Spent' in df.columns:
        df['Average Time Spent'] = pd.to_numeric(df['Average Time Spent'], errors='coerce').round(2)

    sort_cols = [col for col in ['Facility Type', 'Visit Date', 'Average Time Spent'] if col in df.columns]
    df = df.sort_values(by=sort_cols).reset_index(drop=True)

    print("df_parquet финальный для сравнения:")
    print(df)
    return df

@keyword
def compare_dataframes(df1, df2):
    for col in ["Facility Type", "Visit Date"]:
        if col in df1.columns:
            df1[col] = df1[col].astype(str).str.strip()
        if col in df2.columns:
            df2[col] = df2[col].astype(str).str.strip()
    if "Average Time Spent" in df1.columns and "Average Time Spent" in df2.columns:
        df1["Average Time Spent"] = pd.to_numeric(df1["Average Time Spent"], errors="coerce").round(2)
        df2["Average Time Spent"] = pd.to_numeric(df2["Average Time Spent"], errors="coerce").round(2)

    common_cols = sorted(set(df1.columns) & set(df2.columns))
    df1 = df1[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    df2 = df2[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    print("df1 columns:", df1.columns)
    print("df2 columns:", df2.columns)
    print("df1 dtypes:", df1.dtypes)
    print("df2 dtypes:", df2.dtypes)
    print("df1 head:\n", df1.head())
    print("df2 head:\n", df2.head())
    print("df_html after filter:", df1)
    print("df_parquet after filter:", df2)
    print("df_html shape:", df1.shape)
    print("df_parquet shape:", df2.shape)
    print("df_html columns:", df1.columns)
    print("df_parquet columns:", df2.columns)
    if df1.equals(df2):
        print("DataFrames match!")
        return True, None
    else:
        diff1 = pd.concat([df1, df2]).drop_duplicates(keep=False)
        diff2 = pd.concat([df2, df1]).drop_duplicates(keep=False)
        diff = pd.concat([diff1, diff2]).drop_duplicates()
        print("Difference:\n", diff)
        return False, diff