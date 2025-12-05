import pandas as pd

class DataQualityLibrary:
    """
    A library of methods for performing data quality checks on pandas DataFrames.
    """

    @staticmethod
    def check_duplicates(df, column_names=None):
        duplicates = df[df.duplicated(subset=column_names, keep=False)]
        assert duplicates.empty, f"Found duplicate rows: {duplicates}"

    @staticmethod
    def check_count(df1, df2):
        assert len(df1) == len(df2), (
            f"Row count mismatch: source={len(df1)}, target={len(df2)}"
        )

    @staticmethod
    def check_data_full_data_set(df1, df2, key_columns=None):
        if key_columns is None:
            key_columns = df1.columns.tolist()
        missing = pd.merge(df1, df2, on=key_columns, how='left', indicator=True)
        missing_rows = missing[missing['_merge'] == 'left_only']
        assert missing_rows.empty, f"Missing rows in target: {missing_rows}"

    @staticmethod
    def check_dataset_is_not_empty(df):
        assert not df.empty, "Dataset is empty!"

    @staticmethod
    def check_not_null_values(df, column_names):
        for col in column_names:
            nulls = df[df[col].isnull()]
            assert nulls.empty, f"Null values found in column '{col}': {nulls}"
