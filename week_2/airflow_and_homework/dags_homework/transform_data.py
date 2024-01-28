import pandas as pd
import re
import logging

def _remove_zero_in_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    return df[df[column_name] >0]

def _get_date_from_datetime_ser(ser: pd.Series) -> pd.Series:
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    return ser.dt.date

def _camel_case_to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def _rename_column_camel_case_to_snake_case(columns: list) -> list:
    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    return [_camel_case_to_snake_case(col) for col in columns]

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = _remove_zero_in_column(df, 'passenger_count')
    df = _remove_zero_in_column(df, 'trip_distance')
    df['lpep_pickup_date'] = _get_date_from_datetime_ser(df['lpep_pickup_datetime'])

    list_of_vendor_id = df['VendorID'].unique()
    logging.info(f'Homework: Existing values of VendorID: {list_of_vendor_id}')
    
    new_columns = _rename_column_camel_case_to_snake_case(list(df.columns))
    logging.info(f'Homework: Columns transformed to snake case: {[col for col in new_columns if col not in df.columns]}')
    df.columns = new_columns


    # Asserstion: 
    # vendor_id is one of the existing values in the column (currently)
    assert 'vendor_id' in df.columns, 'Column "vendor_id" does not exist in column names.'
    # passenger_count is greater than 0
    assert (df['passenger_count']==0).sum() == 0, 'There are rides with zero passengers'
    # trip_distance is greater than 0
    assert (df['trip_distance']==0).sum() == 0, 'There are rides with zero trip distance'

    return df