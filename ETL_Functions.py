import pandas as pd
import re
import logging
import unicodedata
from datetime import datetime
import string
from string_processing import *
from login_paramters import *
from sqlalchemy import create_engine, Text, TIMESTAMP, Integer, Float, Boolean, exc
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
import json

def extract_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        dataframes = {}  # Dictionary to hold DataFrames for each sheet

        for sheet in xls.sheet_names:
            df_name = sheet.replace("-", "_")  # Adjust DataFrame name
            df = pd.read_excel(xls, sheet)
            column_corrections = {
                '\n': ' ',
                r"[\(\)\.,]": "",
                " ": "_",
                "__": "_",
                '#': '',
                "/": "_"
            }
            # Loop through the corrections dictionary and replace patterns in column names
            for pattern, replacement in column_corrections.items():
                df.columns = df.columns.str.replace(pattern, replacement, regex=True).str.strip().str.strip("_")
            df.rename(columns={'Film_Id_Year_of_Dist_+_film_order': 'Film_ID',
                            'Filimig_location_countries_or_cities': 'Filimig_location',
                            "Film_Type_Fiction_Documentary_Sketch": "Film_Type", "Subject_Keywords": "Keywords",
                            "Subject___Key_words": "Keywords","People__items_in_photo": "People_items_in_photo"}, inplace=True)
            if ("Film_ID" not in df.columns and "Film_Id" not in df.columns) and "Call_No" in df.columns:
                df["Film_ID"] = df["Call_No"].str.split("_").str[0].astype("string")
            # Iterate through columns and attempt to convert string representations to dictionaries
            for col in df.columns:
                df[col] = df[col].apply(lambda x: parse_dict(x) if isinstance(x, str) else x)

            dataframes[df_name] = df  # Store DataFrame for this sheet

        logging.info(f"Data has been extracted successfully with sheets: {dataframes.keys()}")
        return dataframes
    except Exception as e:
        logging.error(f"Loading data has failed because: {e}")

def transform_data(df , col_single = [], col_list = [], col_dict= [], col_int= [],
                  replace_x = [], col_numbers = [], col_locations = [], col_to_date = [], col_durations= [],
                   col_sets_to_join=[], link_col=None, format_col=None , digital_file_col=None):
    
    column_corrections = {
        '\n': ' ',
        r"[\(\)\.,]": "",
        " ": "_",
        "__": "_",
        '#': '',
        "/": "_"
    }
    # Loop through the corrections dictionary and replace patterns in column names
    for pattern, replacement in column_corrections.items():
        df.columns = df.columns.str.replace(pattern, replacement, regex=True).str.strip().str.strip("_")
    df.rename(columns={'Film_Id_Year_of_Dist_+_film_order': 'Film_ID',
                       'Filimig_location_countries_or_cities': 'Filimig_location',
                       "Film_Type_Fiction_Documentary_Sketch": "Film_Type", "Subject_Keywords": "Keywords",
                      "Subject___Key_words": "Keywords","People__items_in_photo": "People_items_in_photo"}, inplace=True)
    if ("Film_ID" not in df.columns and "Film_Id" not in df.columns) and "Call_No" in df.columns:
        df["Film_ID"] = df["Call_No"].str.split("_").str[0].astype("string")
    # Applying column joining and creating new columns
    if col_sets_to_join:
        try:
            df = join_columns_and_create_new_columns(df, col_sets_to_join)
            logging.info(f"Columns {col_sets_to_join}joined and new columns created successfully.")
        except Exception as e:
            logging.error(f"An error occurred during joining columns and creating new columns: {e}")
    
            
    for col in df.columns:
        if col not in ["Path", "Web_Path"]:
            try:
                df[col] = df[col].apply(apply_correction)
                logging.info(f"The Texts of The Column {col} Has been Corrected According To the Dictionary Of Wrong Words Successfully")   
            except Exception as E:
                logging.error(f"An Error Has Occur During Correcting Words of The Column {col} Because of  {E}")    
    if col_dict != []:
        for column in col_dict:
            try:
                df[column] = df[column].apply(lambda x: strip_keys_values(x))
                logging.info(f"The Column {column} Have Been Transformed Into A Dictionary Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Transforming The Column {column} Into Dictionary Because Of {E}")
    if col_locations != []:
        for column in col_locations:
            try:
                df[column] = df[column].apply(process_locations)
                logging.info(f"The Column {column} Has Been Transformed Into A Location Dictionary Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Transforming The Column {column} Into Dictionary Because of {E}")
 
        
    # Identify string columns
    string_columns = df.select_dtypes(include=[object]).columns

    # Fill missing values in string columns
    df[string_columns] = df[string_columns].fillna("") 
    df[string_columns] = df[string_columns].apply(normalize_to_nfc)

  
    if link_col and format_col and digital_file_col and (link_col[0] in df.columns):
        link_col = link_col[0]
        format_col = format_col[0]
        digital_file_col = digital_file_col[0]
        try:
            df = process_paths(df, link_col, format_col, digital_file_col)
        except Exception as e:
            logging.error(f"An error occurred during joining columns of path and creating new columns: {e}")
    if col_single != []:
        for column in col_single:
            try:
                df[column] = df[column].apply(strip_name).astype("string")
                logging.info(f"The Column {column} Has Been Stripped Successfully")
            except Exception as E:
                logging.error(f"An Error Has Been Occur During Spliting The Single Words in column: {column} ")

    if col_list != []:
        for column in col_list:
            try:
                df[column] = df[column].apply(strip_names).astype("string")
                logging.info(f"The Column {column} Has Been Stripped Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Stripping the Column {column} Because of  {E}")
    
    if replace_x != []:
        for column in replace_x:
            try:
                df[column]= df[column].astype(str).apply(lambda x: x.replace("X", "0").replace("x", "0"))       
                df[column]= pd.to_numeric(df[column], errors='coerce').astype('Int64')
                logging.info(f"The Column {column} Has Been Replaced 'X' With '0' Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Replaceing 'X' in the Column {column} Because of  {E}")
    if col_int != []:
        for column in col_int:
            try:
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                logging.info(f"The Column {column} Has Been Transformed to Integers Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Transforming the Column {column} Into Integer Because of  {E}")

    if col_numbers != []:
        for column in col_numbers:
            try:
                df[column] = df[column].apply(extract_numbers).astype("string")
                logging.info(f"Numbers Has Been Extracted from The Column {column} Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Extracing Numbers from  the Column {column}  Because of  {E}")

    
    if col_to_date != []:
        for column in col_to_date:
            try:
                df[column] = pd.to_datetime(df[column])
                logging.info(f"The Column {column} Has Been Transformed Into DateTime Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Transforming the Column {column} Into DateTime Because of  {E}")
    if col_durations != []:
        for column in col_durations:
            try:
                df[column] = df[column].astype(str).apply(extract_minutes).astype(pd.Int64Dtype(), errors='ignore')
                logging.info(f"Minuts Has Been Extracted From The Column {column} Successfully")
            except Exception as E:
                logging.error(f"An Error Has Occur During Extracting Minutes From The Column {column} Because of  {E}")   
    return df

db_params = {
    'user': user,
    'password': password,
    'host': host,
    'port': port,
    'database': database}

def upload_to_database(df_name, selected_dataframe):
    try:
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Check if the table exists
        table_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{df_name}')"
        table_exists = pd.read_sql_query(table_exists_query, engine).iloc[0, 0]

        # If the table doesn't exist, create it and upload all data
        if not table_exists:
            dtypes_mapping = get_dtypes_mapping(selected_dataframe, df_name)
            selected_dataframe.to_sql(df_name, engine, index=False, if_exists='replace', dtype=dtypes_mapping)
            logging.info(f"Table '{df_name}' created and all data uploaded.")
            session.commit()
        else:
            # Table exists, so get existing data from the database
            existing_data_query = f'SELECT * FROM "{df_name}"'
            existing_data_df = pd.read_sql(existing_data_query, engine).astype(selected_dataframe.dtypes)

            # Check for non-existing rows in the selected dataframe
            non_existing_rows_df = find_non_existing_rows(selected_dataframe, existing_data_df)
            if not non_existing_rows_df.empty:
                dtypes_mapping = get_dtypes_mapping(non_existing_rows_df, df_name)
                non_existing_rows_df.to_sql(df_name, engine, index=False, if_exists='append', dtype=dtypes_mapping)
                logging.info(f"Non-existing rows in DataFrame '{df_name}' successfully uploaded to SQL table '{df_name}'")
                session.commit()
            else:
                logging.info(f"No non-existing rows to append to '{df_name}'")

    except Exception as e:
        logging.error(f"An error occurred during uploading DataFrame {df_name}: {e}")

def get_dtypes_mapping(dataframe, df_name):
    dtypes_mapping = {}
    for column_name, dtype in dataframe.dtypes.items():
        if dtype.name == 'object':
            dtypes_mapping[column_name] = Text
        elif dtype.name == 'datetime64[ns]':
            dtypes_mapping[column_name] = TIMESTAMP
        elif dtype.name == 'Int64':
            dtypes_mapping[column_name] = Integer
        elif dtype.name == 'int64':
            dtypes_mapping[column_name] = Integer
        elif dtype.name == 'string':
            dtypes_mapping[column_name] = Text
        elif dtype.name == 'float64':
            dtypes_mapping[column_name] = Float
        elif dtype.name == 'bool':
            dtypes_mapping[column_name] = Boolean
        else:
            dtypes_mapping[column_name] = Text

    # Set specific columns to JSONB data type if df_name starts with 'film' or 'Film'
    if df_name.lower().startswith('film'):
        dtypes_mapping['Crew_In_Arabic'] = JSONB
        dtypes_mapping['Crew_In_Latin'] = JSONB
        dtypes_mapping['Filimig_location'] = JSONB

    return dtypes_mapping

def process_value(value):
    """
    Process a value to handle various data types.
    """
    if isinstance(value, (list, dict)):
        # Convert lists and dictionaries to JSON strings for comparison
        return json.dumps(value, sort_keys=True)
    elif isinstance(value, pd.Timestamp):
        # Convert datetime objects to strings for comparison
        return str(value)
    else:
        # Convert other types to strings for comparison
        return str(value)

def find_non_existing_rows(selected_dataframe, existing_data_df):
    non_existing_rows = []

    for _, selected_row in selected_dataframe.iterrows():
        try:
            selected_row_values = selected_row.apply(process_value)

            found = False
            for _, existing_row in existing_data_df.iterrows():
                existing_row_values = existing_row.apply(process_value)

                if all(selected_row_values == existing_row_values):
                    found = True
                    break

            if not found:
                non_existing_rows.append(selected_row)
        except Exception as e:
            logging.error(f"Error processing row: {e}")

    return pd.DataFrame(non_existing_rows, columns=selected_dataframe.columns)



