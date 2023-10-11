import pandas as pd 
import numpy as np
from pyarabic.araby import strip_tashkeel
import re
import unicodedata
from datetime import datetime
import logging
import string
import ast


# To write logs to a file
logging.basicConfig(filename='pipeline.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def test_keys_values(dictionary):
    if not isinstance(dictionary, dict):
        raise ValueError("Input must be a dictionary")
           
    for key, value in dictionary.items():
        if any(char in string.punctuation and char not in ['.', "-"] for char in key):
            raise ValueError(f"The Key: {key} Cant Be Containig punctuations It Must Be A Mistake Please Review")
        if isinstance(value, list):
            for v in value:
                if any(char in string.punctuation and char not in ['.', "-"] for char in v):
                    raise ValueError(f"The value: {value} Which Is It's key: {key} Cant Be Containig punctuations It Must Be A Mistake Please Review")
        elif isinstance(value, str):
            if any(char in string.punctuation and char not in ['.', "-"] for char in value):
                    raise ValueError(f"The value: {value}  Which Is It's key: {key} Cant Be Containig punctuations It Must Be A Mistake Please Review")

                    
def test_locations(locations):
    if not isinstance(locations, list):
        raise ValueError("Input must be a List")
    for loc in locations:
        if not isinstance(loc, dict):
            raise ValueError("Input must be a Dictionary")
        for key, value in loc.items():
            if key not in ["studio", "city", "country"]:
                raise ValueError(f" the key : '{key}' should be In 'studio', 'city', 'country'")
             


# def string_corrector(text):
correction_dict = {
    ".": "",
    "،": ",",
    "--": ",",
    u"\xa0": " ",
    "\n": " ",
    "ريجسير": "ريجيسير",
    "الفيزي": "الفيزى",
    "خيري": "خيرى",
    "السباعي": "السباعى",
    "نصري": "نصرى",
    "فخري": "فخرى",
    "سالفي": "سالفى",
    "نعمتالله": "نعمت الله",
    "حلمى": "حلمي",
    "السنباطى": "السنباطي",
    "سامى": "سامي",
    "فوسكلو" : "فوسكولو",
    "فتحي" : "فتحى",
    "قوره" : "قورة",
    "كوينى" : "كويني",
    "ذو الفقار": "ذوالفقار",
    "STORY BY":"STORY",
    "LADISLAO": "LADISLAS",
    "MARIO VOLPI": "MARIO VOLPE",
    "ALJAZIRLI": "AL-JAZAYIRILI",
    "EL DIN": "ELDIN",
    "ZULFIKAR": "ZULFIQAR",
    " و ": ",",
    "  ": " ",
    " WA ": ",",
    "LOTUS FILMS": "LOTUS FILM",
    "NASSIBIAN": "NASBYAN",
    "STUDIOS": "STUDIO",
    "استيفان": "إستيفان",
    "RŪSTY": "RŪSTĪ",
    "ISTYFĀN": "ISTĪFĀN",
    "الابياري": "الإبياري",
    "سامى": "سامي",
    "محمد سامي مصطفى": "محمد مصطفى سامي",
    "السباعى": "السباعي",
    "BDĪʻ": "BADĪʻ",
    "MUḤAMMAD SĀMĪ MUṢṬAFÁ": "MUḤAMMAD MUṢṬAFÁ SĀMĪ",
    ";": ",",
    "JŪHAR": "JAWHAR",
    "بشاره": "بشارة",
    "حلمى": "حلمي",
    "فخرى": "فخري",
    "خيرىة": "خيرية",
    "مكاوى": "مكاوي",
    "شكرى": "شكري",
    "كوينى": "كويني",
    "ميمى": "ميمي",
    "فوزىة": "فوزية",
    '"': "",
    "~": "..",
    "أفلام": "افلام",
    "؛": ",",
    "فولبى": "فولبي"
}
def replace_word(text, wrong_word, correct_word):
    # Use re.sub with a regular expression to replace the wrong word with the correct word
    # The re.IGNORECASE flag ensures the replacement is case-insensitive
    return re.sub(re.escape(wrong_word), correct_word, text, flags=re.IGNORECASE)

def apply_correction(value):
    if isinstance(value, (int, float, datetime)):
        # If the value is an integer, float, or datetime, return it as is
        return value
    elif isinstance(value, str):
        # Iterate through the correction dictionary and replace keys with values
        for wrong_word, correct_word in correction_dict.items():
            if wrong_word in value:
                try:
                    value = replace_word(value, wrong_word, correct_word)

                except Exception as E:
                    logging.error(f"'An error occurred during Fixing the word {wrong_word} because of {E} ")
        return value
    elif isinstance(value, dict):
        # Recursively correct keys and values within the dictionary
        corrected_dict = {apply_correction(k): apply_correction(v) for k, v in value.items()}
        return corrected_dict
    elif isinstance(value, list):
        # Recursively correct elements within the list
        return [apply_correction(item) for item in value]
    else:
        return value
def normalize_text(text):
    if isinstance(text, str):
        return unidecode(text)
    else:
        return text
    
def strip_names(names_row):
    if isinstance(names_row, str):
        try :
            stripped_names = [name.strip().upper() for name in names_row.split(',')]
            return ','.join(stripped_names)
        except Exception as E:
            logging.error(f"An Error occured during striping the text : {Text} because of {E}")
    return names_row

def strip_name(name):
    try :
        name = str(name).strip().upper()    
    except Exception as E:
        logging.error(f"An Error Has Been Occur During Stripping the word {word}")
    return name
def remove_diacritics(text):
    if isinstance(text, str):
        return strip_tashkeel(text)
    else:
        return text
def extract_numbers(text):
    # Use regular expression to find all numbers (integers or decimals)
    numbers = re.findall(r'\d+\.\d+|\d+', text)
    if numbers:
        return "-".join(numbers)
    return None

def extract_minutes(value):
    # Regular expression pattern to match numbers
    pattern = r'\b\d+\b'
    
    # Find all matches in the value
    matches = re.findall(pattern, value)
    
    # Convert strings to actual integers and return the first match
    if matches:
        return int(matches[0])
    return None  # Return None if no match is found  

def strip_keys_values(text):
    if not isinstance(text , dict):
        
        # Regular expression to match key-value pairs
        pattern = r'([^,:]+)\s*:\s*([^,]+)(?:,|$)'

        # Find all matches in the text
        matches = re.findall(pattern, str(text))


        # Convert matches to a dictionary and strip keys/values
        result_dict = {}
        for key, value in matches:
            values_list = [apply_correction(val.strip().upper()) for val in value.split("&")]
            result_dict[apply_correction(key.strip().upper())] = values_list if len(values_list) > 1 else apply_correction(value.strip().upper())
        try:
            test_keys_values(result_dict)
            return result_dict
        except Exception as E:
            logging.error(f"{E}")
            return {"Mistake":f"{E}"}
    elif isinstance(text, dict):
        try:
            test_keys_values(text)
            return text
        except Exception as E:
            logging.error(f"{E}")
            return text
    else:
        return {}

        

def process_locations(locations_str):
    if not isinstance(locations_str ,list):
        locations = str(locations_str).split(';')
        locations_list = []
        for location in locations:
            parts = location.strip().split(',')
            if len(parts) == 3:
                studio, city, country = map(str.strip, parts)
                locations_list.append({
                    'studio': studio.upper(),
                    'city': city.upper(),
                    'country': country.upper()
                })
        try:
            test_locations(locations_list)
            return locations_list
        except Exception as E:
            logging.error(f"{E}")
            return [{mistake: E}]
    else:
        return locations_str

def normalize_to_nfc(string):
    if isinstance(string, (int, float, dict, object ,datetime)):
        return string
    elif isinstance(string, str):
        return unicodedata.normalize('NFC', str(string)) # Ensure the input is a string



def join_columns_and_create_new_columns(df, columns_list):
    def convert_to_column_sets(columns_list):
        common_name = columns_list[0]
        cols_to_join = columns_list[:]
        return [(common_name, cols_to_join)]
    column_sets = convert_to_column_sets(columns_list)
    for column_set in column_sets:
        for new_col_name, cols_to_join in column_sets:
            print(new_col_name, cols_to_join)
            def process_notes(row):
                notes_list = [str(row[col]).strip(" .") for col in cols_to_join if row[col] is not None and not pd.isna(row[col])]
                return ', '.join(notes_list).strip(' .')
            if all(col in df.columns for col in cols_to_join):
                df[new_col_name] = df.apply(process_notes, axis=1)
                # Drop the original columns that have been joined, except the new column
                drop_cols = [col for col in cols_to_join if col != new_col_name]
                df.drop(drop_cols, axis=1, inplace=True)

    return df
def process_paths(df, link_column, format_column, digital_file_column):
    try:
        # Create the "Path" column
        df["Path"] = df[link_column] + "." + df[format_column]
        df["Web_Path"] = df["Path"].str.upper().str.replace("WEBOPTIMIZED", "LOW_MEMORY").str.replace("../BEHNAFSDATABASE/", "static/Images/", regex=True).astype("string")
        # Drop specified columns (including the provided digital file column)
        columns_to_drop = [link_column, format_column, digital_file_column]
        df.drop(columns=columns_to_drop, inplace=True)
        logging.info("Columns Path and Web_Path Has been Created Successfully")
    except Exception as E:
        logging.error(f"Columns Path and Web_Path Has Faild because of {E}")

    return df
# Function to convert string representation to dictionary
def parse_dict(s):
    try:
        # Using ast.literal_eval to safely evaluate the string as a Python literal
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return s  # Return the original string if it can't be converted
