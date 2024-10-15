import pandas as pd

def extract_content(file_path):

    file_content = pd.read_csv(file_path, sep=';', low_memory=False, dtype=str)

    return file_content

def extract_content_from_excel(file_path, sheet_name):

    file_content = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)

    return file_content

def consolidate_dataframe(consolidated_file, dataframe):

    try:
        existing_data = pd.read_csv(consolidated_file, sep=';', low_memory=False, dtype=str)
        dataframe = pd.concat([existing_data, dataframe], ignore_index=True)
    except FileNotFoundError:
        pass  # If the file does not exist, we will create it

    dataframe.to_csv(consolidated_file, sep=';', index=False)