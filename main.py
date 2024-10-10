from etl.extract import extract_content, consolidate_dataframe
from etl.transform import remove_columns, convert_date_structure, convert_time_structure
from etl.load import init_database, insert_data
import numpy as np
import math
import os

if __name__ == "__main__":
    
    transformed_dataset_file = 'dataset//transformed_dataset_generated_by_etl.csv'
    consolidate_dataset_file = 'dataset/consolidated_dataset_generated_by_etl.csv'
    dataset_files = ['dataset/DO22OPEN.csv', 'dataset/DO23OPEN.csv', 'dataset/DO24OPEN+(2).csv']
    columns_to_keep = ["ACIDTRAB", "ATESTADO", "ATESTANTE", "CAUSABAS_O", "FONTEINV", "CAUSAMAT", "CIRCOBITO", "DTNASC", "DTOBITO", "ESCFALAGR1", "ESTCIV", "RACACOR", "GESTACAO", "NATURAL", "GRAVIDEZ", "HORAOBITO", "IDADE", "IDADEMAE", "LINHAA", "LINHAB", "LINHAC", "LINHAD", "LINHAII", "LOCOCOR", "MORTEPARTO", "CODMUNOCOR", "OBITOGRAV", "OBITOPUERP", "PESO", "QTDFILMORT", "QTDFILVIVO", "SEXO", "TIPOBITO", "PARTO", "TPMORTEOCO", "TPOBITOCOR"]

    print("Iniciando processo de ETL.")    

    # Processo de consolidação do dataset, processo de Extract
    print("Iniciando processo de consolidação do dataset.")
    
    if not os.path.exists(consolidate_dataset_file):    
        for file_path in dataset_files:
            print(f"Consolidando arquivo {file_path}.")
            
            dataframe = extract_content(file_path)
            dataframe = remove_columns(dataframe, columns_to_keep)
            consolidate_dataframe(consolidate_dataset_file, dataframe)
            
            print(f"Arquivo {file_path} consolidado com sucesso. {len(dataframe)} registros consolidados.")

        dataframe = extract_content(consolidate_dataset_file)
    else:
        dataframe = extract_content(consolidate_dataset_file)

        print(f"Processo de consolidação ignorado. Arquivo de consolidação já existe com {len(dataframe)} registros.")

    # Processo de transformação do dataset. Processo de Transform
    print("Iniciando processo de transformação do dataset.")

    dataframe = dataframe.replace(np.nan, None)

    print("Processo de transformação finalizado.")

    # Processo de carregamento do dataset em banco de dados. Processo de Load
    print("Iniciando processo de carregamento do dataset em banco de dados.")
    init_database()

    data_to_insert = []

    with open('database_scripts/insert_data.sql', 'r') as file:
        insert_query = file.read().split(';')[0]
        file.seek(0)

    dataframe = dataframe.iloc[0:7]
    
    for index, row in dataframe.iterrows():
        data = (
            row["ACIDTRAB"], 
            row["ATESTADO"], 
            row["ATESTANTE"], 
            row["CAUSABAS_O"], 
            row["FONTEINV"], 
            row["CAUSAMAT"], 
            row["CIRCOBITO"], 
            convert_date_structure(row["DTNASC"]), 
            convert_date_structure(row["DTOBITO"]), 
            row["ESCFALAGR1"], 
            row["ESTCIV"], 
            row["RACACOR"], 
            row["GESTACAO"], 
            row["NATURAL"], 
            row["GRAVIDEZ"], 
            convert_time_structure(row["HORAOBITO"]),
            row["IDADE"], 
            row["IDADEMAE"], 
            row["LINHAA"], 
            row["LINHAB"], 
            row["LINHAC"], 
            row["LINHAD"], 
            row["LINHAII"], 
            row["LOCOCOR"], 
            row["MORTEPARTO"], 
            row["CODMUNOCOR"], 
            row["OBITOGRAV"], 
            row["OBITOPUERP"], 
            row["PARTO"], 
            row["PESO"], 
            row["QTDFILMORT"], 
            row["QTDFILVIVO"], 
            row["SEXO"], 
            row["TIPOBITO"], 
            row["TPMORTEOCO"], 
            row["TPOBITOCOR"]
        )

        data_to_insert.append(data)

    insert_data(data_to_insert, insert_query)

    print("Processo de carregamento no banco de dados finalizado.")
    
    print("Processo de ETL finalizado com sucesso.")

    


