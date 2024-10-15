from etl.extract import extract_content, extract_content_from_excel, consolidate_dataframe
from etl.transform import remove_columns, convert_date_structure, convert_time_structure, convert_age, extract_occurrence_state, convert_in_gender_char
from etl.load import init_database, insert_data, check_code_exists
import numpy as np
import logging
import os

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

dimension_tables = 'dataset\Tabelas Dimensão.xlsx'
transformed_dataset_file = 'dataset\\transformed_dataset_generated_by_etl.csv'
consolidate_dataset_file = 'dataset\consolidated_dataset_generated_by_etl.csv'
# dataset_files = ['dataset\DO22OPEN.csv', 'dataset\DO23OPEN.csv', 'dataset\DO24OPEN+(2).csv']
dataset_files = ['dataset\DO23OPEN.csv', 'dataset\DO24OPEN+(2).csv']
columns_to_keep = ["ACIDTRAB", "ATESTADO", "ATESTANTE", "CAUSABAS_O", "FONTEINV", "CAUSAMAT", "CIRCOBITO", "DTNASC", "DTOBITO", "ESCFALAGR1", "ESTCIV", "RACACOR", "GESTACAO", "GRAVIDEZ", "HORAOBITO", "IDADE", "IDADEMAE", "LINHAA", "LINHAB", "LINHAC", "LINHAD", "LINHAII", "LOCOCOR", "MORTEPARTO", "CODMUNOCOR", "OBITOGRAV", "OBITOPUERP", "PESO", "QTDFILMORT", "QTDFILVIVO", "SEXO", "TIPOBITO", "PARTO", "TPMORTEOCO", "TPOBITOCOR"]


if __name__ == "__main__":

    logging.info("Iniciando processo de ETL.")    

    try:
        init_database()
    except Exception as e:
        logging.error(f"Erro ao inicializar o banco de dados: {e}")
        exit(1)

    # Processo de consolidação do dataset, processo de Extract
    logging.info("Iniciando processo de consolidação do dataset.")
    
    if not os.path.exists(consolidate_dataset_file):    
        for file_path in dataset_files:
            logging.info(f"Consolidando arquivo {file_path}.")
            
            dataframe = extract_content(file_path)
            dataframe = remove_columns(dataframe, columns_to_keep)
            consolidate_dataframe(consolidate_dataset_file, dataframe)
            
            logging.info(f"Arquivo {file_path} consolidado com sucesso. {len(dataframe)} registros consolidados.")

        dataframe = extract_content(consolidate_dataset_file)
    else:
        dataframe = extract_content(consolidate_dataset_file)

        logging.info(f"Processo de consolidação ignorado. Arquivo de consolidação já existe com {len(dataframe)} registros.")

    logging.info("Iniciando processo de leitura das tabelas de dimensão")

    dimension_table_df = extract_content_from_excel(dimension_tables, None);

    logging.info("Processo de leitura das tabelas de dimensão finalizado.")

    # Processo de transformação do dataset. Processo de Transform
    logging.info("Iniciando processo de transformação do dataset.")

    dataframe = dataframe.replace(np.nan, None)

    logging.info("Processo de transformação finalizado.")

    data_to_insert = []

    with open('database_scripts/insert_data.sql', 'r') as file:
        queries = file.read().split(';')
        file.seek(0)

    # Processo de carregamento do dataset em banco de dados. Processo de Load
    logging.info("Iniciando processo de carregamento do dataset em banco de dados.")

    data_to_insert = []

    logging.info("Processando tabelas de dimensão")

    id_column = 'id'
    data_column = 'type'

    for sheet_name, sheet_content in dimension_table_df.items():

        for insert_query in queries:
            if f'INTO {sheet_name.upper()}' in insert_query:
                
                data_column = sheet_content.columns[1]  

                insert_query = insert_query.replace('\n', '')

                data_to_insert = []

                logging.info(f"Processando tabela de dimensão {sheet_name}")

                for index, row in sheet_content.iterrows():
                    data = (row[id_column], row[data_column])

                    data_to_insert.append(data)

                insert_data(data_to_insert, insert_query)

                logging.info(f"Processamento da tabela de dimensão {sheet_name} finalizado.")
           
    logging.info("Processamento das tabelas de dimensão finalizado.")
    
    logging.info(f"Processando tabela de fatos")

    insert_query = queries[0]

    data_to_insert = []
    cod_mun_ocor_min_counter = 0

    for _, row in dataframe.iterrows():
        unidade_medida_idade, idade = convert_age(row["IDADE"])
        
        if check_code_exists('COD_MUN_OCOR', row["CODMUNOCOR"]):
            cod_mun_ocor = row["CODMUNOCOR"] 
        else: 
            cod_mun_ocor = 999999
            cod_mun_ocor_min_counter += 1

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
            extract_occurrence_state(row["CODMUNOCOR"]), 
            row["GRAVIDEZ"], 
            convert_time_structure(row["HORAOBITO"]),
            unidade_medida_idade,
            idade, 
            row["IDADEMAE"], 
            row["LINHAA"], 
            row["LINHAB"], 
            row["LINHAC"], 
            row["LINHAD"], 
            row["LINHAII"], 
            row["LOCOCOR"], 
            row["MORTEPARTO"], 
            cod_mun_ocor, 
            row["OBITOGRAV"], 
            row["OBITOPUERP"], 
            row["PARTO"], 
            row["PESO"], 
            row["QTDFILMORT"], 
            row["QTDFILVIVO"], 
            convert_in_gender_char(row["SEXO"]), 
            row["TIPOBITO"], 
            row["TPMORTEOCO"], 
            row["TPOBITOCOR"]
        )

        data_to_insert.append(data)

    logging.warning(f"Total de registros com CODMUNOCOR não encontrados: {cod_mun_ocor_min_counter}")

    insert_data(data_to_insert, insert_query)

    logging.info(f"Processamento da tabela de fatos finalizado.")

    logging.info("Processo de carregamento no banco de dados finalizado.")
    
    logging.info("Processo de ETL finalizado com sucesso.")