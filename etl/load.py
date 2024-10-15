from configparser import ConfigParser
import psycopg2
import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

db_connection = None

def load_config_database(config_file='database.ini', section='postgres'):
    parser = ConfigParser()
    parser.read(config_file)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]

    return config

def connect_db():
    config_postgres = load_config_database()

    try:
        with psycopg2.connect(**config_postgres) as conn:
            global db_connection
            db_connection = conn
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

def create_tables():
    with open('database_scripts\\create_tables.sql', 'r') as file:
        queries = file.read().replace('\n', ' ')
    execute_query(queries)

def insert_data(data_to_insert, query):
    logging.info("Iniciando inserção dos dados no banco de dados")
    
    if db_connection is None or db_connection.closed:
        connect_db()
        
    cur = db_connection.cursor()

    table_name = query.split(' ')[2]
    
    if check_table_empty(table_name):
        logging.warning(f'Tabela {table_name} já possui registros.')
        return
    
    try:
        for idx, data in enumerate(data_to_insert):
            cur.execute(query, data)

            db_connection.commit()
        
        logging.info(f'Inseridos {len(data_to_insert)} registros no banco de dados')
    except (Exception, psycopg2.DatabaseError) as error:

        logging.error(f"Erro durante a inserção do dado {idx + 1}: {error} & {data}")

    db_connection.commit()
    cur.close()

def check_table_empty(table_name):
    cur = db_connection.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erro durante a verificação da tabela: {error}")
        count = 0

    cur.close()
    return count[0] > 0

def execute_query(query):
    cur = db_connection.cursor()
    try:
        cur.execute(query)
        logging.info("Tabelas criadas caso não existam, se existirem, não serão criadas novamente.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erro durante a criação das tabelas: {error}")

    db_connection.commit()
    cur.close()
    db_connection.close()

def check_code_exists(table_name, code):
    cur = db_connection.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = '{code}'")
        count = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(f"Código {code} não encontrado na tabela {table_name}.")
        count = 0

    cur.close()
    return count[0] > 0

def init_database():
    connect_db()
    create_tables()