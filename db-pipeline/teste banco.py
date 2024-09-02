import sqlite3
import logging
import datetime

# Configuração básica do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_db_connection(db_path):
    try:
        # Tentar conectar ao banco de dados
        conn = sqlite3.connect(db_path)
        logger.info(f'Conexão com o banco de dados estabelecida; {datetime.datetime.now()}')
        
        # Testar uma consulta básica
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info(f'Tabelas no banco de dados: {tables}')
        
        # Fechar a conexão
        conn.close()
    except Exception as e:
        logger.error(f'Erro ao conectar ao banco de dados: {e}; {datetime.datetime.now()}')

# Caminho para o banco de dados SQLite
db_path = "C:/Users/gilbertosilva/OneDrive - ENGELUX DESENVOLVIMENTO IMOBILIARIO LTDA/Documentos/GitHub/Projetos-Python/db-pipeline/data/NyflightsDB.db"
test_db_connection(db_path)
