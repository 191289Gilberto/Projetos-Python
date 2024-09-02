import sqlite3

def test_query(db_path):
    try:
        # Conectar ao banco de dados
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Executar uma consulta básica
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Tabelas no banco de dados:", tables)
        
        # Fechar a conexão
        conn.close()
    except sqlite3.Error as e:
        print(f'Erro ao conectar ao banco de dados: {e}')

# Caminho para o banco de dados SQLite
db_path = "C:/Users/gilbertosilva/OneDrive - ENGELUX DESENVOLVIMENTO IMOBILIARIO LTDA/Documentos/GitHub/Projetos-Python/db-pipeline/data/NyflightsDB.db"
test_query(db_path)
