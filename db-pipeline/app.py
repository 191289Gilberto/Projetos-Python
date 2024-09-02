import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime

load_dotenv()

def data_clean(df, metadados):
    '''
    Função principal para saneamento dos dados
    INPUT: Pandas DataFrame, dicionário de metadados
    OUTPUT: Pandas DataFrame, base tratada
    '''
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

#def feat_eng(df):
 #  Função ???????????????????????????
  #  INPUT: ???????????????????????????
   # OUTPUT: ???????????????????????????
    #'''
    #colocar log info
    #pass

def classifica_hora(hra):
    '''
    Classifica a hora do dia em períodos.
    INPUT: Hora do dia em formato numérico.
    OUTPUT: String representando o período do dia.
    '''
    if 0 <= hra < 6:
        return "MADRUGADA"
    elif 6 <= hra < 12:
        return "MANHA"
    elif 12 <= hra < 18:
        return "TARDE"
    else:
        return "NOITE"

def feat_eng(df):
    '''
    Função para realizar a engenharia de features no DataFrame.
    INPUT: Pandas DataFrame com dados limpos e pré-processados.
    OUTPUT: Pandas DataFrame com novas colunas de features.
    '''
    # Calcula o tempo de voo esperado em horas
    df["tempo_voo_esperado"] = (df["datetime_chegada_formatted"] - df["datetime_partida_formatted"]) / pd.Timedelta(hours=1)
    
    # Calcula o tempo de voo em horas a partir da coluna `tempo_voo`
    df["tempo_voo_hr"] = df["tempo_voo"] / 60
    
    # Calcula o atraso em horas
    df["atraso"] = df["tempo_voo_hr"] - df["tempo_voo_esperado"]
    
    # Adiciona a coluna do dia da semana
    df["dia_semana"] = df["data_voo"].dt.day_of_week  # 0 = segunda-feira, etc.
    
    # Adiciona a coluna de horário (classificação do período do dia)
    df["horario"] = df["datetime_partida_formatted"].dt.hour.apply(lambda x: classifica_hora(x))
    
    # Adiciona a coluna de status do voo (ONTIME ou ATRASO)
    df["flg_status"] = df["atraso"].apply(lambda x: "ATRASO" if x > 0.5 else "ONTIME")
    
    # Adiciona logs informativos sobre a execução
    logger.info(f'Engenharia de features concluída; {datetime.datetime.now()}')
    
    return df


def save_data_sqlite(df):
    try:
        conn = sqlite3.connect("C:/Users/gilbertosilva/OneDrive - ENGELUX DESENVOLVIMENTO IMOBILIARIO LTDA/Documentos/GitHub/Projetos-Python/db-pipeline/data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()

def fetch_sqlite_data(table):
    try:
        conn = sqlite3.connect("C:/Users/gilbertosilva/OneDrive - ENGELUX DESENVOLVIMENTO IMOBILIARIO LTDA/Documentos/GitHub/Projetos-Python/db-pipeline/data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'),index_col=0)
    df = data_clean(df, metadados)
    print(df.head())
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves"])
    df = feat_eng(df)
    #save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')