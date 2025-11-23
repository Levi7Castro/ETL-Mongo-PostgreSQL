import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def carregar_estados_ibge():

    Estados = ['CE','MA','PI','RN','SP','RJ','MG','BA']
    dfs = []

    # --- Coleta de dados da API ---
    try:
        for estado in Estados:
            url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{estado}"
            response = requests.get(url)
            response.raise_for_status()  # gera erro se não for 200

            data = response.json()
            df = pd.json_normalize(data)
            dfs.append(df)

    except requests.RequestException as e:
        print("Erro ao consultar a API IBGE:", e)

    # Concatenar todos os estados
    df = pd.concat(dfs, ignore_index=True)

    # Selecionar e renomear colunas
    colunas = ['id', 'sigla', 'nome', 'regiao.nome']
    renomear_colunas = {
        'id': 'Id',
        'nome': 'Estado',
        'sigla': 'Uf',
        'regiao.nome': 'Regiao'
    }

    df = df[colunas].rename(columns=renomear_colunas)

    # --- Conexão com o banco ---
    try:
        user = "postgres"
        password = "1234"
        host = "localhost"
        port = "5432"
        database = "DW"

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()

        df.to_sql("estados", engine, if_exists="replace", index=False)
        print("Tabela enviada com sucesso!")

    except SQLAlchemyError as e:
        print("Erro ao inserir dados no PostgreSQL:", e)

    finally:
        conn.close()
