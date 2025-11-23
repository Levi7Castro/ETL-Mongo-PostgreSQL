from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

# Carrega variáveis do .env
load_dotenv()

# ----------- MongoDB ----------
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")

client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client[mongo_db]
collection = db.get_collection(mongo_collection)

columns = {
    'Numero_da_Ocorrencia': 1,
    'Numero_da_Ficha': 1,
    'Operador_Padronizado': 1,
    'Classificacao_da_Ocorrência': 1,
    'Data_da_Ocorrencia': 1,
    'Municipio': 1,
    'UF': 1,
    'Regiao': 1,
    'Descricao_do_Tipo': 1,
    'Historico': 1
}

filter = {'UF': 'CE'}

data = list(collection.find(filter, columns))
df = pd.DataFrame(data)

df = df.drop(columns=["_id"], errors="ignore")
df['Data_da_Ocorrencia'] = pd.to_datetime(df['Data_da_Ocorrencia'])


# ----------- PostgreSQL ----------
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")
pg_host = os.getenv("POSTGRES_HOST")
pg_port = os.getenv("POSTGRES_PORT")
pg_database = os.getenv("POSTGRES_DB")

try:
    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
    )
    df.to_sql("anac_ce", engine, if_exists="replace", index=False)
    print("Tabela enviada com sucesso!")

except SQLAlchemyError as e:
    print("Erro ao inserir dados no PostgreSQL:", e)
