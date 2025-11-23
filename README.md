🚀 ETL: MongoDB → PostgreSQL

Pipeline completo de Extração, Transformação e Carga (ETL) utilizando Python, MongoDB e PostgreSQL.
Este projeto demonstra como integrar bases NoSQL e SQL, com limpeza de dados, transformação e carga automatizada, usando boas práticas como variáveis de ambiente (.env) e organização profissional de código.

📌 Objetivo do Projeto

Criar um pipeline capaz de:

✔ Extrair dados de uma coleção MongoDB
✔ Tratar e transformar com Pandas
✔ Converter campos de data
✔ Carregar automaticamente no PostgreSQL com SQLAlchemy
✔ Manter credenciais seguras usando .env
✔ Seguir boas práticas de ETL

🧱 Arquitetura do ETL
MongoDB → Python (Pandas + PyMongo) → PostgreSQL (SQLAlchemy)

🛠 Tecnologias Utilizadas

Python 3.12
MongoDB + PyMongo
Pandas
PostgreSQL + SQLAlchemy
python-dotenv
Git e GitHub

📂 Estrutura do Projeto

ETL/
│── Carga_anac_ce.py        # Script principal do ETL
│── .env                    # Variáveis de ambiente (não enviadas ao GitHub)
│── .gitignore              # Arquivos ignorados no repositório
│── README.md               # Documentação do projeto
