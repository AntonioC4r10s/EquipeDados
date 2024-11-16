from transform import transformar_dados
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
import os

load_dotenv()
DATABASE_URL = st.secrets["DATABASE_URL"]

def get_engine():
    return create_engine(DATABASE_URL)

def insert_db():
    """Função para inserir dados no banco"""
    df = transformar_dados()
    print(df.keys())
    engine = get_engine()
    try:
        with engine.begin() as connection:
            df.to_sql(name='respostas_formulario', con=connection, if_exists='replace')
        return 'Dados atualizados'
    except Exception as error:
        return error

def queries():
    """Função que faz consulta no banco. Não necessária para o load, mas montei para visualização"""
    engine = get_engine()
    query = """
        SELECT 
            timestamp, 
            nome_completo, 
            papel_atual_comunidade, 
            url_trilha_aguardando, 
            area_atuacao, 
            linguagem_frameworks, 
            disponibilidade_horario, 
            comprometimento_hackathon, 
            preparado_trabalhar_em_equipe
        FROM respostas_formulario
        ORDER BY timestamp DESC
        LIMIT 100;
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, con=connection)
            return df
    except Exception as error:
        return f'Ocorreu um erro ao executar a consulta: {error}'

if __name__ == '__main__':
    insert_db()
