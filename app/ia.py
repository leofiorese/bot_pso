from ollama import ChatResponse, chat
import pandas as pd
import subprocess
import logging

from actions.query_to_dataframe.query_to_dataframe import query_to_dataframe

# modelos disponiveis:
# gpt-oss:20b
# llama3.1:8b 
# gemma3:12b

def dataframe_to_text(df, user_prompt):
    resumo = f"""
    Como base, quero que você sempre faça uma análise detalhada de todos os dados fornecidos em meu dataframe e responda com insights valiosos e detalhados, cruzando todos os dados e respeitando o pedido pelo usuário. 

    O DataFrame contém {len(df)} linhas e {len(df.columns)} colunas.

    As colunas são: {', '.join(df.columns)}.

    Segue o dataframe completo:

    {df}

    Escopo a ser realizado: 

    {user_prompt}
    """

    logging.info("Prompt enviado para o modelo IA: \n%s", resumo)
    logging.info("-" * 50)

    return resumo

def split_dataframe(df, max_rows=1000):
    num_chunks = len(df) // max_rows + 1
    chunks = [df[i * max_rows: (i + 1) * max_rows] for i in range(num_chunks)]
    
    return chunks

def generate_insights(df, user_prompt):

    chunks = split_dataframe(df)
    insights = []

    for chunk in chunks:
        chunk_text = dataframe_to_text(chunk, user_prompt)

        print(chunk_text)

        response: ChatResponse = chat(
            model='gpt-oss:20b', 
            messages=[{
                'role': 'user',
                'content': chunk_text}],
            think="medium",
            stream=False,
        )

        print(response['message']['content'])
        
        try:
            insights.append(response['text'])
        except KeyError as e:
            logging.error(f"Chave 'text' não encontrada na resposta: {e}")
            insights.append(str(response))
    
    return insights

