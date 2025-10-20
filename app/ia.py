import pandas as pd
import ollama
import logging
import os
from dotenv import load_dotenv

from ollama import chat
from ollama import ChatResponse

# Carregar variáveis de ambiente
load_dotenv()

# Carregar o modelo DeepSeek-R1 (ou outro modelo do Ollama)
model = ollama.chat(model="deepseek-r1:8b")  # Certifique-se de que o nome do modelo está correto

# Função para converter o DataFrame em texto (resumo simples)
def dataframe_to_text(df):
    # Converter o DataFrame para texto (exemplo de resumo simples)
    resumo = f"""
    Resumo dos dados de produtividade:

    Total de registros: {len(df)}
    Média de horas realizadas: {df['horas_realizadas'].mean():.2f}
    Média de horas planejadas: {df['horas_planejadas'].mean():.2f}
    Diferença média entre horas realizadas e planejadas: {df['horas_realizadas'].mean() - df['horas_planejadas'].mean():.2f}
    """

    return resumo

# Função para dividir grandes DataFrames em partes menores
def split_dataframe(df, max_rows=1000):
    # Divida o DataFrame em partes menores, caso seja muito grande
    num_chunks = len(df) // max_rows + 1
    chunks = [df[i * max_rows: (i + 1) * max_rows] for i in range(num_chunks)]
    return chunks

# Função para gerar insights com o modelo
def generate_insights(df):
    # Se o texto for muito grande, divida em partes menores
    chunks = split_dataframe(df)
    insights = []

    for chunk in chunks:
        # Prepare um texto para o modelo
        chunk_text = dataframe_to_text(chunk)

        print(chunk_text)
        
        # # Enviar o texto para o modelo DeepSeek-R1 e obter a resposta
        # response = ollama.chat(model="deepseek-r1:8b", messages=[{"role": "user", "content": chunk_text}])  # Correção aqui: usar o método chat diretamente

        # print(response)

        response: ChatResponse = chat(model='deepseek-r1:8b', messages=[
        {
            'role': 'user',
            'content': chunk_text,
        },
        ])
        print(response['message']['content'])
        
        # A resposta do modelo é um objeto ChatResponse
        try:
            insights.append(response['text'])  # Acessando o texto da resposta
        except KeyError as e:
            logging.error(f"Chave 'text' não encontrada na resposta: {e}")
            insights.append(str(response))  # Acessando o texto da resposta
    
    return insights

# Exemplo de uso
if __name__ == "__main__":
    # Suponha que o DataFrame 'df' já tenha os dados carregados
    df = pd.DataFrame({
        'horas_planejadas': [40, 35, 50, 45, 30],
        'horas_realizadas': [38, 30, 45, 50, 28]
    })

    # Gerar insights com o modelo
    insights = generate_insights(df)

    # Exibir os insights gerados
    # for insight in insights:
    #     print(insight)
