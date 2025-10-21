import pandas as pd
import subprocess
import logging

from actions.query_to_dataframe.query_to_dataframe import query_to_dataframe

def dataframe_to_text(df, user_prompt):
    resumo = f"""
    Como base, quero que você sempre faça uma análise detalhada de todos os dados fornecidos em meu dataframe e responda com um insight valioso e detalhado, cruzando todos os dados. 

    Independente da quantidade de dados, sempre responda o que foi pedido.

    A resposta deve seguir um padrão a seguir:

    1) Análise Estatística: Forneça uma análise estatística detalhada dos dados, incluindo médias, medianas, desvios padrão e quaisquer outras métricas relevantes.

    2) Tendências e Padrões: Identifique quaisquer tendências ou padrões significativos nos dados, explicando seu significado e possíveis implicações.

    3) Correlações: Analise as correlações entre diferentes colunas do dataframe, destacando quaisquer relações interessantes ou inesperadas.

    4) Anomalias: Identifique quaisquer anomalias ou outliers nos dados e discuta seu impacto potencial na análise geral.

    5) Insight: Forneça um insight acionável baseado na análise dos dados, sugerindo possíveis ações ou decisões que poderiam ser tomadas com base nas descobertas.

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

def run_ollama_subprocess(chunk_text):
    try:
        # Preparando a entrada (query e prompt) para o Ollama
        input_text = f"Prompt: {chunk_text}"

        # Comando para executar o modelo com Ollama
        process = subprocess.Popen(
            ['ollama', 'run', 'deepseek-r1:8b'],  # 'run' é o comando para rodar o modelo
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Enviar o input_text via stdin
        stdout, stderr = process.communicate(input=input_text.encode(), timeout=300)  # Timeout de 5 minutos

        # Verificando se ocorreu algum erro
        if stderr:
            logging.error(f"Erro ao executar o processo: {stderr.decode()}")
            process.terminate()
            return None

        # Caso contrário, retornamos o resultado da execução do modelo
        process.terminate()
        return stdout.decode()

    except Exception as e:
        logging.error(f"Falha ao executar o subprocesso: {e}")
        process.terminate()
        return None

def generate_insights(df, user_prompt):
    chunks = split_dataframe(df)
    insights = []

    for chunk in chunks:
        chunk_text = dataframe_to_text(chunk, user_prompt)

        # Executa o subprocesso para rodar o modelo com a query e o prompt
        result = run_ollama_subprocess(chunk_text)

        if result:
            insights.append(result)  # Armazena a resposta do modelo
        else:
            logging.error("Falha ao gerar insights com IA.")
            insights.append("Erro ao gerar insights.")

    return insights

if __name__ == "__main__":

    query = """
    SELECT 
        CODIGO_PROJETO, NOME_ATIVIDADE, B_DT_FIM_ATIVIDADE, B_DT_INICIO_ATIVIDADE, 
        DURACAO_PREVISTA_HORAS, TRABALHO_RECURSO_APONTADO_ATIVIDADE, TRABALHO_RECURSO_FALTANDO_ATIVIDADE, TRABALHO_RECURSO_PREVISTO_ATIVIDADE, NOME_RECURSO
    FROM 
        relatorio_pso_planejado
    WHERE 
        codigo_projeto = 'Teste'
    """
    df = query_to_dataframe(query) 

    # df = pd.DataFrame({
    #     'horas_planejadas': [40, 35, 50, 45, 30],
    #     'horas_realizadas': [38, 30, 45, 50, 28]
    # })

    user_prompt = """
    1 - Analisar detalhadamente as colunas que se referem ao trabalho por recurso e correlacionar com a duração prevista de cada atividade (separe por nome de atividade, se o mesmo, logo as atividades são iguais).
    2 - Após essa análise, me informe também se necessito replanejar os recursos alocados (tanto para mais quanto para menos horas) e se há atividades com risco de atraso baseado nas horas de trabalho total apontado, horas de trabalho total faltando, horas de trabalho total previsto.
    3 - Dê previsões (data) de conclusão das atividades com base nas horas apontadas e faltantes, considerando 8 horas diárias de trabalho de cada recurso.
    """

    if df is not None:
        insights = generate_insights(df, user_prompt)

    for insight in insights:
        print(insight)
