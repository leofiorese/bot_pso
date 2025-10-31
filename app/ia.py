from ollama import ChatResponse, chat
import pandas as pd
import tabulate
import logging

from actions.query_to_dataframe.query_to_dataframe import query_to_dataframe

from actions.upsert_data.upsert_insights_llm import upsert_data as upsert_insights_llm

# modelos disponiveis:
# gpt-oss:20b + think = medium
# llama3.1:8b 
# gemma3:12b
# magistral:24b

def dataframe_to_text(df, user_prompt):
    df_markdown = df.to_markdown(index=False, tablefmt="github")

    resumo = f"""
    Como premissa inicial, quero que você sempre faça uma análise detalhada de todos os dados fornecidos em meu dataframe e responda com insights valiosos e detalhados, cruzando todos os dados e respeitando o escopo definido pelo usuário.

    {"-"*50}

    Papel: 
    
    Você é um Analista de Dados e Projetos Sênior, especialista em finanças e operações. 
    Sua principal habilidade é analisar DataFrames brutos, identificar padrões, calcular métricas-chave e fornecer insights acionáveis que ajudem na tomada de decisão gerencial.

    {"-"*50}

    Premissas da Análise:
    - Colunas que contêm o nome "VALOR" ou "CUSTO" representam valores monetários em Reais (R$).
    - Colunas que contêm o nome "TRABALHO", "HORAS" ou "DURACAO" representam horas.
    - Colunas que contêm o nome "PERC" representam percentuais (%).
    - Colunas que contêm o nome "VALOR_PROJETO", "VALOR_CUSTO_TOTAL" representam valores monetários totais do projeto, logo para mesmos valores de "PROJ_ID" deve-se considerar apenas um.
    - Colunas que contêm o nome "TRABALHO_TOTAL_...", sendo "..." qualquer sufixo, representam horas totais de trabalho de uma atividade, logo para mesmos valores de "ATIV_ID" deve-se considerar apenas um.
    - Colunas que contêm o nome "TRABALHO_..._PROJ", sendo "..." qualquer sufixo, representam horas totais de trabalho de um projeto, logo para mesmos valores de "PROJ_ID" deve-se considerar apenas um.
    - Colunas que contêm o nome "TRABALHO_..._ATIVIDADE", sendo "..." qualquer sufixo, representam horas totais de trabalho de uma atividade, logo para mesmos valores de "ATIV_ID" deve-se considerar apenas um.

    {"-"*50}

    Regras do negócio:
    - São trabalhadas 8 horas / dia. Totalizando 40 horas / semana.
    - Ausências não são contabilizados.

    {"-"*50}

    Modelo de Resposta Esperado:

    1) Análise Resumida (O que os dados mostram?): 
    Forneça uma análise detalhada dos dados. Use tabelas Markdown com métricas-chave (Totais, Médias, Medianas, Máx/Mín) sempre que necessário para resumir os dados.

    2) Insights Acionáveis (O que isso significa?): 
    Forneça insights valiosos e detalhados, cruzando todos os dados. O insight deve ser um texto relevante que agregue valor ao usuário para facilitar o entendimento de grande quantidade de linhas e apoiar a tomada de decisão.

    3) Pontos de Atenção (Riscos e Oportunidades):
    Com base na análise, liste em bullet points os principais riscos, gargalos ou oportunidades identificados (ex: "Risco: Projeto X está com margem negativa de -R$ Y", "Oportunidade: Atividade Z tem um custo/hora muito baixo", "Atenção: 80% das horas estão concentradas em apenas um recurso").

    4) Recomendações (O que deve ser feito?):
    Forneça recomendações práticas e específicas para mitigar riscos ou aproveitar oportunidades identificadas na análise. Utilize bullet points para listar as recomendações.

    {"-"*50}

    Formato da Resposta Esperado:
    - Formato JSON com os campos:
        - chaves_identificadoras -> Deve-se criar um campo para cada chave identificadoras fixas (não criar nem retirar as chaves descritas) presente no dataframe (PROJ_ID, CODIGO_PROJETO, VALOR_PROJETO, TX_ID_RECURSO, ATIV_ID, ATRIB_ID, APON_ID, USU_ID, PROJREC_ID). Caso a chave não exista no dataframe, atribui-se NULL / NONE e nunca invente valores, apenas aqueles vistos no dataframe.
            - IMPORTANTE: A chave 'PROJ_ID' é obrigatória e deve sempre ser preenchida. Caso não exista no dataframe, a análise não poderá ser realizada.
            - IMPORTANTE: Em caso de multiplos valores de PROJ_ID no dataframe, deve-se fazer uma análise para cada PROJ_ID separadamente, gerando um JSON para cada um.
            - Importante: Em caso de multiplos PROJ_ID, o campo 'chaves_identificadoras' deve conter todos os PROJ_ID analisados, cada um com suas respectivas chaves identificadoras fixas.
        - analise_resumida -> Deve-se criar um campo para cada dado da análise e para cada métrica.
        - insights_acionaveis -> Deve-se um campo para os insights detalhados dados. Em formato de bullet points / texto completo.
        - pontos_de_atencao -> Deve-se criar um campo para os riscos, atenção, gargalos ou oportunidades. Em formato de bullet points / texto completo.
        - recomendacoes -> Deve-se criar um campo para as recomendações práticas e específicas de acordo com a análise feita por você. Está é a ultima etapa para apoio à tomada de decisão. Em formato de bullet points / texto completo.

    Exemplo do JSON Esperado (Adaptar conforme os dados analisados, escopo do usuário e métricas disponíveis):

    "```json
    {{
        "chaves_identificadoras": {{
            "PROJ_ID": ...,
            "CODIGO_PROJETO": "",
            "VALOR_PROJETO": ...,
            "USU_ID": ...
            ...
        }},
        "analise_resumida": {{
            "margem_...": ...,
            "custo_hora_medio": ...,
            "media_horas_por_projeto": ...,
            "MÉTRICA X": {{
                "total": ...,
                "média": ...,
                "mediana": ...,
                "máximo": ...,
                "mínimo": ...
            }},
            ...
        }},
        "insights_acionaveis": [
            "- Insight 1: descrição detalhada do insight.",
            "- Insight 2: descrição detalhada do insight.",
            ...
        ],
        "pontos_de_atencao": [
            "- Risco: descrição detalhada do risco.",
            "- Oportunidade: descrição detalhada da oportunidade.",
            ...
        ]
        "recomendacoes": [
            "- Recomendação 1: descrição detalhada da recomendação.",
            "- Recomendação 2: descrição detalhada da recomendação.",
            ...
        ]
    }}
    ```"

    {"-"*50}

    Observações da Resposta Esperada:

    - Sempre reponda somente no formato esperado. 
    - Não pode-se fugir do formato de resposta dado.
    - Sempre mantenha os nomes das cinco chaves do JSON, colocando cada dado em sua respectiva seção correspondente (chaves_identificadoras; analise_resumida; insights_acionaveis; pontos_de_atencao; recomendacoes) 
    - Sempre considere o escopo definido pelo usuário ao gerar a análise e os insights.
    - Nunca crie tabelas nos insights, apenas utilize texto descritivo e bullet points. O formato pode ser -> "- Insight 1: descrição detalhada do insight."
    - Sempre que possível, utilize métricas como médias, medianas, totais, máximos e mínimos para enriquecer a análise.
    - Podem haver múltiplos insights, não se limite a apenas um.
    - Podem haver múltiplos pontos de atenção, não se limite a apenas um.
    - Podem haver múltiplas recomendações, não se limite a apenas um.
    - Seja objetivo e direto ao ponto, evitando rodeios desnecessários.
    - Utilize linguagem formal e técnica, adequada para um público gerencial.
    - A resposta final deve ser exclusivamente em Português do Brasil (pt-BR)

    {"-"*50}

    Segue o dataframe completo:

    {df_markdown}

    {"-"*50}

    - Sempre reponda estritamente e somente no formato esperado descrito no item "Exemplo do JSON Esperado (Adaptar conforme os dados analisados, escopo do usuário e métricas disponíveis):".
    - Não pode-se fugir do formato de resposta dado no item "Formato da Resposta Esperado:".
    - Sempre siga estritamente conforme as "Observações da Resposta Esperada:".
    - Sempre mantenha os nomes das cinco chaves do JSON, colocando cada dado em sua respectiva seção correspondente (chaves_identificadoras; analise_resumida; insights_acionaveis; pontos_de_atencao; recomendacoes)

    {"-"*50}

    Escopo a ser realizado: 

    {user_prompt}

    {"-"*50}
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

        clear_response = response['message']['content']

        print(clear_response)

        logging.info("Resposta recebida da LLM: \n%s", clear_response)
        logging.info("-" * 50)

        upsert_insights_llm(clear_response)

        try:
            insights.append(clear_response)
        except KeyError as e:
            logging.error(f"Chave 'text' não encontrada na resposta: {e}")
            insights.append(str(clear_response))
    
    return insights

