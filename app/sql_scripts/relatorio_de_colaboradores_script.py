import logging

def script_sql(dateadd_value):
    # Assumi que o nome da view/tabela no sistema de origem é RELATORIO_DE_COLABORADORES
    # Se for diferente, basta alterar abaixo.
    return f"""

SELECT
    usuarios.LOGIN AS Login,
    usuarios.NOME AS Nome,
    usuarios.SIGLA AS Sigla,
    usuarios.EMAIL AS Email,
    usuarios.CPF AS CPF,
    usuarios.RG AS RG,
    usuarios.TELEFONE AS Telefone,
    usuarios.ENDERECO AS Endereco,
    usuarios.SEXO AS Sexo,
    usuarios.DADOS_PAGAMENTO AS DadosPagamento,
    usuarios.DESCRICAO AS Descricao,
    usuarios.OBS AS Obs,
    usuarios.DT_ADMISSAO AS DataAdmissao,
    usuarios.DT_NASCIMENTO AS DataNascimento,
    cr.NOME AS NomeCentroResultado,
    pj.NOME AS NomePessoaJuridica,
    taxa_historico.VALOR AS ValorTaxaHistorico,
    taxa_historico.INCLUIDO_EM AS IncluidoEm
FROM
    PSO_USUARIOS usuarios
LEFT JOIN
    PSO_CENTROS_RESULTADO cr ON usuarios.CR_ID = cr.CR_ID
LEFT JOIN
    PSO_PESSOAS_JURIDICAS pj ON usuarios.EMP_ID = pj.PJ_ID
LEFT JOIN
    PSO_TAXA taxa ON usuarios.TAXA_ID_CUS = taxa.TAXA_ID
LEFT JOIN
    PSO_TAXA_HISTORICO taxa_historico ON taxa.TAXA_ID = taxa_historico.TAXA_ID;
"""

def gerar_script_final(dateadd_string):
    logging.info(f"relatorio_de_colaboradores_script.py: Recebido dateadd_string = '{dateadd_string}', gerando a query.")
    script_completo = script_sql(dateadd_string)

    return script_completo