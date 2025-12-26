import os
import shutil
import logging

def arquivar_csv(csv_file_path, table_name):
    """
    Move o arquivo CSV processado para o diretório de rede,
    renomeando-o para o nome da tabela (ex: RELATORIO_ORCADO.csv).
    ATENÇÃO: Isso sobrescreve o arquivo anterior na pasta de destino.
    """
    if not os.path.exists(csv_file_path):
        logging.warning(f"Tentativa de arquivar arquivo inexistente: {csv_file_path}")
        return

    try:
        # --- 1. CONFIGURAÇÃO DO CAMINHO DE REDE ---
        dir_destino = r"Z:\3-Corporativo\PMO\0-Gerência do PMO\0-Internos\04- PS Office\04. Relatório\BASE"
        
        # Cria a pasta se não existir
        os.makedirs(dir_destino, exist_ok=True)

        # --- 2. DEFINIÇÃO DO NOME (Baseado no Table Name) ---
        # Garante que o nome seja seguro e adicione a extensão .csv
        novo_nome = f"{table_name}.csv"
        
        caminho_destino = os.path.join(dir_destino, novo_nome)

        # --- 3. LÓGICA DE SOBRESCRITA ---
        # Se já existe um arquivo antigo com esse nome, removemos antes para evitar erros no move
        if os.path.exists(caminho_destino):
            try:
                os.remove(caminho_destino)
                logging.info(f"Arquivo anterior removido para substituição: {caminho_destino}")
            except Exception as e:
                logging.warning(f"Não foi possível remover o arquivo antigo ({e}). Tentando sobrescrever...")

        # --- 4. MOVER O ARQUIVO ---
        shutil.move(csv_file_path, caminho_destino)

        logging.info(f"Arquivo base atualizado com sucesso: {caminho_destino}")
    
    except Exception as e:
        logging.error(f"Falha ao arquivar/atualizar o CSV '{csv_file_path}': {e}")