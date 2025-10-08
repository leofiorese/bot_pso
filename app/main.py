# main.py
import os
import logging
import re
from app.db.db import get_conn
from app.actions.process_csv import process_csv
from app.actions.upsert_data import upsert_data
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from dotenv import load_dotenv
from pathlib import Path
import time
import importlib

load_dotenv()

LOGIN_URL  = os.getenv("PSO_LOGIN_URL")
REPORT_URL = os.getenv("PSO_REPORT_URL")
USERNAME   = os.getenv("PSO_USERNAME")
PASSWORD   = os.getenv("PSO_PASSWORD")
HEADLESS   = os.getenv("HEADLESS", "True").lower() == "true"

# Configuração de download
DOWNLOAD_DIR = Path("./app/downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3
LOGFILE = "pso_bot.log"

# Seletores
SEL_COOKIE_OK      = "text=OK, entendi."
SEL_LOGIN_INPUT    = "input[placeholder='Login']"
SEL_PASSWORD_INPUT = "input[placeholder='Senha']"
SEL_SUBMIT_BTN     = "input[type='submit']"
SEL_TEXTAREA       = "textarea[name='QUERY']"  # Atualize o seletor do campo de texto
SEL_TESTAR_EXCEL   = "div#tit_buttons input[type='submit'][name='button_Testar2'][value='Testar (EXCEL)']"

logging.basicConfig(level=logging.INFO, filename=LOGFILE,
                    format="%(asctime)s %(levelname)s %(message)s")

def _sanitize(name: str) -> str:
    return re.sub(r"[^\w\-.]", "_", name)

def load_script():
    """
    Carrega o conteúdo do arquivo 'rel_script.py' e retorna o valor da variável script_sql.
    """
    script_path = Path('app/rel_script.py')

    if script_path.exists():
        # Carregar o módulo do arquivo rel_script.py
        spec = importlib.util.spec_from_file_location("rel_script", script_path)
        rel_script = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rel_script)
        return rel_script.script_sql_modified
    else:
        logging.error("Arquivo 'rel_script.py' não encontrado!")
        raise FileNotFoundError(f"O arquivo 'rel_script.py' não foi encontrado em {script_path}")

def do_login(page):
    logging.info("Abrindo tela de login...")
    page.goto(LOGIN_URL, timeout=60_000)

    try:
        page.locator(SEL_COOKIE_OK).click(timeout=3_000)
    except PWTimeoutError:
        pass

    page.locator(SEL_LOGIN_INPUT).fill(USERNAME)
    page.locator(SEL_PASSWORD_INPUT).fill(PASSWORD)

    logging.info("Esperando botão 'Entrar' ficar disponível...")
    page.wait_for_selector(SEL_SUBMIT_BTN, state="visible", timeout=30_000)

    try:
        logging.info("Clicando no botão 'Entrar'...")
        page.locator(SEL_SUBMIT_BTN).click()
    except PWTimeoutError:
        logging.error("Timeout ao clicar no botão 'Entrar'.")
        raise

    logging.info("Aguardando tela inicial após login...")
    page.wait_for_load_state("networkidle")

    try:
        page.wait_for_selector("text=Release Notes", timeout=15_000)
    except PWTimeoutError:
        logging.warning("Não confirmou elemento pós-login; prosseguindo assim mesmo.")


def get_dateadd_value():
    """
    Pergunta ao usuário se deseja usar uma data personalizada.
    """
    while True:
        custom_date_input = input("Deseja usar uma data personalizada? (sim/não): ").strip().lower()
        if custom_date_input == "não":
            return '-1'  # Valor padrão se não for personalizado
        elif custom_date_input == "sim":
            try:
                # Pergunta para o usuário qual valor de data ele deseja
                days_input = input("Informe o número de dias (valor positivo, o sistema aplicará o negativo): ").strip()
                days_input = int(days_input)  # Verifica se é um número válido
                return f'-{days_input}'  # Converte para negativo para o SQL
            except ValueError:
                print("Por favor, insira um número válido para o número de dias.")
        else:
            print("Resposta inválida. Responda com 'sim' ou 'não'.")

def goto_report(page):
    logging.info("Indo para tela de relatório...")
    page.goto(REPORT_URL, timeout=60_000)
    page.wait_for_load_state("networkidle")

    logging.info("Esperando textarea ficar disponível...")
    page.wait_for_selector(SEL_TEXTAREA, state="visible", timeout=60_000)

    # Carregar o script_sql do arquivo rel_script.py
    script_sql = load_script()

    # Preenche a área de texto com o conteúdo do script_sql
    logging.info("Preenchendo a área de texto com o conteúdo do script SQL...")
    page.locator(SEL_TEXTAREA).fill(script_sql)

    # Espera o botão "Testar (EXCEL)" ficar visível e clicável
    logging.info("Esperando botão 'Testar (EXCEL)' ficar disponível...")
    page.wait_for_selector(SEL_TESTAR_EXCEL, state="visible", timeout=60_000)

    if not page.locator(SEL_TESTAR_EXCEL).is_visible():
        logging.error("Botão 'Testar (EXCEL)' não está visível.")
        raise RuntimeError("Botão 'Testar (EXCEL)' não está visível.")

    if not page.locator(SEL_TESTAR_EXCEL).is_enabled():
        logging.error("Botão 'Testar (EXCEL)' não está habilitado.")
        raise RuntimeError("Botão 'Testar (EXCEL)' não está habilitado.")

    try:
        logging.info("Clicando no botão 'Testar (EXCEL)'...")
        page.locator(SEL_TESTAR_EXCEL).click()
    except PWTimeoutError:
        logging.error("Timeout ao clicar no botão 'Testar (EXCEL)'.")
        raise

    with page.expect_download(timeout=120_000) as d_info:
        page.locator(SEL_TESTAR_EXCEL).click()

    d = d_info.value
    suggested = d.suggested_filename or "relatorio.xlsx"
    target = DOWNLOAD_DIR / f"{time.strftime('%Y%m%d_%H%M%S')}_{_sanitize(suggested)}"
    d.save_as(str(target))

    logging.info(f"Download salvo: {target}")

    print(f"Download concluído: {target}")

    return target

def run_once():
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            do_login(page)
            csv_file_path = goto_report(page)

            df = process_csv(csv_file_path)
        
            upsert_data(df, "RELATORIO_PSO")
        
        finally:
            context.close(); browser.close()

def main():
    if not all([LOGIN_URL, REPORT_URL, USERNAME, PASSWORD]):
        raise SystemExit("Defina PSO_LOGIN_URL, PSO_REPORT_URL, PSO_USERNAME e PSO_PASSWORD no .env")

    last = None

    for i in range(1, MAX_RETRIES + 1):
        try:
            run_once()
            return
        except Exception as e:
            last = e
            logging.exception(f"Tentativa {i} falhou")
            time.sleep(4 * i)

    raise last

if __name__ == "__main__":
    main()
