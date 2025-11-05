# main.py
import os
import logging
import re
from db.db import get_conn
from actions.process_csv.process_csv import process_csv
from actions.upsert_data.upsert_realizado_data import upsert_data as upsert_data_realizado
from actions.upsert_data.upsert_orcado_data import upsert_data as upsert_data_orcado
from actions.upsert_data.upsert_planejado_data import upsert_data as upsert_data_planejado
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from dotenv import load_dotenv
from sql_scripts.realizado_script import gerar_script_final as gerar_script_final_realizado
from sql_scripts.orcado_script import gerar_script_final as gerar_script_final_orcado
from sql_scripts.planejado_script import gerar_script_final as gerar_script_final_planejado
from pathlib import Path
import time
import sys
import config_default_script as config_default_script

def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
BASE_PATH = get_base_path()

#os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(BASE_PATH, "playwright-browsers")

load_dotenv(os.path.join(BASE_PATH, '.env'))

LOGIN_URL  = os.getenv("PSO_LOGIN_URL")
REPORT_URL = os.getenv("PSO_REPORT_URL")
USERNAME   = os.getenv("PSO_USERNAME")
PASSWORD   = os.getenv("PSO_PASSWORD")
HEADLESS   = os.getenv("HEADLESS", "True").lower() == "true"

DOWNLOAD_DIR = Path(os.path.join(BASE_PATH, "app", "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3
LOGFILE = os.path.join(BASE_PATH, "pso_bot.log")

SEL_COOKIE_OK      = "text=OK, entendi."
SEL_LOGIN_INPUT    = "input[placeholder='Login']"
SEL_PASSWORD_INPUT = "input[placeholder='Senha']"
SEL_SUBMIT_BTN     = "input[type='submit']"
SEL_TEXTAREA       = "textarea[name='QUERY']"
SEL_TESTAR_EXCEL   = "div#tit_buttons input[type='submit'][name='button_Testar2'][value='Testar (EXCEL)']"

logging.basicConfig(level=logging.INFO, filename=LOGFILE,
                    format="%(asctime)s %(levelname)s %(message)s")

def _sanitize(name: str) -> str:
    return re.sub(r"[^\w\-.]", "_", name)

def do_login(page):
    logging.info("Abrindo tela de login...")
    page.goto(LOGIN_URL, timeout=60_000)

    try:
        page.locator(SEL_COOKIE_OK).click(timeout=3_000)
    except PWTimeoutError:
        pass

    page.locator(SEL_LOGIN_INPUT).fill(USERNAME)
    page.locator(SEL_PASSWORD_INPUT).fill(PASSWORD)

    page.wait_for_selector(SEL_SUBMIT_BTN, state="visible", timeout=30_000)
    try:
        page.locator(SEL_SUBMIT_BTN).click()
    except PWTimeoutError:
        logging.error("Timeout ao clicar no botão 'Entrar'.")
        raise

    page.wait_for_load_state("networkidle")
    try:
        page.wait_for_selector("text=Release Notes", timeout=15_000)
    except PWTimeoutError:
        logging.warning("Não confirmou elemento pós-login; prosseguindo assim mesmo.")

def get_dateadd_value(custom_date_response, days_value, script_choice):
    if custom_date_response == "sim" and days_value is not None:
        return f'-{days_value}'
    
    defaults = {
        "Orçado": "-30",
        "Realizado": "-30",
        "Planejado": "-30"
    }  
    
    return defaults.get(script_choice, "-30")

def goto_report(page, dateadd_string, script_choice):
    logging.info("Indo para tela de relatório...")
    
    page.goto(REPORT_URL, timeout=60_000)
    page.wait_for_load_state("networkidle")
    page.wait_for_selector(SEL_TEXTAREA, state="visible", timeout=60_000)

    if script_choice == "Orçado":
        script_sql = gerar_script_final_orcado(dateadd_string)
    
    elif script_choice == "Planejado":
        script_sql = gerar_script_final_planejado(dateadd_string)
    
    else: 
        script_sql = gerar_script_final_realizado(dateadd_string)

    logging.info("Preenchendo text area com o conteúdo do script SQL...")
    page.locator(SEL_TEXTAREA).fill(script_sql)

    page.wait_for_selector(SEL_TESTAR_EXCEL, state="visible", timeout=60_000)
    if not page.locator(SEL_TESTAR_EXCEL).is_visible() or not page.locator(SEL_TESTAR_EXCEL).is_enabled():
        logging.error("Botão 'Testar (EXCEL)' não está visível ou habilitado.")
        raise RuntimeError("Botão 'Testar (EXCEL)' não está visível ou habilitado.")

    with page.expect_download(timeout=120_000) as d_info:
        page.locator(SEL_TESTAR_EXCEL).click()

    d = d_info.value
    suggested = d.suggested_filename or "relatorio.xlsx"
    target = DOWNLOAD_DIR / f"{time.strftime('%Y%m%d_%H%M%S')}_{_sanitize(suggested)}"
    d.save_as(str(target))
    logging.info(f"Download salvo: {target}")

    return target

def run_once(custom_date_response, days_value, script_choice, user_choice):
    
    if user_choice == 0:
        logging.info("Iniciando processo em modo AUTOMÁTICO...")

        script_choices = ["Orçado", "Planejado", "Realizado"]

        last = None

        for script_choice in script_choices:
            config_default_script.script_choice_default = script_choice

            for i in range(1, MAX_RETRIES + 1):
                try:
                    logging.info(f"Iniciando consulta para: {config_default_script.script_choice_default}")
                
                    dateadd_string = get_dateadd_value(custom_date_response, days_value, config_default_script.script_choice_default)

                    with sync_playwright() as p:
                        browser = p.firefox.launch(headless=HEADLESS)
                        context = browser.new_context(accept_downloads=True)
                        page = context.new_page()

                        try:
                            do_login(page)
                            csv_file_path = goto_report(page, dateadd_string, config_default_script.script_choice_default)

                            df = process_csv(csv_file_path, config_default_script.script_choice_default)

                            if config_default_script.script_choice_default == "Orçado":
                                upsert_data_orcado(df, "RELATORIO_PSO_ORCADO", csv_file_path)

                            elif config_default_script.script_choice_default == "Planejado":
                                upsert_data_planejado(df, "RELATORIO_PSO_PLANEJADO", csv_file_path)

                            else:
                                upsert_data_realizado(df, "RELATORIO_PSO_REALIZADO", csv_file_path)

                        finally:
                            context.close()
                            browser.close()
                            logging.info("Navegador fechado.")
                            logging.info(f"Consulta para {config_default_script.script_choice_default} concluída com sucesso.")
                            logging.info("-" * 50)
                            logging.info("Aguardando 5 segundos antes da próxima consulta...")
                            time.sleep(5)
                            logging.info("-" * 50)

                    break 
                
                except Exception as e:
                    last = e
                    logging.exception(f"Tentativa {i} para {config_default_script.script_choice_default} falhou.")
                    time.sleep(4 * i)

    if user_choice == 1:
        logging.info("Iniciando processo em modo MANUAL...")

        for i in range(1, MAX_RETRIES + 1):
                try:
                    logging.info(f"Iniciando consulta para: {config_default_script.script_choice_default}")
                
                    dateadd_string = get_dateadd_value(custom_date_response, days_value, config_default_script.script_choice_default)

                    with sync_playwright() as p:
                        browser = p.firefox.launch(headless=HEADLESS)
                        context = browser.new_context(accept_downloads=True)
                        page = context.new_page()

                        try:
                            do_login(page)
                            csv_file_path = goto_report(page, dateadd_string, config_default_script.script_choice_default)

                            df = process_csv(csv_file_path, config_default_script.script_choice_default)

                            if config_default_script.script_choice_default == "Orçado":
                                upsert_data_orcado(df, "RELATORIO_PSO_ORCADO", csv_file_path)

                            elif config_default_script.script_choice_default == "Planejado":
                                upsert_data_planejado(df, "RELATORIO_PSO_PLANEJADO", csv_file_path)

                            else:
                                upsert_data_realizado(df, "RELATORIO_PSO_REALIZADO", csv_file_path)

                        finally:
                            context.close()
                            browser.close()
                            logging.info("Navegador fechado.")
                            logging.info(f"Consulta para {config_default_script.script_choice_default} concluída com sucesso.")
                            logging.info("-" * 50)
                            logging.info("Aguardando 5 segundos antes da próxima consulta...")
                            time.sleep(5)
                            logging.info("-" * 50)

                    break 
                
                except Exception as e:
                    last = e
                    logging.exception(f"Tentativa {i} para {config_default_script.script_choice_default} falhou.")
                    time.sleep(4 * i)
