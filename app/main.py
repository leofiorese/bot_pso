
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

# ===== IMPORTS (novas tabelas) =====
from actions.upsert_data.upsert_agrupamento import upsert_data as upsert_data_agrupamento
from actions.upsert_data.upsert_apontamentos import upsert_data as upsert_data_apontamentos
from actions.upsert_data.upsert_atividades import upsert_data as upsert_data_atividades
from actions.upsert_data.upsert_atribuicoes import upsert_data as upsert_data_atribuicoes
from actions.upsert_data.upsert_calendarios import upsert_data as upsert_data_calendarios
from actions.upsert_data.upsert_centros_de_resultado import upsert_data as upsert_data_centros_de_resultado
from actions.upsert_data.upsert_d_calend_proj import upsert_data as upsert_data_d_calend_proj
from actions.upsert_data.upsert_despesa_orcada import upsert_data as upsert_data_despesa_orcada
from actions.upsert_data.upsert_despesa_tipo import upsert_data as upsert_data_despesa_tipo
from actions.upsert_data.upsert_despesas import upsert_data as upsert_data_despesas
from actions.upsert_data.upsert_empresas import upsert_data as upsert_data_empresas
from actions.upsert_data.upsert_faturamento import upsert_data as upsert_data_faturamento
from actions.upsert_data.upsert_grref import upsert_data as upsert_data_grref
from actions.upsert_data.upsert_info_colabs import upsert_data as upsert_data_info_colabs
from actions.upsert_data.upsert_projetos import upsert_data as upsert_data_projetos
from actions.upsert_data.upsert_pso_taxa import upsert_data as upsert_data_pso_taxa
from actions.upsert_data.upsert_pso_usu_funcoes import upsert_data as upsert_data_pso_usu_funcoes
from actions.upsert_data.upsert_recursos import upsert_data as upsert_data_recursos
from actions.upsert_data.upsert_resumo_de_horas_ativ import upsert_data as upsert_data_resumo_de_horas_ativ
from actions.upsert_data.upsert_resumo_de_horas import upsert_data as upsert_data_resumo_de_horas
from actions.upsert_data.upsert_taxa_historico import upsert_data as upsert_data_taxa_historico

from sql_scripts.agrupamento_script import gerar_script_final as gerar_script_final_agrupamento
from sql_scripts.apontamentos_script import gerar_script_final as gerar_script_final_apontamentos
from sql_scripts.atividades_script import gerar_script_final as gerar_script_final_atividades
from sql_scripts.atribuicoes_script import gerar_script_final as gerar_script_final_atribuicoes
from sql_scripts.calendarios_script import gerar_script_final as gerar_script_final_calendarios
from sql_scripts.centros_de_resultado_script import gerar_script_final as gerar_script_final_centros_de_resultado
from sql_scripts.d_calend_proj_script import gerar_script_final as gerar_script_final_d_calend_proj
from sql_scripts.despesa_orcada_script import gerar_script_final as gerar_script_final_despesa_orcada
from sql_scripts.despesa_tipo_script import gerar_script_final as gerar_script_final_despesa_tipo
from sql_scripts.despesas_script import gerar_script_final as gerar_script_final_despesas
from sql_scripts.empresas_script import gerar_script_final as gerar_script_final_empresas
from sql_scripts.faturamento_script import gerar_script_final as gerar_script_final_faturamento
from sql_scripts.grref_script import gerar_script_final as gerar_script_final_grref
from sql_scripts.info_colabs_script import gerar_script_final as gerar_script_final_info_colabs
from sql_scripts.projetos_script import gerar_script_final as gerar_script_final_projetos
from sql_scripts.pso_taxa_script import gerar_script_final as gerar_script_final_pso_taxa
from sql_scripts.pso_usu_funcoes_script import gerar_script_final as gerar_script_final_pso_usu_funcoes
from sql_scripts.recursos_script import gerar_script_final as gerar_script_final_recursos
from sql_scripts.resumo_de_horas_ativ_script import gerar_script_final as gerar_script_final_resumo_de_horas_ativ
from sql_scripts.resumo_de_horas_script import gerar_script_final as gerar_script_final_resumo_de_horas
from sql_scripts.taxa_historico_script import gerar_script_final as gerar_script_final_taxa_historico
# ===================================

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

# ===== Despacho de scripts/upsserts e defaults =====
SCRIPT_GENERATORS = {
    "Orçado": gerar_script_final_orcado,
    "Planejado": gerar_script_final_planejado,
    "Realizado": gerar_script_final_realizado,
    "AGRUPAMENTO": gerar_script_final_agrupamento,
    "APONTAMENTOS": gerar_script_final_apontamentos,
    "ATIVIDADES": gerar_script_final_atividades,
    "ATRIBUICOES": gerar_script_final_atribuicoes,
    "CALENDARIOS": gerar_script_final_calendarios,
    "CENTROS_DE_RESULTADO": gerar_script_final_centros_de_resultado,
    "D_CALEND_PROJ": gerar_script_final_d_calend_proj,
    "DESPESA_ORCADA": gerar_script_final_despesa_orcada,
    "DESPESA_TIPO": gerar_script_final_despesa_tipo,
    "DESPESAS": gerar_script_final_despesas,
    "EMPRESAS": gerar_script_final_empresas,
    "FATURAMENTO": gerar_script_final_faturamento,
    "GRREF": gerar_script_final_grref,
    "INFO_COLABS": gerar_script_final_info_colabs,
    "PROJETOS": gerar_script_final_projetos,
    "PSO_TAXA": gerar_script_final_pso_taxa,
    "PSO_USU_FUNCOES": gerar_script_final_pso_usu_funcoes,
    "RECURSOS": gerar_script_final_recursos,
    "RESUMO_DE_HORAS_ATIV": gerar_script_final_resumo_de_horas_ativ,
    "RESUMO_DE_HORAS": gerar_script_final_resumo_de_horas,
    "TAXA_HISTORICO": gerar_script_final_taxa_historico,
}

UPSERT_HANDLERS = {
    "Orçado":      lambda df, csv: upsert_data_orcado(df, "RELATORIO_PSO_ORCADO", csv),
    "Planejado":   lambda df, csv: upsert_data_planejado(df, "RELATORIO_PSO_PLANEJADO", csv),
    "Realizado":   lambda df, csv: upsert_data_realizado(df, "RELATORIO_PSO_REALIZADO", csv),

    "AGRUPAMENTO":           lambda df, csv: upsert_data_agrupamento(df, "AGRUPAMENTO", csv),
    "APONTAMENTOS":          lambda df, csv: upsert_data_apontamentos(df, "APONTAMENTOS", csv),
    "ATIVIDADES":            lambda df, csv: upsert_data_atividades(df, "ATIVIDADES", csv),
    "ATRIBUICOES":           lambda df, csv: upsert_data_atribuicoes(df, "ATRIBUICOES", csv),
    "CALENDARIOS":           lambda df, csv: upsert_data_calendarios(df, "CALENDARIOS", csv),
    "CENTROS_DE_RESULTADO":  lambda df, csv: upsert_data_centros_de_resultado(df, "CENTROS_DE_RESULTADO", csv),
    "D_CALEND_PROJ":         lambda df, csv: upsert_data_d_calend_proj(df, "D_CALEND_PROJ", csv),
    "DESPESA_ORCADA":        lambda df, csv: upsert_data_despesa_orcada(df, "DESPESA_ORCADA", csv),
    "DESPESA_TIPO":         lambda df, csv: upsert_data_despesa_tipo(df, "DESPESA_TIPO", csv),
    "DESPESAS":              lambda df, csv: upsert_data_despesas(df, "DESPESAS", csv),
    "EMPRESAS":              lambda df, csv: upsert_data_empresas(df, "EMPRESAS", csv),
    "FATURAMENTO":           lambda df, csv: upsert_data_faturamento(df, "FATURAMENTO", csv),
    "GRREF":                 lambda df, csv: upsert_data_grref(df, "GRREF", csv),
    "INFO_COLABS":           lambda df, csv: upsert_data_info_colabs(df, "INFO_COLABS", csv),
    "PROJETOS":              lambda df, csv: upsert_data_projetos(df, "PROJETOS", csv),
    "PSO_TAXA":              lambda df, csv: upsert_data_pso_taxa(df, "PSO_TAXA", csv),
    "PSO_USU_FUNCOES":       lambda df, csv: upsert_data_pso_usu_funcoes(df, "PSO_USU_FUNCOES", csv),
    "RECURSOS":              lambda df, csv: upsert_data_recursos(df, "RECURSOS", csv),
    "RESUMO_DE_HORAS_ATIV":  lambda df, csv: upsert_data_resumo_de_horas_ativ(df, "RESUMO_DE_HORAS_ATIV", csv),
    "RESUMO_DE_HORAS":       lambda df, csv: upsert_data_resumo_de_horas(df, "RESUMO_DE_HORAS", csv),
    "TAXA_HISTORICO":        lambda df, csv: upsert_data_taxa_historico(df, "TAXA_HISTORICO", csv),
}

DEFAULTS_30 = {k: "-500" for k in SCRIPT_GENERATORS.keys()}

def get_dateadd_value(custom_date_response, days_value, script_choice):
    if custom_date_response == "sim" and days_value is not None:
        return f'-{days_value}'
    return DEFAULTS_30.get(script_choice, "-500")
# ================================================

def goto_report(page, dateadd_string, script_choice):
    logging.info("Indo para tela de relatório...")
    
    page.goto(REPORT_URL, timeout=60_000)
    page.wait_for_load_state("networkidle")
    page.wait_for_selector(SEL_TEXTAREA, state="visible", timeout=60_000)

    # despacho em 1 linha
    script_sql = SCRIPT_GENERATORS.get(script_choice, gerar_script_final_realizado)(dateadd_string)

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

        script_choices = [
            "Orçado", "Planejado", "Realizado",
            "AGRUPAMENTO","APONTAMENTOS","ATIVIDADES","ATRIBUICOES","CALENDARIOS",
            "CENTROS_DE_RESULTADO","D_CALEND_PROJ","DESPESA_ORCADA","DESPESA_TIPO","DESPESAS",
            "EMPRESAS","FATURAMENTO","GRREF","INFO_COLABS","PROJETOS","PSO_TAXA","PSO_USU_FUNCOES",
            "RECURSOS","RESUMO_DE_HORAS_ATIV","RESUMO_DE_HORAS","TAXA_HISTORICO",
        ]

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

                            # despacho upsert em 1 linha
                            UPSERT_HANDLERS[config_default_script.script_choice_default](df, csv_file_path)

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

                        # despacho upsert em 1 linha
                        UPSERT_HANDLERS[config_default_script.script_choice_default](df, csv_file_path)

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
