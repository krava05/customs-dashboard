# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 7.6
# Дата: 2025-10-10
# Описание: 
# - Добавлен недостающий импорт 're', что исправляет ошибку 
#   're is not defined' при обработке ответа AI.
# ===============================================

import os
import streamlit as st
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ArrayQueryParameter, ScalarQueryParameter
import pandas as pd
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import json
from datetime import datetime
import re # <<< ВОТ ЭТО ИСПРАВЛЕНИЕ

# --- КОНФИГУРАЦИЯ СТРАНИЦЫ ---
st.set_page_config(page_title="Аналітика Митних Даних", layout="wide")

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    def password_entered():
        if os.environ.get('K_SERVICE'): correct_password = os.environ.get("APP_PASSWORD")
        else: correct_password = st.secrets.get("APP_PASSWORD")
        if st.session_state.get("password") and st.session_state["password"] == correct_password:
            st.session_state["password_correct"] = True; del st.session_state["password"]
        else: st.session_state["password_correct"] = False
    if st.session_state.get("password_correct", False): return True
    st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
    if "password_correct" in st.session_state and not st.session_state["password_correct"]: st.error("😕 Пароль невірний.")
    return False

# --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ GOOGLE ---
def initialize_clients():
    if 'clients_initialized' in st.session_state: return
    try:
        if os.environ.get('K_SERVICE'):
            st.session_state.bq_client = bigquery.Client(project=PROJECT_ID)
            api_key = os.environ.get("GOOGLE_AI_API_KEY")
        else:
            st.session_state.bq_client = bigquery.Client()
            api_key = st.secrets.get("GOOGLE_AI_API_KEY")
        if api_key:
            genai.configure(api_key=api_key)
            st.session_state.genai_ready = True
        st.session_state.clients_initialized = True
        st.session_state.client_ready = True
    except Exception as e:
        st.error(f"Помилка аутентифікації в Google: {e}")
        st.session_state.client_ready = False

# --- ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ ---
@st.cache_data(ttl=3600)
def run_query(query, job_config=None):
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- ФУНКЦИЯ "AI-АНАЛИТИК" ---
def get_analytical_ai_query(user_question, max_items=50):
    if not st.session_state.get('genai_ready', False):
        st.warning("AI-сервис не готов.")
        return None
    
    prompt = f"""
    You are a SQL generation machine. Your ONLY task is to convert a user's question into a Google BigQuery SQL query and return it in a JSON format.

    DATABASE SCHEMA:
    - Table: `{TABLE_ID}`
    - Columns are all STRING type: data_deklaracii, napryamok, nazva_kompanii, kod_yedrpou, kraina_partner, kod_uktzed, opis_tovaru, mytna_vartist_hrn, vaha_netto_kg, vyd_transportu.
    - Data language is Ukrainian.

    CRITICAL INSTRUCTIONS:
    1.  **OUTPUT FORMAT**: Your entire response MUST be a single, valid JSON object with one key: "sql_query". Do NOT add any introductory text, explanations, or markdown.
    2.  **CASTING**: When using SUM() on `mytna_vartist_hrn` or `vaha_netto_kg`, you MUST cast them using `SAFE_CAST(column AS FLOAT64)`.
    3.  **AGGREGATION**: If the user asks for a list of companies/importers/exporters, you MUST `GROUP BY nazva_kompanii, kod_yedrpou` and calculate aggregates like `COUNT(*) as declaration_count` and `SUM(SAFE_CAST(...))`.
    4.  **SEMANTIC SEARCH**: For searching goods in `opis_tovaru` (e.g., "drone parts"), use a broad `REGEXP_CONTAINS` pattern with `(?i)` flag. For "drone parts," search for 'дрон|квадрокоптер|бпла|безпілотник|пропелер'.
    5.  **SORTING**: `ORDER BY` the most relevant aggregate metric in `DESC` order.
    6.  **LIMIT**: `LIMIT` the results to {max_items}.

    VALID JSON RESPONSE EXAMPLE:
    {{
      "sql_query": "SELECT nazva_kompanii, COUNT(*) as declaration_count FROM `{TABLE_ID}` WHERE REGEXP_CONTAINS(opis_tovaru, '(?i)дрон') GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
    }}

    USER'S QUESTION: "{user_question}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        response = model.generate_content(prompt, safety_settings=safety_settings)
        
        response_text = response.text.strip()
        match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if not match:
            st.error(f"AI-модель вернула ответ без JSON. Ответ модели: '{response_text}'")
            return None
        
        json_text = match.group(0)
        response_json = json.loads(json_text)
        return response_json.get("sql_query")
    except Exception as e:
        raw_response = "Ответ от AI был пустым или некорректным."
        if 'response' in locals() and hasattr(response, 'text'):
            raw_response = response.text
        st.error(f"Помилка при обработке ответа AI: {e}. Ответ модели: '{raw_response}'")
        return None

# --- ЗАГРУЗКА СПИСКОВ ДЛЯ ФИЛЬТРОВ ---
# ... (остальной код до конца без изменений) ...
@st.cache_data(ttl=3600)
def get_filter_options():
    options = {}
    options['direction'] = ['Імпорт', 'Експорт']
    query_countries = f"SELECT DISTINCT kraina_partner FROM `{TABLE_ID}` WHERE kraina_partner IS NOT NULL ORDER BY kraina_partner"
    options['countries'] = list(run_query(query_countries)['kraina_partner'])
    query_transport = f"SELECT DISTINCT vyd_transportu FROM `{TABLE_ID}` WHERE vyd_transportu IS NOT NULL ORDER BY vyd_transportu"
    options['transport'] = list(run_query(query_transport)['vyd_transportu'])
    query_years = f"SELECT DISTINCT EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) as year FROM `{TABLE_ID}` WHERE data_deklaracii IS NOT NULL ORDER BY year DESC"
    options['years'] = list(run_query(query_years)['year'].dropna().astype(int))
    return options

def reset_all_filters():
    st.session_state.selected_directions = []
    st.session_state.selected_countries = []
    st.session_state.selected_transports = []
    st.session_state.selected_years = filter_options['years']
    st.session_state.weight_from = 0
    st.session_state.weight_to = 0
    st.session_state.uktzed_input = ""
    st.session_state.yedrpou_input = ""
    st.session_state.company_input = ""

if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery."); st.stop()

filter_options = get_filter_options()
if 'selected_directions' not in st.session_state:
    reset_all_filters()

st.header("🤖 AI-Аналитик: Задайте сложный вопрос")
ai_analytical_question = st.text_area( "Задайте ваш вопрос...", key="ai_analytical_question")
search_button_analytical_ai = st.button("Проанализировать с помощью AI", type="primary")
if search_button_analytical_ai and ai_analytical_question:
    with st.spinner("✨ AI-аналитик думает..."):
        analytical_sql = get_analytical_ai_query(ai_analytical_question)
        if analytical_sql:
            st.subheader("Сгенерированный SQL-запрос:")
            st.code(analytical_sql, language='sql')
            with st.spinner("Выполняется сложный запрос..."):
                analytical_results_df = run_query(analytical_sql)
                st.subheader("Результат анализа:")
                st.success(f"Анализ завершен. Найдено {len(analytical_results_df)} записей.")
                st.dataframe(analytical_results_df)
        else:
            st.error("Не удалось сгенерировать аналитический SQL-запрос.")

st.divider()

st.header("📊 Ручные фильтры")
with st.expander("Панель Фильтров", expanded=True):
    st.button("Сбросить все фильтры", on_click=reset_all_filters, use_container_width=True, type="secondary")
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1: st.multiselect("Напрямок:", options=filter_options['direction'], key='selected_directions')
    with col2: st.multiselect("Країна-партнер:", options=filter_options['countries'], key='selected_countries')
    with col3: st.multiselect("Вид транспорту:", options=filter_options['transport'], key='selected_transports')
    col4, col5 = st.columns([2,1])
    with col4: st.multiselect("Роки:", options=filter_options['years'], key='selected_years')
    with col5:
        st.write("Вага нетто, кг")
        weight_col1, weight_col2 = st.columns(2)
        weight_from = weight_col1.number_input("Від", min_value=0, step=100, key="weight_from")
        weight_to = weight_col2.number_input("До", min_value=0, step=100, key="weight_to")
    col6, col7, col8 = st.columns(3)
    with col6: st.text_input("Код УКТЗЕД (через кому):", key='uktzed_input')
    with col7: st.text_input("Код ЄДРПОУ (через кому):", key='yedrpou_input')
    with col8: st.text_input("Назва компанії (через кому):", key='company_input')
    search_button_filters = st.button("🔍 Знайти за фильтрами", use_container_width=True, type="primary")

if search_button_filters:
    query_parts = []; query_params = []
    def process_text_input(input_str): return [item.strip() for item in input_str.split(',') if item.strip()]
    if st.session_state.selected_directions:
        query_parts.append("napryamok IN UNNEST(@directions)")
        query_params.append(ArrayQueryParameter("directions", "STRING", st.session_state.selected_directions))
    if st.session_state.selected_countries:
        query_parts.append("kraina_partner IN UNNEST(@countries)")
        query_params.append(ArrayQueryParameter("countries", "STRING", st.session_state.selected_countries))
    if st.session_state.selected_transports:
        query_parts.append("vyd_transportu IN UNNEST(@transports)")
        query_params.append(ArrayQueryParameter("transports", "STRING", st.session_state.selected_transports))
    if st.session_state.selected_years:
        query_parts.append("EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) IN UNNEST(@years)")
        query_params.append(ArrayQueryParameter("years", "INT64", st.session_state.selected_years))
    if st.session_state.weight_from > 0:
        query_parts.append("SAFE_CAST(vaha_netto_kg AS FLOAT64) >= @weight_from")
        query_params.append(ScalarQueryParameter("weight_from", "FLOAT64", st.session_state.weight_from))
    if st.session_state.weight_to > 0 and st.session_state.weight_to >= st.session_state.weight_from:
        query_parts.append("SAFE_CAST(vaha_netto_kg AS FLOAT64) <= @weight_to")
        query_params.append(ScalarQueryParameter("weight_to", "FLOAT64", st.session_state.weight_to))
    uktzed_list = process_text_input(st.session_state.uktzed_input)
    if uktzed_list:
        conditions = []
        for i, item in enumerate(uktzed_list):
            param_name = f"uktzed{i}"
            conditions.append(f"kod_uktzed LIKE @{param_name}")
            query_params.append(ScalarQueryParameter(param_name, "STRING", f"{item}%"))
        query_parts.append(f"({' OR '.join(conditions)})")
    yedrpou_list = process_text_input(st.session_state.yedrpou_input)
    if yedrpou_list:
        query_parts.append("kod_yedrpou IN UNNEST(@yedrpou)")
        query_params.append(ArrayQueryParameter("yedrpou", "STRING", yedrpou_list))
    company_list = process_text_input(st.session_state.company_input)
    if company_list:
        conditions = []
        for i, item in enumerate(company_list):
            param_name = f"company{i}"
            conditions.append(f"UPPER(nazva_kompanii) LIKE @{param_name}")
            query_params.append(ScalarQueryParameter(param_name, "STRING", f"%{item.upper()}%"))
        query_parts.append(f"({' OR '.join(conditions)})")
    if not query_parts:
        st.warning("Будь ласка, оберіть хоча б один фільтр.")
    else:
        where_clause = " AND ".join(query_parts)
        final_query = f"SELECT * FROM `{TABLE_ID}` WHERE {where_clause} LIMIT 1000"
        job_config = QueryJobConfig(query_parameters=query_params)
        st.code(final_query, language='sql')
        with st.spinner("Виконується запит..."):
            results_df = run_query(final_query, job_config=job_config)
            st.success(f"Знайдено {len(results_df)} записів.")
            st.dataframe(results_df)
