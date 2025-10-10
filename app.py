# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 7.1
# Дата: 2025-10-10
# Описание: 
# - Обновлен промпт для AI-Аналитика, чтобы он использовал SAFE_CAST 
#   для числовых колонок (mytna_vartist_hrn, vaha_netto_kg) 
#   перед их суммированием. Это исправляет ошибку 400.
# ===============================================

import os
import streamlit as st
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ArrayQueryParameter, ScalarQueryParameter
import pandas as pd
import google.generativeai as genai
import json
from datetime import datetime

# --- КОНФИГУРАЦИЯ СТРАНИЦЫ ---
st.set_page_config(page_title="Аналітика Митних Даних", layout="wide")

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    # ... (код без изменений) ...
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
    # ... (код без изменений) ...
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
    
    # ИЗМЕНЕНИЕ ЗДЕСЬ: Добавлена новая инструкция по использованию SAFE_CAST
    prompt = f"""
    You are an expert SQL analyst. Your task is to convert a user's analytical question into a single, executable Google BigQuery SQL query.

    DATABASE SCHEMA:
    The table is `{TABLE_ID}`. All columns are STRING type.
    Columns: data_deklaracii, napryamok, nazva_kompanii, kod_yedrpou, kraina_partner, kod_uktzed, opis_tovaru, mytna_vartist_hrn, vaha_netto_kg, vyd_transportu.
    All text is in Ukrainian. The user's question may be in Russian or Ukrainian.

    INSTRUCTIONS:
    1.  IMPORTANT: When calculating sums on `mytna_vartist_hrn` or `vaha_netto_kg`, you MUST cast them to a numeric type first using `SAFE_CAST(column AS FLOAT64)`. Example: `SUM(SAFE_CAST(mytna_vartist_hrn AS FLOAT64))`.
    2.  If the user asks for a list of companies (e.g., "importers"), you MUST use GROUP BY nazva_kompanii, kod_yedrpou.
    3.  Calculate aggregate metrics: `COUNT(*) as declaration_count`, `SUM(SAFE_CAST(mytna_vartist_hrn AS FLOAT64)) as total_value_hrn`, `SUM(SAFE_CAST(vaha_netto_kg AS FLOAT64)) as total_weight_kg`.
    4.  For semantic search on goods, create a broad `REGEXP_CONTAINS` pattern for the `opis_tovaru` column.
    5.  Filter by `napryamok` if the user specifies "importers" (`'Імпорт'`) or "exporters" (`'Експорт'`).
    6.  Sort the results (`ORDER BY`) by the most relevant metric, in descending order.
    7.  Limit the results to {max_items}.
    8.  Return ONLY a valid JSON object with a single key "sql_query" containing the full SQL string.
    
    USER'S QUESTION: "{user_question}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        response = model.generate_content(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        response_json = json.loads(response_text)
        return response_json.get("sql_query")
    except Exception as e:
        st.error(f"Помилка при генерації аналітичного SQL запиту: {e}")
        return None

# --- ЗАГРУЗКА СПИСКОВ ДЛЯ ФИЛЬТРОВ ---
# ... (код без изменений) ...
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

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
# ... (весь остальной код интерфейса и фильтров остается без изменений) ...
if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery."); st.stop()

filter_options = get_filter_options()
if 'selected_directions' not in st.session_state:
    st.session_state.selected_directions = []
    st.session_state.selected_countries = []
    st.session_state.selected_transports = []
    st.session_state.selected_years = filter_options['years']
    st.session_state.weight_from = 0
    st.session_state.weight_to = 0
    st.session_state.uktzed_input = ""
    st.session_state.yedrpou_input = ""
    st.session_state.company_input = ""

# --- РАЗДЕЛ: AI-АНАЛИТИК ---
st.header("🤖 AI-Аналитик: Задайте сложный вопрос")
ai_analytical_question = st.text_area( "Задайте ваш вопрос. Например: 'Найди топ-10 импортеров деталей для дронов по сумме'", key="ai_analytical_question")
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

# --- СЕКЦИЯ ФИЛЬТРОВ ---
st.header("📊 Ручные фильтры")
with st.expander("Панель Фильтров", expanded=True):
    # ... (код этой секции без изменений)
    def reset
