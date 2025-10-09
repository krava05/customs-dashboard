# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 3.0
# Дата: 2025-10-09
# Описание: 
# - Стабильная версия. Полностью переписана логика формирования 
#   SQL-запроса для гарантированного исправления всех 
#   синтаксических ошибок.
# ===============================================

import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.generativeai as genai
import json
from datetime import datetime

# --- КОНФИГУРАЦИЯ СТРАНИЦЫ ---
st.set_page_config(page_title="Аналітика Митних Даних", layout="wide")

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- ФУНКЦІЯ ПЕРЕВІРКИ ПАРОЛЮ ---
def check_password():
    def password_entered():
        if os.environ.get('K_SERVICE'):
            correct_password = os.environ.get("APP_PASSWORD")
        else:
            correct_password = st.secrets.get("APP_PASSWORD")
        if st.session_state.get("password") and st.session_state["password"] == correct_password:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if st.session_state.get("password_correct", False):
        return True
    st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("😕 Пароль невірний.")
    return False

# --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ GOOGLE ---
def initialize_clients():
    if 'clients_initialized' in st.session_state:
        return
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

# --- ФУНКЦІЯ ЗАВАНТАЖЕННЯ ДАНИХ ---
@st.cache_data(ttl=3600)
def run_query(query):
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- ФУНКЦИЯ "AI-АНАЛИТИК" ---
def get_analytical_ai_query(user_question, max_items=50):
    if not st.session_state.get('genai_ready', False):
        return None
    prompt = f"""You are an expert SQL analyst... USER'S QUESTION: "{user_question}" """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        response = model.generate_content(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        response_json = json.loads(response_text)
        return response_json.get("sql_query")
    except Exception as e:
        st.error(f"Помилка при генерації аналітичного SQL запиту: {e}")
        return None

# --- ФУНКЦІЯ "AI-ПОИСК" ---
def get_ai_search_query(user_query, max_items=100):
    if not st.session_state.get('genai_ready', False):
        return None
    prompt = f"""Based on the user's request, generate a SQL query... User request: "{user_query}" """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        response = model.generate_content(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        response_json = json.loads(response_text)
        return response_json.get("sql_query")
    except Exception as e:
        st.error(f"Помилка при генерації SQL за допомогою AI: {e}")
        return None

# --- ЗАГРУЗКА СПИСКОВ ДЛЯ ФИЛЬТРОВ ---
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
if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery.")
    st.stop()

# --- РАЗДЕЛ: AI-АНАЛИТИК ---
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

# --- СЕКЦИЯ ФИЛЬТРОВ И ПОИСКА ---
st.header("📊 Фильтрация и ручной поиск данных")
filter_options = get_filter_options()
with st.expander("Панель Фильтров и Поиска", expanded=True):
    st.subheader("Простой AI-поиск по описи товара")
    ai_search_query_text = st.text_input("Опишіть товар...", key="ai_search_input")
    search_button_ai = st.button("Найти с помощью AI")
    if search_button_ai and ai_search_query_text:
        with st.spinner("✨ AI генерирует запрос..."):
            ai_sql = get_ai_search_query(ai_search_query_text)
            if ai_sql:
                st.code(ai_sql, language='sql')
                ai_results_df = run_query(ai_sql)
                st.success(f"Найдено {len(ai_results_df)} записів.")
                st.dataframe(ai_results_df)
            else:
                st.error("Не удалось сгенерировать SQL-запрос.")
    st.markdown("---")
    
    st.subheader("Ручные фильтры")
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_directions = st.multiselect("Напрямок:", options=filter_options['direction'])
    with col2:
        selected_countries = st.multiselect("Країна-партнер:", options=filter_options['countries'])
    with col3:
        selected_transports = st.multiselect("Вид транспорту:", options=filter_options['transport'])

    col4, col5 = st.columns([2,1])
    with col4:
        selected_years = st.multiselect("Роки:", options=filter_options['years'], default=filter_options['years'])
    with col5:
        st.write("Вага нетто, кг")
        weight_col1, weight_col2 = st.columns(2)
        weight_from = weight_col1.number_input("Від", min_value=0, step=100, key="weight_from")
        weight_to = weight_col2.number_input("До", min_value=0, step=100, key="weight_to")

    col6, col7, col8 = st.columns(3)
    with col6:
        uktzed_input = st.text_input("Код УКТЗЕД (через кому):")
    with col7:
        yedrpou_input = st.text_input("Код ЄДРПОУ (через кому):")
    with col8:
        company_input = st.text_input("Назва компанії (через кому):")
    
    search_button_filters = st.button("🔍 Знайти за фильтрами", use_container_width=True)

# --- ЛОГИКА ФИЛЬТРОВ ---
if search_button_filters:
    query_parts = []
    
    def process_text_input(input_str):
        return [item.strip() for item in input_str.split(',') if item.strip()]

    # ========================================================================
    # <<< ОКОНЧАТЕЛЬНОЕ ИСПРАВЛЕНИЕ СИНТАКСИСА ЗДЕСЬ >>>
    # Используется более простой и надежный способ для обработки кавычек
    # ========================================================================

    if selected_directions:
        sanitized_list = []
        for d in selected_directions:
            sanitized_list.append(f"'{d.replace('\'', '\'\'')}'")
        query_parts.append(f"napryamok IN ({', '.join(sanitized_list)})")
    
    if selected_countries:
        sanitized_list = []
        for c in selected_countries:
            sanitized_list.append(f"'{c.replace('\'', '\'\'')}'")
        query_parts.append(f"kraina_partner IN ({', '.join(sanitized_list)})")

    if selected_transports:
        sanitized_list = []
        for t in selected_transports:
            sanitized_list.append(f"'{t.replace('\'', '\'\'')}'")
        query_parts.append(f"vyd_transportu IN ({', '.join(sanitized_list)})")

    if selected_years:
        years_str = ', '.join(map(str, selected_years))
        query_parts.append(f"EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) IN ({years_str})")

    if weight_from > 0:
        query_parts.append(f"SAFE_CAST(vaha_netto_kg AS FLOAT64) >= {weight_from}")
    if weight_to > 0 and weight_to >= weight_from:
        query_parts.append(f"SAFE_CAST(vaha_netto_kg AS FLOAT64) <= {weight_to}")

    uktzed_list = process_text_input(uktzed_input)
    if uktzed_list:
        conditions = []
        for item in uktzed_list:
            conditions.append(f"kod_uktzed LIKE '{item.replace('\'', '\'\'')}%'")
        query_parts.append(f"({' OR '.join(conditions)})")

    yedrpou_list = process_text_input(yedrpou_input)
    if yedrpou_list:
        sanitized_list = []
        for item in yedrpou_list:
            sanitized_list.append(f"'{item.replace('\'', '\'\'')}'")
        query_parts.append(f"kod_yedrpou IN ({', '.join(sanitized_list)})")

    company_list = process_text_input(company_input)
    if company_list:
        conditions = []
        for item in company_list:
            conditions.append(f"UPPER(nazva_kompanii) LIKE '%{item.replace('\'', '\'\'').upper()}%'")
        query_parts.append(f"({' OR '.join(conditions)})")

    if not query_parts:
        st.warning("Будь ласка, оберіть хоча б один фільтр.")
    else:
        where_clause = " AND ".join(query_parts)
        final_query = f"SELECT * FROM `{TABLE_ID}` WHERE {where_clause} LIMIT 1000"
        st.code(final_query, language='sql')
        with st.spinner("Виконується запит..."):
            results_df = run_query(final_query)
            st.success(f"Знайдено {len(results_df)} записів.")
            st.dataframe(results_df)
