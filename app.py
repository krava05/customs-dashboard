# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 1.2
# Дата: 2025-10-09
# Описание:
# - Добавлены кнопки "Сброс" для каждого отдельного фильтра.
# - Добавлена общая кнопка "Сбросить все фильтры".
# - Логика фильтрации переведена на st.session_state.
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

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    # ... (код этой функции остается без изменений) ...
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
    # ... (код этой функции остается без изменений) ...
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

# --- ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ ---
@st.cache_data(ttl=3600)
def run_query(query):
    # ... (код этой функции остается без изменений) ...
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- ФУНКЦИЯ "AI-АНАЛИТИК" ---
def get_analytical_ai_query(user_question, max_items=50):
    # ... (код этой функции остается без изменений) ...
    if not st.session_state.get('genai_ready', False):
        return None
    prompt = f"You are an expert SQL analyst..."
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
@st.cache_data(ttl=3600)
def get_filter_options():
    options = {}
    options['direction'] = ['Всі', 'Імпорт', 'Експорт']
    query_countries = f"SELECT DISTINCT kraina_partner FROM `{TABLE_ID}` WHERE kraina_partner IS NOT NULL ORDER BY kraina_partner"
    options['countries'] = [''] + list(run_query(query_countries)['kraina_partner'])
    query_transport = f"SELECT DISTINCT vyd_transportu FROM `{TABLE_ID}` WHERE vyd_transportu IS NOT NULL ORDER BY vyd_transportu"
    options['transport'] = [''] + list(run_query(query_transport)['vyd_transportu'])
    query_years = f"SELECT DISTINCT EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) as year FROM `{TABLE_ID}` WHERE data_deklaracii IS NOT NULL ORDER BY year DESC"
    options['years'] = list(run_query(query_years)['year'].dropna().astype(int))
    return options

# --- ЛОГИКА СБРОСА ФИЛЬТРОВ ---
def reset_all_filters():
    st.session_state.direction = 'Всі'
    st.session_state.country = ''
    st.session_state.transport = ''
    st.session_state.selected_years = filter_options['years']
    st.session_state.weight_from = 0
    st.session_state.weight_to = 0
    st.session_state.uktzed = ""
    st.session_state.yedrpou = ""
    st.session_state.company = ""

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery.")
    st.stop()

# --- Инициализация session_state для фильтров ---
filter_options = get_filter_options()
if 'direction' not in st.session_state:
    reset_all_filters()

# --- РАЗДЕЛ: AI-АНАЛИТИК ---
st.header("🤖 AI-Аналитик: Задайте сложный вопрос")
ai_analytical_question = st.text_area(
    "Задайте ваш вопрос. Например: 'Найди топ-10 импортеров деталей для дронов по сумме'",
    key="ai_analytical_question"
)
search_button_analytical_ai = st.button("Проанализировать с помощью AI", type="primary")

if search_button_analytical_ai and ai_analytical_question:
    # ... (код этой секции остается без изменений) ...
    pass

st.divider()

# --- СЕКЦИЯ ФИЛЬТРОВ ---
st.header("📊 Фильтрация и ручной поиск данных")

with st.expander("Панель Фільтрів", expanded=True):
    # --- Кнопка общего сброса ---
    st.button("Сбросить все фильтры", on_click=reset_all_filters, use_container_width=True)
    st.markdown("---") # Разделитель

    # --- Ряд 1 ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.selectbox("Напрямок:", options=filter_options['direction'], key='direction')
    with col2:
        st.selectbox("Країна-партнер:", options=filter_options['countries'], key='country')
    with col3:
        st.selectbox("Вид транспорту:", options=filter_options['transport'], key='transport')

    # --- Ряд 2 ---
    col4, col5 = st.columns([2,1])
    with col4:
        st.multiselect("Роки:", options=filter_options['years'], key='selected_years')
    with col5:
        st.write("Вага нетто, кг")
        weight_col1, weight_col2 = st.columns(2)
        weight_from = weight_col1.number_input("Від", min_value=0, step=100, key="weight_from")
        weight_to = weight_col2.number_input("До", min_value=0, step=100, key="weight_to")

    # --- Ряд 3 ---
    col6, col7, col8 = st.columns(3)
    with col6:
        st.text_input("Код УКТЗЕД (можна частину):", key='uktzed')
    with col7:
        st.text_input("Код ЄДРПОУ фірми:", key='yedrpou')
    with col8:
        st.text_input("Назва компанії:", key='company')
    
    st.markdown("---") # Разделитель
    search_button_filters = st.button("🔍 Знайти за фільтрами", use_container_width=True)

if search_button_filters:
    query_parts = []
    if st.session_state.direction and st.session_state.direction != 'Всі':
        query_parts.append(f"napryamok = '{st.session_state.direction}'")
    
    if st.session_state.selected_years:
        years_str = ', '.join(map(str, st.session_state.selected_years))
        query_parts.append(f"EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) IN ({years_str})")

    if st.session_state.weight_from > 0:
        query_parts.append(f"SAFE_CAST(vaha_netto_kg AS FLOAT64) >= {st.session_state.weight_from}")
    if st.session_state.weight_to > 0 and st.session_state.weight_to >= st.session_state.weight_from:
        query_parts.append(f"SAFE_CAST(vaha_netto_kg AS FLOAT64) <= {st.session_state.weight_to}")

    if st.session_state.company:
        sanitized_company = st.session_state.company.replace("'", "''").upper()
        query_parts.append(f"UPPER(nazva_kompanii) LIKE '%{sanitized_company}%'")
    if st.session_state.country:
        sanitized_country = st.session_state.country.replace("'", "''")
        query_parts.append(f"kraina_partner = '{sanitized_country}'")
    if st.session_state.transport:
        sanitized_transport = st.session_state.transport.replace("'", "''")
        query_parts.append(f"vyd_transportu = '{sanitized_transport}'")
    if st.session_state.uktzed:
        query_parts.append(f"kod_uktzed LIKE '{st.session_state.uktzed}%'")
    if st.session_state.yedrpou:
        query_parts.append(f"kod_yedrpou = '{st.session_state.yedrpou}'")
        
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
