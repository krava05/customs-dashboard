# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 8.0
# Дата: 2025-10-10
# Описание: 
# - Добавлен новый инструмент "AI-помощник по кодам УКТЗЕД".
#   Он по текстовому описанию товара предлагает список 
#   релевантных кодов для ручного поиска.
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
import re

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
    # ... (код без изменений) ...
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- ФУНКЦИЯ "AI-АНАЛИТИК" ---
def get_analytical_ai_query(user_question, max_items=50):
    # ... (код без изменений) ...
    return None

# --- НОВАЯ ФУНКЦИЯ "AI-ПОМОЩНИК ПО КОДАМ" ---
def get_ai_code_suggestions(product_description):
    if not st.session_state.get('genai_ready', False):
        st.warning("AI-сервис не готов.")
        return None
    
    prompt = f"""
    You are an expert in customs classification and HS codes (УКТЗЕД).
    Analyze the user's product description. Your goal is to suggest a list of the most relevant 4-digit, 6-digit, or 10-digit HS codes (`kod_uktzed`).

    CRITICAL INSTRUCTIONS:
    1.  **OUTPUT FORMAT**: Your entire response MUST be a single, valid JSON object with one key: "codes". The value should be an array of suggested code strings.
    2.  Do not add any explanations or introductory text.

    VALID JSON RESPONSE EXAMPLE:
    {{
      "codes": ["8507", "85076000", "8807"]
    }}

    USER'S PRODUCT DESCRIPTION: "{product_description}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        safety_settings = {
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
        return response_json.get("codes", [])
    except Exception as e:
        st.error(f"Помилка при получении кодов от AI: {e}")
        return None


# --- ЗАГРУЗКА СПИСКОВ ДЛЯ ФИЛЬТРОВ ---
@st.cache_data(ttl=3600)
def get_filter_options():
    # ... (код без изменений) ...
    options = {}
    options['direction'] = ['Імпорт', 'Експорт']
    query_countries = f"SELECT DISTINCT kraina_partner FROM `{TABLE_ID}` WHERE kraina_partner IS NOT NULL ORDER BY kraina_partner"
    options['countries'] = list(run_query(query_countries)['kraina_partner'])
    query_transport = f"SELECT DISTINCT vyd_transportu FROM `{TABLE_ID}` WHERE vyd_transportu IS NOT NULL ORDER BY vyd_transportu"
    options['transport'] = list(run_query(query_transport)['vyd_transportu'])
    query_years = f"SELECT DISTINCT EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) as year FROM `{TABLE_ID}` WHERE data_deklaracii IS NOT NULL ORDER BY year DESC"
    options['years'] = list(run_query(query_years)['year'].dropna().astype(int))
    return options

# --- ЛОГИКА СБРОСА ФИЛЬТРОВ ---
def reset_all_filters():
    # ... (код без изменений) ...
    st.session_state.selected_directions = []
    st.session_state.selected_countries = []
    st.session_state.selected_transports = []
    st.session_state.selected_years = filter_options['years']
    st.session_state.weight_from = 0
    st.session_state.weight_to = 0
    st.session_state.uktzed_input = ""
    st.session_state.yedrpou_input = ""
    st.session_state.company_input = ""


# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery."); st.stop()

filter_options = get_filter_options()
if 'selected_directions' not in st.session_state:
    reset_all_filters()

# --- РАЗДЕЛ: AI-АНАЛИТИК ---
st.header("🤖 AI-Аналитик: Задайте сложный вопрос")
ai_analytical_question = st.text_area( "Задайте ваш вопрос...", key="ai_analytical_question")
search_button_analytical_ai = st.button("Проанализировать с помощью AI", type="primary")
if search_button_analytical_ai and ai_analytical_question:
    # ... (код без изменений) ...
    pass

st.divider()

# --- СЕКЦИЯ ФИЛЬТРОВ ---
st.header("📊 Ручные фильтры")
with st.expander("Панель Фильтров", expanded=True):
    st.button("Сбросить все фильтры", on_click=reset_all_filters, use_container_width=True, type="secondary")
    st.markdown("---")

    # --- НОВЫЙ РАЗДЕЛ: AI-ПОМОЩНИК ПО КОДАМ ---
    st.subheader("🤖 AI-помощник по кодам УКТЗЕД")
    ai_code_description = st.text_input("Введите описание товара, чтобы получить варианты кодов:")
    if st.button("Предложить коды"):
        if ai_code_description:
            with st.spinner("AI подбирает коды..."):
                suggested_codes = get_ai_code_suggestions(ai_code_description)
                if suggested_codes:
                    st.success("Рекомендуемые коды (можно скопировать и вставить в фильтр ниже):")
                    # Превращаем список в строку, удобную для копирования
                    codes_str = ", ".join(suggested_codes)
                    st.code(codes_str)
                else:
                    st.error("Не удалось подобрать коды.")
        else:
            st.warning("Введите описание товара.")
    
    st.markdown("---")
    st.subheader("Поиск по фильтрам")

    # --- (остальной код фильтров без изменений) ---
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

# --- ЛОГИКА ФИЛЬТРОВ ---
if search_button_filters:
    # ... (код этой секции остается без изменений) ...
    pass
