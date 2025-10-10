# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 15.0
# Дата: 2025-10-10
# Описание: 
# - "AI-помощник по кодам" стал умнее: теперь он не только предлагает
#   теоретические коды, но и проверяет их наличие в базе данных,
#   показывая только те, которые реально используются.
# - В таблицу результатов помощника добавлена колонка "Кол-во в базе".
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

# --- ВЕРСИЯ ПРИЛОЖЕНИЯ ---
APP_VERSION = "Версия 15.0"

# --- КОНФИГУРАЦИЯ СТРАНИЦЫ ---
st.set_page_config(page_title="Аналітика Митних Даних", layout="wide")

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- (Функции check_password, initialize_clients, run_query без изменений) ---
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

def run_query(query, job_config=None):
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


# --- ФУНКЦИЯ "AI-ПОМОЩНИК ПО КОДАМ" ---
def get_ai_code_suggestions(product_description):
    if not st.session_state.get('genai_ready', False): return None
    prompt = f"""
    You are an expert in customs classification... USER'S PRODUCT DESCRIPTION: "{product_description}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        safety_settings = { HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE }
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        response = model.generate_content(prompt, generation_config=generation_config, safety_settings=safety_settings)
        response_json = json.loads(response.text)
        if isinstance(response_json, dict):
            return response_json.get("suggestions", [])
        elif isinstance(response_json, list):
            return response_json
        else:
            return []
    except Exception as e:
        st.error(f"Помилка при получении кодов от AI: {e}")
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

# --- ЛОГИКА СБРОСА ФИЛЬТРОВ ---
def reset_all_filters():
    # ... (код без изменений) ...

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
if not check_password():
    st.stop()

st.markdown(f"""...""", unsafe_allow_html=True) # Version info
st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery."); st.stop()

filter_options = get_filter_options()
if 'selected_directions' not in st.session_state:
    reset_all_filters()


# --- РАЗДЕЛ: AI-ПОМОЩНИК ПО КОДАМ (ЛОГИКА ОБНОВЛЕНА) ---
st.header("🤖 AI-помощник по кодам УКТЗЕД")
ai_code_description = st.text_input("Введите описание товара...", key="ai_code_helper_input")

if st.button("Предложить коды"):
    if ai_code_description:
        with st.spinner("Этап 1/2: AI подбирает теоретические коды..."):
            theoretical_codes = get_ai_code_suggestions(ai_code_description)
        
        if theoretical_codes:
            df_theoretical = pd.DataFrame(theoretical_codes)
            code_list = df_theoretical['code'].tolist()
            
            with st.spinner("Этап 2/2: Проверяем наличие кодов в вашей базе данных..."):
                query_check = "SELECT kod_uktzed, COUNT(*) as usage_count FROM `ua-customs-analytics.ua_customs_data.declarations` WHERE kod_uktzed IN UNNEST(@codes) GROUP BY kod_uktzed"
                job_config_check = QueryJobConfig(query_parameters=[ArrayQueryParameter("codes", "STRING", code_list)])
                df_existing = run_query(query_check, job_config=job_config_check)

                if not df_existing.empty:
                    # Объединяем теоретические и практические результаты
                    df_final = pd.merge(df_theoretical, df_existing, left_on='code', right_on='kod_uktzed', how='inner')
                    # Переименовываем и упорядочиваем колонки
                    df_final = df_final.rename(columns={'code': 'Код', 'description': 'Описание', 'usage_count': 'Кол-во в базе'})
                    st.session_state.suggested_codes_table = df_final[['Код', 'Описание', 'Кол-во в базе']].sort_values(by='Кол-во в базе', ascending=False)
                else:
                    st.session_state.suggested_codes_table = pd.DataFrame() # Пустой DataFrame
        else:
            st.error("Не удалось подобрать теоретические коды.")
            st.session_state.suggested_codes_table = None

    else:
        st.warning("Введите описание товара.")

if 'suggested_codes_table' in st.session_state and st.session_state.suggested_codes_table is not None:
    if not st.session_state.suggested_codes_table.empty:
        st.success("Найдено совпадение теоретических кодов с вашей базой данных:")
        st.dataframe(st.session_state.suggested_codes_table, use_container_width=True)
    else:
        st.info("AI предложил релевантные коды, но ни один из них пока не найден в вашей базе данных.")

    if st.button("Очистить результат", type="secondary"):
        st.session_state.suggested_codes_table = None
        st.rerun()

st.divider()

# --- СЕКЦИЯ РУЧНЫХ ФИЛЬТРОВ ---
st.header("📊 Ручные фильтры")
with st.expander("Панель Фильтров", expanded=True):
    # ... (остальной код фильтров и их логики без изменений) ...
    pass
