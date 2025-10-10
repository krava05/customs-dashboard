# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 14.1
# Дата: 2025-10-10
# Описание: 
# - Улучшена обработка ответа от AI-помощника. Теперь код 
#   корректно работает, даже если AI возвращает список напрямую,
#   а не объект. Это исправляет ошибку "'list' object has no attribute 'get'".
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
APP_VERSION = "Версия 14.1"

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
    You are an expert in customs classification and Ukrainian HS codes (УКТЗЕД).
    Analyze the user's product description. Your goal is to suggest a list of the most relevant 4 to 10-digit HS codes.
    CRITICAL INSTRUCTIONS:
    1.  Your entire response MUST be a single, valid JSON object with one key: "suggestions".
    2.  The value of "suggestions" must be an array of JSON objects.
    3.  Each object must have two keys: "code" (the HS code as a string) and "description" (a brief explanation in Ukrainian).
    4.  Do not add any explanations or introductory text.
    VALID JSON RESPONSE EXAMPLE:
    {{
      "suggestions": [
        {{"code": "88073000", "description": "Частини до безпілотних літальних апаратів"}},
        {{"code": "85076000", "description": "Акумулятори літій-іонні"}}
      ]
    }}
    USER'S PRODUCT DESCRIPTION: "{product_description}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        safety_settings = { HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE }
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        response = model.generate_content(prompt, generation_config=generation_config, safety_settings=safety_settings)
        response_json = json.loads(response.text)
        
        # <<< ИЗМЕНЕНИЕ ЗДЕСЬ >>>
        # Проверяем, вернула ли модель объект (как просили) или сразу список
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
    st.session_state.selected_directions = []
    st.session_state.selected_countries = []
    st.session_state.selected_transports = []
    st.session_state.selected_years = []
    st.session_state.weight_from = 0
    st.session_state.weight_to = 0
    st.session_state.uktzed_input = ""
    st.session_state.yedrpou_input = ""
    st.session_state.company_input = ""

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
if not check_password():
    st.stop()

st.markdown(
    f"""
    <style> .version-info {{ position: fixed; top: 55px; right: 15px; font-size: 0.8em; color: gray; z-index: 100; }} </style>
    <div class="version-info">{APP_VERSION}</div>
