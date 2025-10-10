# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 15.1
# ===============================================

import os
import streamlit as st
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ArrayQueryParameter, ScalarQueryParameter
import pandas as pd
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import json
import re

# --- КОНФИГУРАЦИЯ ---
APP_VERSION = "Версия 15.1"
st.set_page_config(page_title="Аналітика Митних Даних", layout="wide")
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- ФУНКЦИИ ---

def check_password():
    """Проверяет пароль доступа к приложению."""
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

def initialize_clients():
    """Инициализирует клиенты для BigQuery и Generative AI."""
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

def run_query(query, job_config=None):
    """Выполняет запрос к BigQuery и возвращает DataFrame."""
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def get_ai_code_suggestions(product_description):
    """Получает от AI список теоретических кодов УКТЗЕД."""
    if not st.session_state.get('genai_ready', False):
        return None
    
    prompt = f"""
    Ти експерт з митної класифікації та українських кодів УКТЗЕД.
    Проаналізуй опис товару та надай список потенційних кодів УКТЗЕД.
    Включи коди різної довжини (наприклад, 4, 6, 10 знаків).

    Твоя відповідь МАЄ БУТИ ТІЛЬКИ у форматі JSON, що є єдиним списком рядків.
    Приклад правильної відповіді: ["8517", "851712", "8517120000"]
    Не додавай жодних описів, пояснень чи іншого тексту поза межами JSON-масиву.

    ОПИС ТОВАРУ: "{product_description}"
    """
    try:
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        model = genai.GenerativeModel('models/gemini-pro-latest')
        # -------------------------
        
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        response = model.generate_content(prompt, generation_config=generation_config)
        
        cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
        response_json = json.loads(cleaned_text)

        if isinstance(response_json, list) and all(isinstance(i, str) for i in response_json):
            return response_json
        else:
            st.error("AI повернув дані у неочікуваному форматі.")
            return []
            
    except Exception as e:
        st.error(f"Помилка при отриманні кодів від AI: {e}")
        return None

def find_and_validate_codes(product_description):
    """Получает коды от AI и проверяет их наличие в базе данных BigQuery."""
    
    # 1. Получить теоретические коды от AI
    theoretical_codes = get_ai_code_suggestions(product_description)
    
    if theoretical_codes is None or not theoretical_codes:
        st.warning("AI не зміг запропонувати коди. Спробуйте змінити опис товару.")
        return None, [], []

    unique_codes = list(set(filter(None, theoretical_codes)))
    if not unique_codes:
        st.warning("Відповідь AI не містить кодів для перевірки.")
        return None, [], []

    # 2. Построить и выполнить запрос для проверки кодов в BigQuery
    query_parts = []
    query_params = []
    for i, code in enumerate(unique_codes):
        param_name = f"code{i}"
        query_parts.append(f"STARTS_WITH(kod_uktzed, @{param_name})")
        query_params.append(ScalarQueryParameter(param_name, "STRING", code))
        
    where_clause = " OR ".join(query_parts)
    
    validation_query = f"""
    WITH RankedDescriptions AS (
      SELECT
        kod_uktzed,
        opis_tovaru,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER(PARTITION BY kod_uktzed ORDER BY COUNT(*) DESC) as rn
      FROM `{TABLE_ID}`
      WHERE ({where_clause}) AND kod_uktzed IS NOT NULL
      GROUP BY kod_uktzed, opis_tovaru
    ),
    TotalCounts AS (
      SELECT kod_uktzed, SUM(cnt) as total_declarations
      FROM RankedDescriptions
      GROUP BY kod_uktzed
    )
    SELECT
      rd.kod_uktzed AS "Код УКТЗЕД в базі",
      rd.opis_tovaru AS "Найчастіший опис в базі",
      tc.total_declarations AS "Кількість декларацій"
    FROM RankedDescriptions rd
    JOIN TotalCounts tc ON rd.kod_uktzed = tc.kod_uktzed
    WHERE rd.rn = 1
    ORDER BY tc.total_declarations DESC
    LIMIT 50
    """
    
    job_config = QueryJobConfig(query_parameters=query_params)
    validated_df = run_query(validation_query, job_config=job_config)
    
    # 3. Определить, какие из предложенных AI кодов дали результат
    found_prefixes = set()
    if not validated_df.empty:
        # Получаем столбец с кодами, которые нашлись в базе
        db_codes_series = validated_df["Код УКТЗЕД в базі"]
        for db_code in db_codes_series:
            for ai_code in unique_codes:
                if str(db_code).startswith(ai_code):
                    found_prefixes.add(ai_code)
    
    unfound_codes = set(unique_codes) - found_prefixes
    
    return validated_df, list(found_prefixes), list(unfound_codes)


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
    st.session_state.selected_years = []
    st.session_state.weight_from = 0
    st.session_state.weight_to = 0
    st.session_state.uktzed_input = ""
    st.session_state.yedrpou_input = ""
    st.session_state.company_input = ""

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---

if not check_password():
    st.stop()

st.sidebar.info(APP_VERSION)
st.title("Аналітика Митних Даних 📈")

initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery.")
    st.stop()

# --- БЛОК AI-ПОМОЩНИКА ---
st.header("🤖 AI-помічник по кодам УКТЗЕД")
ai_code_description = st.text_input("Введіть опис товару для пошуку реальних кодів у вашій базі:", key="ai_code_helper_input")

if st.button("💡 Запропонувати та перевірити коди", type="primary"):
    if ai_code_description:
        with st.spinner("AI підбирає коди, а ми перевіряємо їх у базі..."):
            validated_df, found, unfound = find_and_validate_codes(ai_code_description)
            st.session_state.validated_codes_df = validated_df
            st.session_state.found_ai_codes = found
            st.session_state.unfound_ai_codes = unfound
    else:
        st.warning("Будь ласка, введіть опис товару.")

if 'validated_codes_df' in st.session_state:
    validated_df = st.session_state.validated_codes_df
    
    if validated_df is not None and not validated_df.empty:
        st.success(f"✅ Знайдено {len(validated_df)} релевантних кодів у вашій базі даних:")
        st.dataframe(validated_df, use_container_width=True)
        if st.session_state.found_ai_codes:
            st.info(f"Коди знайдено за цими пропозиціями AI: `{', '.join(st.session_state.found_ai_codes)}`")
    else:
        st.warning("🚫 У вашій базі даних не знайдено жодного коду, що відповідає пропозиціям AI.")

    if st.session_state.unfound_ai_codes:
        st.caption(f"Теоретичні коди від AI, для яких не знайдено збігів: `{', '.join(st.session_state.unfound_ai_codes)}`")

    if st.button("Очистити результат AI", type="secondary"):
        keys_to_delete = ['validated_codes_df', 'found_ai_codes', 'unfound_ai_codes']
        for key in keys_to_delete:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

st.divider()

# --- БЛОК РУЧНЫХ ФИЛЬТРОВ ---
filter_options = get_filter_options()
if 'selected_directions' not in st.session_state:
    reset_all_filters()

st.header("📊 Ручні фільтри")
with st.expander("Панель Фільтрів", expanded=True):
    st.button("Скинути всі фільтри", on_click=reset_all_filters, use_container_width=True, type="secondary")
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
    search_button_filters = st.button("🔍 Знайти за фільтрами", use_container_width=True, type="primary")

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
        with st.spinner("Виконується запит..."):
            results_df = run_query(final_query, job_config=job_config)
            st.success(f"Знайдено {len(results_df)} записів.")
            st.dataframe(results_df)
