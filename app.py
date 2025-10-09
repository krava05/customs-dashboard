import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.generativeai as genai
import json

# --- КОНФИГУРАЦІЯ СТОРІНКИ ---
st.set_page_config(
    page_title="Аналітика Митних Даних",
    layout="wide"
)

# --- ГЛОБАЛЬНІ ЗМІННІ ---
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

    st.text_input(
        "Введіть пароль для доступу", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("😕 Пароль невірний.")
    return False

# --- ІНІЦІАЛІЗАЦІЯ КЛІЄНТІВ GOOGLE ---
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

# --- ФУНКЦІЯ ДЛЯ AI-ПОШУКУ ---
def get_ai_search_query(user_query, max_items=100):
    if not st.session_state.get('genai_ready', False):
        st.warning("Google AI не ініціалізовано. AI-пошук недоступний.")
        return None
    prompt = f"""
    Based on the user's request, generate a SQL query for Google BigQuery.
    The table is `{TABLE_ID}`.
    Select all fields.
    Use `REGEXP_CONTAINS` with the `(?i)` flag for case-insensitive search on the `opis_tovaru` field.
    Limit the results to {max_items}.
    Return ONLY a valid JSON object with a single key "sql_query" containing the full SQL string.

    User request: "{user_query}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        response = model.generate_content(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        response_json = json.loads(response_text)
        return response_json.get("sql_query")
    except Exception as e:
        st.error(f"Помилка при генерації SQL за допомогою AI: {e}")
        return None

# --- ЗАВАНТАЖЕННЯ СПИСКІВ ДЛЯ ФІЛЬТРІВ ---
@st.cache_data(ttl=3600)
def get_filter_options():
    options = {}
    # Статичні опції
    options['direction'] = ['', 'Імпорт', 'Експорт']
    # Динамічні опції з BigQuery
    query_companies = f"SELECT DISTINCT nazva_kompanii FROM `{TABLE_ID}` WHERE nazva_kompanii IS NOT NULL ORDER BY nazva_kompanii"
    options['companies'] = [''] + list(run_query(query_companies)['nazva_kompanii'])
    query_countries = f"SELECT DISTINCT kraina_partner FROM `{TABLE_ID}` WHERE kraina_partner IS NOT NULL ORDER BY kraina_partner"
    options['countries'] = [''] + list(run_query(query_countries)['kraina_partner'])
    query_transport = f"SELECT DISTINCT vyd_transportu FROM `{TABLE_ID}` WHERE vyd_transportu IS NOT NULL ORDER BY vyd_transportu"
    options['transport'] = [''] + list(run_query(query_transport)['vyd_transportu'])
    return options

# --- ОСНОВНИЙ ІНТЕРФЕЙС ---
if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()

if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery.")
    st.stop()

# --- СЕКЦІЯ AI-ПОШУКУ ---
st.header("🤖 Інтелектуальний пошук товарів за описом")
ai_search_query_text = st.text_input("Опишіть товар, який шукаєте...", key="ai_search_input")
search_button_ai = st.button("Знайти за допомогою AI", type="primary")

if search_button_ai and ai_search_query_text:
    with st.spinner("✨ AI генерує запит і шукає дані..."):
        ai_sql = get_ai_search_query(ai_search_query_text)
        if ai_sql:
            st.code(ai_sql, language='sql')
            ai_results_df = run_query(ai_sql)
            st.success(f"Знайдено {len(ai_results_df)} записів.")
            st.dataframe(ai_results_df)
        else:
            st.error("Не вдалося згенерувати SQL-запит.")

st.divider()

# --- СЕКЦІЯ ФІЛЬТРІВ ---
st.header("📊 Фільтрація та аналіз даних")
filter_options = get_filter_options()

with st.expander("Панель Фільтрів", expanded=True):
    # Перший ряд фільтрів
    col1, col2, col3 = st.columns(3)
    with col1:
        direction = st.selectbox("Напрямок:", options=filter_options['direction'])
    with col2:
        country = st.selectbox("Країна-партнер:", options=filter_options['countries'])
    with col3:
        transport = st.selectbox("Вид транспорту:", options=filter_options['transport'])

    # Другий ряд фільтрів
    col4, col5 = st.columns([1, 3])
    with col4:
        uktzed = st.text_input("Код УКТЗЕД (можна частину):")
    with col5:
        company = st.selectbox("Назва компанії:", options=filter_options['companies'])

    search_button_filters = st.button("🔍 Знайти за фільтрами")

# --- ЛОГІКА ФОРМУВАННЯ ЗАПИТУ І ВІДОБРАЖЕННЯ РЕЗУЛЬТАТІВ ---
if search_button_filters:
    query_parts = []
    if direction:
        query_parts.append(f"napryamok = '{direction}'")
    if company:
        query_parts.append(f"nazva_kompanii = '{company.replace(\"'\", \"''\")}'")
    if country:
        query_parts.append(f"kraina_partner = '{country.replace(\"'\", \"''\")}'")
    if transport:
        query_parts.append(f"vyd_transportu = '{transport.replace(\"'\", \"''\")}'")
    if uktzed:
        query_parts.append(f"kod_uktzed LIKE '{uktzed}%'")

    if not query_parts:
        st.warning("Будь ласка, оберіть хоча б один фільтр.")
    else:
        where_clause = " AND ".join(query_parts)
        final_query = f"""
        SELECT *
        FROM `{TABLE_ID}`
        WHERE {where_clause}
        LIMIT 1000
        """
        st.code(final_query, language='sql')
        with st.spinner("Виконується запит..."):
            results_df = run_query(final_query)
            st.success(f"Знайдено {len(results_df)} записів.")
            st.dataframe(results_df)
