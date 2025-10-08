import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.generativeai as genai
import json

# --- КОНФИГУРАЦИЯ СТРАНИЦЫ ---
st.set_page_config(
    page_title="Аналітика Митних Даних",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    """Returns `True` if the user had a correct password."""
    def password_entered():
        if st.session_state["password"] == st.secrets.get("APP_PASSWORD", os.environ.get("APP_PASSWORD")):
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

# --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ GOOGLE ---
def initialize_clients():
    """Initialize BigQuery and GenerativeAI clients and store in session state."""
    if 'clients_initialized' in st.session_state:
        return

    try:
        # Для Cloud Run аутентификация происходит автоматически через сервисный аккаунт
        if os.environ.get('K_SERVICE'):
            st.session_state.bq_client = bigquery.Client(project=PROJECT_ID)
            # Убедитесь, что GOOGLE_AI_API_KEY установлен в переменных окружения Cloud Run
            api_key = st.secrets.get("GOOGLE_AI_API_KEY", os.environ.get("GOOGLE_AI_API_KEY"))
            if not api_key:
                 st.error("Ключ API для Google AI не знайдено в оточенні.")
                 st.session_state.genai_ready = False
            else:
                genai.configure(api_key=api_key)
                st.session_state.genai_ready = True
        else: # Локальный запуск
            # Убедитесь, что ваш JSON ключ доступен по этому пути
            SERVICE_ACCOUNT_FILE = "ua-customs-analytics-08c5189db4e4.json"
            st.session_state.bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
            # Для локального запуска используйте secrets.toml или установите переменную окружения
            api_key = st.secrets.get("GOOGLE_AI_API_KEY")
            if not api_key:
                 st.error("Для локального запуску створіть файл .streamlit/secrets.toml та додайте GOOGLE_AI_API_KEY = 'Ваш_ключ'")
                 st.session_state.genai_ready = False
            else:
                 genai.configure(api_key=api_key)
                 st.session_state.genai_ready = True

        st.session_state.clients_initialized = True
        st.session_state.client_ready = True

    except Exception as e:
        st.error(f"Помилка аутентифікації в Google: {e}")
        st.session_state.client_ready = False
        st.session_state.genai_ready = False

# --- ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ ---
@st.cache_data(ttl=600) # Кэширование данных на 10 минут
def run_query(query):
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- ФУНКЦИЯ ДЛЯ AI-ПОИСКА ТОВАРОВ ---
def get_ai_search_query(user_query, max_items=100):
    if not st.session_state.get('genai_ready', False):
        st.warning("Google AI не ініціалізовано. AI-пошук недоступний.")
        return None

    prompt = f"""
    Based on the user's request, generate a SQL query for Google BigQuery.
    The table is `{TABLE_ID}`.
    Select the fields: `opis_tovaru`, `nazva_kompanii`, `kraina_partner`, `data_deklaracii`, `mytna_vartist_hrn`, `vaha_netto_kg`.
    Use `REGEXP_CONTAINS` with the `(?i)` flag for case-insensitive search on the `opis_tovaru` field.
    The query must be a simple SELECT statement. Do not use CTEs or subqueries.
    Limit the results to {max_items}.
    Return ONLY a valid JSON object with a single key "sql_query" containing the full SQL string.

    User request: "{user_query}"

    Example of a valid JSON response:
    {{
        "sql_query": "SELECT opis_tovaru, nazva_kompanii, kraina_partner, data_deklaracii, mytna_vartist_hrn, vaha_netto_kg FROM `{TABLE_ID}` WHERE REGEXP_CONTAINS(opis_tovaru, '(?i)some search term') LIMIT 100"
    }}
    """
    try:
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = model.generate_content(prompt)
        # Очистка ответа от возможных markdown-оберток
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        response_json = json.loads(response_text)
        return response_json.get("sql_query")
    except Exception as e:
        st.error(f"Помилка при генерації SQL за допомогою AI: {e}")
        return None

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
if not check_password():
    st.stop()

st.title("Аналітика Митних Даних 📈")
initialize_clients()

if st.session_state.get('client_ready', False):
    st.success("✅ Підключення до Google BigQuery успішне.")
else:
    st.error("❌ Не вдалося підключитися до Google BigQuery.")
    st.stop()

# --- ВКЛАДКИ ---
tab1, tab2 = st.tabs(["AI-Пошук Товарів 🤖", "Панель Фільтрів 📊"])

with tab1:
    st.header("Інтелектуальний пошук товарів за описом")
    ai_search_query_text = st.text_input(
        "Опишіть товар, який шукаєте (наприклад, 'кава зернова з Колумбії' або 'дитячі іграшки з пластику')",
        key="ai_search_input"
    )
    search_button = st.button("Знайти за допомогою AI", type="primary")

    if "ai_search_results" not in st.session_state:
        st.session_state.ai_search_results = pd.DataFrame()

    if search_button and ai_search_query_text:
        with st.spinner("✨ AI генерує запит і шукає дані..."):
            ai_sql = get_ai_search_query(ai_search_query_text)
            if ai_sql:
                st.code(ai_sql, language='sql')
                st.session_state.ai_search_results = run_query(ai_sql)
            else:
                st.error("Не вдалося згенерувати SQL-запит.")
                st.session_state.ai_search_results = pd.DataFrame()

    if not st.session_state.ai_search_results.empty:
        st.success(f"Знайдено **{len(st.session_state.ai_search_results)}** записів.")
        st.dataframe(st.session_state.ai_search_results)
    elif search_button:
        st.info("За вашим запитом нічого не знайдено.")


with tab2:
    st.header("Фільтрація та аналіз даних")
    with st.expander("Панель Фільтрів", expanded=True):
        # ... (здесь код всех ваших старых фильтров)
        st.write("Тут будуть ваші стандартні фільтри (за компанією, кодом УКТЗЕД тощо).")
        # TODO: Добавьте сюда ваши фильтры, которые были раньше

    # ... (здесь код для построения SQL на основе фильтров и отображения таблицы)
    # st.dataframe(...)
