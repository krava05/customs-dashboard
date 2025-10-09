import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.generativeai as genai
import json

print("--- DEBUG: Скрипт app.py начал выполняться ---")

# --- КОНФИГУРАЦИЯ СТРАНИЦЫ ---
try:
    st.set_page_config(
        page_title="Аналітика Митних Даних",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    print("--- DEBUG: st.set_page_config() выполнен успешно ---")
except Exception as e:
    print(f"--- CRITICAL ERROR on set_page_config: {e} ---")

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"
print(f"--- DEBUG: Глобальные переменные установлены. PROJECT_ID: {PROJECT_ID} ---")

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    print("--- DEBUG: Вход в функцию check_password() ---")
    def password_entered():
        print("--- DEBUG: Вход в функцию password_entered() ---")
        if os.environ.get('K_SERVICE'):
            correct_password = os.environ.get("APP_PASSWORD")
        else:
            correct_password = st.secrets.get("APP_PASSWORD")

        if st.session_state.get("password") and st.session_state["password"] == correct_password:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
            print("--- DEBUG: Пароль верный ---")
        else:
            st.session_state["password_correct"] = False
            print("--- DEBUG: Пароль неверный ---")

    if st.session_state.get("password_correct", False):
        print("--- DEBUG: check_password() возвращает True (пароль уже был введен) ---")
        return True

    st.text_input(
        "Введіть пароль для доступу", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("😕 Пароль невірний.")
    print("--- DEBUG: check_password() завершена, ожидание ввода пароля ---")
    return False

# --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ GOOGLE ---
def initialize_clients():
    print("--- DEBUG: Вход в функцию initialize_clients() ---")
    if 'clients_initialized' in st.session_state:
        print("--- DEBUG: Клиенты уже инициализированы, выход. ---")
        return

    try:
        print("--- DEBUG: Проверка окружения (K_SERVICE)... ---")
        if os.environ.get('K_SERVICE'):
            print("--- DEBUG: Запуск в Cloud Run. Инициализация BigQuery клиента... ---")
            st.session_state.bq_client = bigquery.Client(project=PROJECT_ID)
            print("--- DEBUG: BigQuery клиент успешно инициализирован. ---")
            
            api_key = os.environ.get("GOOGLE_AI_API_KEY")
            if not api_key:
                 print("--- DEBUG ERROR: GOOGLE_AI_API_KEY не найден. ---")
                 st.session_state.genai_ready = False
            else:
                print("--- DEBUG: API ключ найден. Настройка genai... ---")
                genai.configure(api_key=api_key)
                st.session_state.genai_ready = True
                print("--- DEBUG: genai успешно настроен. ---")
        else: 
            print("--- DEBUG: Локальный запуск. Пропускаем инициализацию. ---")
            # Логика для локального запуска...

        st.session_state.clients_initialized = True
        st.session_state.client_ready = True
        print("--- DEBUG: Инициализация клиентов завершена успешно. ---")

    except Exception as e:
        print(f"--- CRITICAL ERROR in initialize_clients: {e} ---")
        st.error(f"Помилка аутентифікації в Google: {e}")
        st.session_state.client_ready = False
        st.session_state.genai_ready = False

# --- (Остальной код без изменений) ---
@st.cache_data(ttl=600)
def run_query(query):
    # ...
    return
def get_ai_search_query(user_query, max_items=100):
    # ...
    return

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
print("--- DEBUG: Начало отрисовки основного интерфейса ---")
if not check_password():
    st.stop()

print("--- DEBUG: Пароль прошел. Инициализация клиентов... ---")
initialize_clients()

if not st.session_state.get('client_ready', False):
    print("--- DEBUG ERROR: Клиент не готов. Остановка. ---")
    st.error("❌ Не вдалося підключитися до Google BigQuery.")
    st.stop()

print("--- DEBUG: Клиент готов. Отрисовка заголовка... ---")
st.title("Аналітика Митних Даних 📈")
# ... (остальной код интерфейса)
print("--- DEBUG: Скрипт дошел до конца ---")
