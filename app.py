import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re
import google.generativeai as genai

# --- НАЛАШТУВАННЯ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"
LOCAL_JSON_KEY = "ua-customs-analytics-08c5189db4e4.json"

# --- ФУНКЦІЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    def password_entered():
        correct_password = ""
        if "APP_PASSWORD" in st.secrets:
            correct_password = st.secrets["APP_PASSWORD"]
        else:
            correct_password = os.environ.get("APP_PASSWORD")
        if correct_password and st.session_state["password"] == correct_password:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    
    if ("APP_PASSWORD" not in st.secrets) and (not os.environ.get("APP_PASSWORD")):
        st.error("Пароль не настроен!")
        return False
    if "password_correct" not in st.session_state:
        st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.error("😕 Пароль невірний.")
        st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
        return False
    else:
        return True

# --- ФУНКЦИЯ ДЛЯ ГЕНЕРАЦИИ SQL С ПОМОЩЬЮ ИИ ---
def generate_sql_from_prompt(user_question):
    try:
        api_key = st.secrets.get("GOOGLE_API_KEY") if 'GOOGLE_API_KEY' in st.secrets else os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            st.error("API ключ для Google AI не настроен!")
            return None
        
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('models/gemini-pro-latest')

        prompt = f"""
        Ты — ИИ-ассистент...
        Вопрос пользователя: "{user_question}"
        SQL-запрос:
        """
        response = model.generate_content(prompt)
        sql_query = re.sub(r"```sql|```", "", response.text).strip()
        return sql_query
    except Exception as e:
        st.error(f"Помилка під час генерації SQL: {e}")
        return None

# --- ОСНОВНОЙ КОД ПРИЛОЖЕНИЯ ---
os.environ.setdefault('APP_PASSWORD', '123456')

if check_password():
    try:
        if os.environ.get('K_SERVICE'):
            client = bigquery.Client(project=PROJECT_ID)
        else:
            client = bigquery.Client.from_service_account_json(LOCAL_JSON_KEY)
        st.session_state['client_ready'] = True
    except Exception as e:
        st.error(f"Помилка аутентифікації в Google Cloud: {e}")
        st.session_state['client_ready'] = False

    @st.cache_data
    def run_query(query):
        if st.session_state.get('client_ready', False):
            try:
                df = client.query(query).to_dataframe()
                return df
            except Exception as e:
                st.error(f"Помилка під час виконання запиту: {e}")
                st.code(query)
                return pd.DataFrame()
        return pd.DataFrame()

    if st.session_state.get('client_ready', False):
        st.set_page_config(layout="wide")
        st.title("Аналітика Митних Даних")

        tab1, tab2 = st.tabs(["Пошук за Фільтрами", "AI-Пошук"])

        with tab1:
            with st.expander("Панель Фільтрів", expanded=True):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    start_date = st.date_input("Дата, з", value=None, format="DD.MM.YYYY")
                    nazva_kompanii = st.text_input("Назва компанії")
                with col2:
                    end_date = st.date_input("Дата, по", value=None, format="DD.MM.YYYY")
                    kod_yedrpou = st.text_input("Код ЄДРПОУ")
                with col3:
                    direction = st.selectbox("Напрямок", ["Все", "Імпорт", "Експорт"], key="manual_direction")
                    country_list = run_query(f"SELECT DISTINCT kraina_partner FROM `{TABLE_ID}` WHERE kraina_partner IS NOT NULL ORDER BY kraina_partner")['kraina_partner'].tolist()
                    kraina_partner = st.multiselect("Країна-партнер", country_list)
                with col4:
                    uktzed_list = run_query(f"SELECT DISTINCT kod_uktzed FROM `{TABLE_ID}` WHERE kod_uktzed IS NOT NULL ORDER BY kod_uktzed")['kod_uktzed'].tolist()
                    kod_uktzed_filter = st.multiselect("Код УКТЗЕД", uktzed_list)
                    transport_list = run_query(f"SELECT DISTINCT vyd_transportu FROM `{TABLE_ID}` WHERE vyd_transportu IS NOT NULL ORDER BY vyd_transportu")['vyd_transportu'].tolist()
                    transport_filter = st.multiselect("Вид транспорту", transport_list)
                opis_tovaru = st.text_input("Пошук по опису товару")

            query_parts = [f"SELECT * FROM `{TABLE_ID}` WHERE 1=1"]
            if start_date and end_date:
                if start_date > end_date: st.error("Дата початку не може бути пізніше дати закінчення.")
                else: query_parts.append(f" AND data_deklaracii BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'")
            if nazva_kompanii: query_parts.append(f" AND LOWER(nazva_kompanii) LIKE LOWER('%{nazva_kompanii}%')")
            if kod_yedrpou: query_parts.append(f" AND kod_yedrpou LIKE '%{kod_yedrpou}%'")
            
            # --- ИСПРАВЛЕНИЕ: Используем более простую и надежную конкатенацию строк ---
            if kraina_partner:
                formatted_countries = ", ".join(["'" + c.replace("'", "''") + "'" for c in kraina_partner])
                query_parts.append(f" AND kraina_partner IN ({formatted_countries})")
            if transport_filter:
                formatted_transports = ", ".join(["'" + t.replace("'", "''") + "'" for t in transport_filter])
                query_parts.append(f" AND vyd_transportu IN ({formatted_transports})")
            if kod_uktzed_filter:
                formatted_uktzed = ", ".join(["'" + c.replace("'", "''") + "'" for c in kod_uktzed_filter])
                query_parts.append(f" AND kod_uktzed IN ({formatted_uktzed})")
            
            if direction != "Все": query_parts.append(f" AND napryamok = '{direction}'")
            if opis_tovaru: query_parts.append(f" AND LOWER(opis_tovaru) LIKE LOWER('%{opis_tovaru}%')")
            
            manual_query = "".join(query_parts) + " ORDER BY data_deklaracii DESC LIMIT 5000"
            df_manual = run_query(manual_query)
            
            if not df_manual.empty:
                st.success(f"Знайдено {len(df_manual)} записів (показано до 5000)")
                df_manual['data_deklaracii'] = pd.to_datetime(df_manual['data_deklaracii'], errors='coerce').dt.strftime('%d.%m.%Y')
                df_manual['kod_yedrpou'] = pd.to_numeric(df_manual['kod_yedrpou'], errors='coerce').fillna(0).astype(int).astype(str).replace('0', '')
                st.dataframe(df_manual, use_container_width=True)
            else:
                st.warning("За вашим запитом нічого не знайдено.")

        with tab2:
            st.subheader("Пошук за допомогою Штучного Інтелекту")
            ai_question = st.text_area("Задайте питання до бази даних (наприклад, 'покажи 5 найбільших імпортерів з Польщі по вартості')", height=100, key="ai_question")
            
            if st.button("Знайти за допомогою AI"):
                if ai_question:
                    with st.spinner("ІІ генерує SQL-запит..."):
                        sql_query = generate_sql_from_prompt(ai_question)
                    if sql_query:
                        st.success("Згенерований SQL-запит:")
                        st.code(sql_query, language="sql")
                        with st.spinner("Виконую запит до бази даних..."):
                            df_ai = run_query(sql_query)
                        st.subheader("Результат AI-пошуку")
                        if not df_ai.empty:
                            df_ai['data_deklaracii'] = pd.to_datetime(df_ai['data_deklaracii'], errors='coerce').dt.strftime('%d.%m.%Y')
                            df_ai['kod_yedrpou'] = pd.to_numeric(df_ai['kod_yedrpou'], errors='coerce').fillna(0).astype(int).astype(str).replace('0', '')
                            st.dataframe(df_ai, use_container_width=True)
                        else:
                            st.warning("За вашим AI-запитом нічого не знайдено.")
                else:
                    st.warning("Будь ласка, введіть питання для AI.")
