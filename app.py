import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re
import google.generativeai as genai

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    # Эта функция теперь просто проверяет переменную окружения APP_PASSWORD
    def password_entered():
        correct_password = os.environ.get("APP_PASSWORD")
        if correct_password and st.session_state.get("password") == correct_password:
            st.session_state["password_correct"] = True
            if "password" in st.session_state:
                del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    # Показываем поле ввода
    st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
    
    # Показываем ошибку, если пароль был введен, но неверный
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("😕 Пароль невірний.")
        
    return False

# --- ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---
st.set_page_config(layout="wide")

# Сначала показываем только экран входа
if not check_password():
    st.stop() # Если пароль неверный, останавливаем выполнение остального кода

# --- ЕСЛИ ПАРОЛЬ ВЕРНЫЙ, ЗАГРУЖАЕМ ВСЕ ОСТАЛЬНОЕ ---

st.title("Аналітика Митних Даних")

# НАЛАШТУВАННЯ
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# АУТЕНТИФИКАЦИЯ
try:
    if os.environ.get('K_SERVICE'): # Проверка, что мы в Cloud Run
        client = bigquery.Client(project=PROJECT_ID)
    else: # Локальный запуск
        LOCAL_JSON_KEY = "ua-customs-analytics-08c5189db4e4.json"
        client = bigquery.Client.from_service_account_json(LOCAL_JSON_KEY)
    st.session_state['client_ready'] = True
except Exception as e:
    st.error(f"Помилка аутентифікації в Google: {e}")
    st.session_state['client_ready'] = False

# ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ
@st.cache_data
def run_query(query):
    if st.session_state.get('client_ready', False):
        try:
            df = client.query(query).to_dataframe()
            return df
        except Exception as e:
            st.error(f"Помилка під час виконання запиту: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# ОСНОВНОЙ ИНТЕРФЕЙС С ФИЛЬТРАМИ
if st.session_state.get('client_ready', False):
    with st.expander("Панель Фільтрів", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            start_date = st.date_input("Дата, з", value=None, format="DD.MM.YYYY")
            nazva_kompanii = st.text_input("Назва компанії")
        with col2:
            end_date = st.date_input("Дата, по", value=None, format="DD.MM.YYYY")
            kod_yedrpou = st.text_input("Код ЄДРПОУ")
        with col3:
            direction = st.selectbox("Напрямок", ["Все", "Імпорт", "Експорт"])
            country_list = run_query(f"SELECT DISTINCT kraina_partner FROM `{TABLE_ID}` WHERE kraina_partner IS NOT NULL ORDER BY kraina_partner")['kraina_partner'].tolist()
            kraina_partner = st.multiselect("Країна-партнер", country_list)
        with col4:
            uktzed_list = run_query(f"SELECT DISTINCT kod_uktzed FROM `{TABLE_ID}` WHERE kod_uktzed IS NOT NULL ORDER BY kod_uktzed")['kod_uktzed'].tolist()
            kod_uktzed_filter = st.multiselect("Код УКТЗЕД", uktzed_list)
            transport_list = run_query(f"SELECT DISTINCT vyd_transportu FROM `{TABLE_ID}` WHERE vyd_transportu IS NOT NULL ORDER BY vyd_transportu")['vyd_transportu'].tolist()
            transport_filter = st.multiselect("Вид транспорту", transport_list)
        opis_tovaru = st.text_input("Пошук по опису товару")

    # ПОСТРОЕНИЕ SQL-ЗАПРОСА
    query_parts = [f"SELECT * FROM `{TABLE_ID}` WHERE 1=1"]
    if start_date and end_date:
        if start_date > end_date:
            st.error("Дата початку не може бути пізніше дати закінчення.")
        else:
            query_parts.append(f" AND data_deklaracii BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'")
    if nazva_kompanii: query_parts.append(f" AND LOWER(nazva_kompanii) LIKE LOWER('%{nazva_kompanii}%')")
    if kod_yedrpou: query_parts.append(f" AND kod_yedrpou LIKE '%{kod_yedrpou}%'")
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
    
    query = "".join(query_parts) + " ORDER BY data_deklaracii DESC LIMIT 5000"
    
    # ОТОБРАЖЕНИЕ ДАННЫХ
    df = run_query(query)
    if not df.empty:
        df['data_deklaracii'] = pd.to_datetime(df['data_deklaracii'], errors='coerce').dt.strftime('%d.%m.%Y')
        st.success(f"Знайдено {len(df)} записів (показано до 5000)")
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("За вашим запитом нічого не знайдено.")
