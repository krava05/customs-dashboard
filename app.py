import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account

# --- ФУНКЦИЯ ПРОВЕРКИ ПАРОЛЯ ---
def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    
    if "APP_PASSWORD" not in st.secrets:
        st.error("Пароль не настроен в 'секретах' приложения!")
        return False

    if "password_correct" not in st.session_state:
        st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.error("😕 Пароль невірний. Спробуйте ще раз.")
        st.text_input("Введіть пароль для доступу", type="password", on_change=password_entered, key="password")
        return False
    else:
        return True

# --- ОСНОВНОЙ КОД ПРИЛОЖЕНИЯ ---
if check_password():
    PROJECT_ID = "ua-customs-analytics"
    TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

    # --- БЕЗОПАСНАЯ АУТЕНТИФИКАЦИЯ (НОВЫЙ АЛГОРИТМ) ---
    try:
        creds_dict = {
            "type": st.secrets["gcp_type"],
            "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"],
            "private_key": st.secrets["gcp_private_key"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"],
            "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"],
            "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"],
        }
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        st.session_state['client_ready'] = True
    except Exception as e:
        st.error(f"Помилка аутентифікації в Google: {e}")
        st.session_state['client_ready'] = False

    # ... остальной код приложения ...
    @st.cache_data
    def load_data(query):
        if st.session_state.get('client_ready', False):
            try:
                df = client.query(query).to_dataframe()
                return df
            except Exception as e:
                st.error(f"Помилка під час завантаження даних з BigQuery: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    if st.session_state.get('client_ready', False):
        st.set_page_config(layout="wide")
        st.title("Аналітика Митних Даних")
        st.sidebar.header("Фільтри")
        # ... (все фильтры и отображение данных остаются без изменений)
        nazva_kompanii = st.sidebar.text_input("Назва компанії")
        kod_yedrpou = st.sidebar.text_input("Код ЄДРПОУ")
        kraina_partner = st.sidebar.text_input("Країна-партнер")
        kod_uktzed = st.sidebar.text_input("Код УКТЗЕД")
        direction = st.sidebar.selectbox("Напрямок", ["Все", "Імпорт", "Експорт"])
        query = f"SELECT * FROM `{TABLE_ID}` WHERE 1=1"
        if nazva_kompanii: query += f" AND LOWER(nazva_kompanii) LIKE LOWER('%{nazva_kompanii}%')"
        if kod_yedrpou: query += f" AND kod_yedrpou LIKE '%{kod_yedrpou}%'"
        if kraina_partner: query += f" AND LOWER(kraina_partner) LIKE LOWER('%{kraina_partner}%')"
        if kod_uktzed: query += f" AND kod_uktzed LIKE '%{kod_uktzed}%'"
        if direction != "Все": query += f" AND napryamok = '{direction}'"
        query += " LIMIT 5000"
        df = load_data(query)
        if not df.empty:
            st.success(f"Знайдено {len(df)} записів (показано до 5000)")
            df['mytna_vartist_hrn'] = pd.to_numeric(df['mytna_vartist_hrn'], errors='coerce')
            df['vaha_netto_kg'] = pd.to_numeric(df['vaha_netto_kg'], errors='coerce')
            total_value = df['mytna_vartist_hrn'].sum()
            total_weight = df['vaha_netto_kg'].sum()
            col1, col2 = st.columns(2)
            col1.metric("Загальна митна вартість, грн", f"{total_value:,.2f}")
            col2.metric("Загальна вага, кг", f"{total_weight:,.2f}")
            st.dataframe(df)
        else:
            st.warning("За вашим запитом нічого не знайдено.")
