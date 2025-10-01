import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json

# --- НАЛАШТУВАННЯ ---
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- БЕЗОПАСНАЯ АУТЕНТИФИКАЦИЯ ---
# Проверяем, запущен ли код в облаке Streamlit
if 'GCP_CREDENTIALS' in st.secrets:
    # Используем "секрет" из облака
    creds_dict = st.secrets["GCP_CREDENTIALS"]
    creds_json_str = json.dumps(creds_dict)
    with open("gcp_credentials.json", "w") as f:
        f.write(creds_json_str)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"
else:
    # Используем локальный файл при запуске на вашем компьютере
    JSON_KEY_PATH = "ua-customs-analytics-08c5189db4e4.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = JSON_KEY_PATH


# --- ФУНКЦІЯ ДЛЯ ЗАВАНТАЖЕННЯ ДАНИХ ---
@st.cache_data
def load_data(query):
    try:
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Помилка під час завантаження даних з BigQuery: {e}")
        return pd.DataFrame()

# --- ІНТЕРФЕЙС ЗАСТОСУНКУ (остается без изменений) ---
st.set_page_config(layout="wide")
st.title("Аналітика Митних Даних")
st.sidebar.header("Фільтри")
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