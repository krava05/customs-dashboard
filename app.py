import streamlit as st

st.title("Диагностика Секретов")

st.write("Все доступные ключи 'секретов':")

# Этот код попытается показать все ключи, которые видит приложение
try:
    st.write(st.secrets.keys())
except Exception as e:
    st.error(f"Не удалось получить ключи секретов: {e}")

st.write("---")

# Проверяем наличие конкретного секрета APP_PASSWORD
st.write("Проверка наличия 'APP_PASSWORD':")
if 'APP_PASSWORD' in st.secrets:
    st.success("Секрет 'APP_PASSWORD' НАЙДЕН!")
else:
    st.warning("Секрет 'APP_PASSWORD' НЕ НАЙДЕН.")
