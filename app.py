# ===============================================
# app.py - Система анализа таможенных данных
# Версия: 20.1
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
from io import BytesIO

# --- КОНФИГУРАЦИЯ ---
APP_VERSION = "Версия 20.1"
st.set_page_config(page_title="Аналітика Митних Даних", layout="wide")
PROJECT_ID = "ua-customs-analytics"
TABLE_ID = f"{PROJECT_ID}.ua_customs_data.declarations"

# --- СЛОВАРЬ ДЛЯ ОПИСАНИЯ ГРУПП УКТЗЕД ---
GROUP_DESCRIPTIONS = {
    '01': 'Живі тварини', '02': 'М\'ясо та їстівні субпродукти', '03': 'Риба і ракоподібні', '04': 'Молочні продукти, яйця, мед', '05': 'Інші продукти тваринного походження',
    '06': 'Живі дерева та інші рослини', '07': 'Овочі', '08': 'Їстівні плоди та горіхи', '09': 'Кава, чай, прянощі', '10': 'Зернові культури',
    '11': 'Продукція борошномельно-круп\'яної промисловості', '12': 'Олійне насіння та плоди', '13': 'Шелак, камеді, смоли', '14': 'Рослинні матеріали для виготовлення плетених виробів', '15': 'Жири та олії',
    '16': 'Готові харчові продукти з м\'яса, риби', '17': 'Цукор і кондитерські вироби', '18': 'Какао та продукти з нього', '19': 'Готові продукти із зерна', '20': 'Продукти переробки овочів, плодів, горіхів',
    '21': 'Різні харчові продукти', '22': 'Алкогольні і безалкогольні напої та оцет', '23': 'Залишки і відходи харчової промисловості', '24': 'Тютюн', '25': 'Сіль, сірка, землі та каміння, цемент',
    '26': 'Руди, шлак і зола', '27': 'Палива мінеральні, нафта', '28': 'Продукти неорганічної хімії', '29': 'Органічні хімічні сполуки', '30': 'Фармацевтична продукція',
    '31': 'Добрива', '32': 'Екстракти дубильні або барвильні', '33': 'Ефірні олії та косметика', '34': 'Мило, мийні засоби, воски', '35': 'Білкові речовини, клеї, ферменти',
    '36': 'Вибухові речовини, піротехнічні вироби', '37': 'Фото- і кінематографічні товари', '38': 'Різноманітна хімічна продукція', '39': 'Пластмаси, полімерні матеріали', '40': 'Каучук, гума та вироби з них',
    '41': 'Необроблені шкури', '42': 'Вироби зі шкіри', '43': 'Хутро', '44': 'Деревина та вироби з неї', '45': 'Корок та вироби з нього',
    '46': 'Вироби із соломи', '47': 'Маса з деревини', '48': 'Папір та картон', '49': 'Друкована продукція', '50': 'Шовк',
    '51': 'Вовна, волос тварин', '52': 'Бавовна', '53': 'Інші рослинні текстильні волокна', '54': 'Нитки синтетичні або штучні', '55': 'Волокна синтетичні або штучні',
    '56': 'Вата, повсть, фетр', '57': 'Килими', '58': 'Спеціальні тканини', '59': 'Текстильні матеріали, просочені', '60': 'Трикотажні полотна',
    '61': 'Одяг та аксесуари, трикотажні', '62': 'Одяг та аксесуари, текстильні', '63': 'Інші готові текстильні вироби', '64': 'Взуття', '65': 'Головні убори',
    '66': 'Парасольки, тростини', '67': 'Оброблене пір\'я та пух', '68': 'Вироби з каменю, гіпсу, цементу', '69': 'Керамічні вироби', '70': 'Скло та вироби з нього',
    '71': 'Перли, дорогоцінне каміння, біжутерія', '72': 'Чорні метали', '73': 'Вироби з чорних металів', '74': 'Мідь і вироби з неї', '75': 'Нікель і вироби з нього',
    '76': 'Алюміній і вироби з нього', '78': 'Свинець і вироби з нього', '79': 'Цинк і вироби з нього', '80': 'Олово і вироби з нього', '81': 'Інші недорогоцінні метали',
    '82': 'Інструменти, ножові вироби', '83': 'Інші вироби з недорогоцінних металів', '84': 'Машини, обладнання і механічні пристрої', '85': 'Електричні машини й устаткування', '86': 'Залізничне обладнання',
    '87': 'Транспортні засоби, крім залізничних', '88': 'Літальні апарати', '89': 'Судна, човни', '90': 'Прилади та апарати оптичні, фотографічні', '91': 'Годинники',
    '92': 'Музичні інструменти', '93': 'Зброя та боєприпаси', '94': 'Меблі, освітлювальні прилади', '95': 'Іграшки, ігри та спортивний інвентар', '96': 'Різні промислові товари',
    '97': 'Твори мистецтва, предмети колекціонування'
}

@st.cache_data
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    processed_data = output.getvalue()
    return processed_data

# --- ФУНКЦИИ --- (без изменений)

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
        st.error(f"Помилка аутентифікації в Google: {e}"); st.session_state.client_ready = False

def run_query(query, job_config=None):
    if st.session_state.get('client_ready', False):
        try:
            return st.session_state.bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception as e:
            st.error(f"Помилка під час виконання запиту до BigQuery: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def get_ai_code_suggestions(product_description):
    if not st.session_state.get('genai_ready', False): return None
    prompt = f"""
    Ти експерт з митної класифікації та українських кодів УКТЗЕД. Проаналізуй опис товару та надай список потенційних кодів УКТЗЕД.
    Включи коди різної довжини (наприклад, 4, 6, 10 знаків). Твоя відповідь МАЄ БУТИ ТІЛЬКИ у форматі JSON, що є єдиним списком рядків.
    Приклад правильної відповіді: ["8517", "851712", "8517120000"] Не додавай жодних описів, пояснень чи іншого тексту поза межами JSON-масиву.
    ОПИС ТОВАРУ: "{product_description}"
    """
    try:
        model = genai.GenerativeModel('models/gemini-pro-latest')
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        response = model.generate_content(prompt, generation_config=generation_config)
        cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
        response_json = json.loads(cleaned_text)
        if isinstance(response_json, list) and all(isinstance(i, str) for i in response_json): return response_json
        else: st.error("AI повернув дані у неочікуваному форматі."); return []
    except Exception as e:
        st.error(f"Помилка при отриманні кодів від AI: {e}"); return None

def find_and_validate_codes(product_description):
    theoretical_codes = get_ai_code_suggestions(product_description)
    if theoretical_codes is None or not theoretical_codes:
        st.warning("AI не зміг запропонувати коди. Спробуйте змінити опис товару."); return None, [], []
    unique_codes = list(set(filter(None, theoretical_codes)))
    if not unique_codes:
        st.warning("Відповідь AI не містить кодів для перевірки."); return None, [], []
    query_parts = []; query_params = []
    for i, code in enumerate(unique_codes):
        param_name = f"code{i}"; query_parts.append(f"STARTS_WITH(kod_uktzed, @{param_name})")
        query_params.append(ScalarQueryParameter(param_name, "STRING", code))
    where_clause = " OR ".join(query_parts)
    validation_query = f"""
    WITH BaseData AS (SELECT kod_uktzed, opis_tovaru, SAFE_CAST(mytna_vartist_hrn AS FLOAT64) as customs_value FROM `{TABLE_ID}` WHERE ({where_clause}) AND kod_uktzed IS NOT NULL),
    RankedDescriptions AS (SELECT kod_uktzed, opis_tovaru, ROW_NUMBER() OVER(PARTITION BY kod_uktzed ORDER BY COUNT(*) DESC) as rn FROM BaseData WHERE opis_tovaru IS NOT NULL GROUP BY kod_uktzed, opis_tovaru),
    Aggregates AS (SELECT kod_uktzed, COUNT(*) as total_declarations, SUM(customs_value) as total_value, AVG(customs_value) as avg_value FROM BaseData GROUP BY kod_uktzed)
    SELECT a.kod_uktzed AS `Код УКТЗЕД в базі`, rd.opis_tovaru AS `Найчастіший опис в базі`, a.total_declarations AS `Кількість декларацій`, a.total_value AS `Загальна вартість грн`, a.avg_value AS `Середня вартість грн`
    FROM Aggregates a JOIN RankedDescriptions rd ON a.kod_uktzed = rd.kod_uktzed WHERE rd.rn = 1 ORDER BY a.total_declarations DESC LIMIT 50
    """
    job_config = QueryJobConfig(query_parameters=query_params); validated_df = run_query(validation_query, job_config=job_config)
    if validated_df is not None and not validated_df.empty:
        pd.options.display.float_format = '{:,.2f}'.format
        validated_df['Загальна вартість грн'] = validated_df['Загальна вартість грн'].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "N/A")
        validated_df['Середня вартість грн'] = validated_df['Середня вартість грн'].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "N/A")
    found_prefixes = set()
    if validated_df is not None and not validated_df.empty:
        db_codes_series = validated_df["Код УКТЗЕД в базі"]
        for db_code in db_codes_series:
            for ai_code in unique_codes:
                if str(db_code).startswith(ai_code): found_prefixes.add(ai_code)
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
    query_months = f"SELECT DISTINCT EXTRACT(MONTH FROM SAFE_CAST(data_deklaracii AS DATE)) as month FROM `{TABLE_ID}` WHERE data_deklaracii IS NOT NULL ORDER BY month"
    options['months'] = list(run_query(query_months)['month'].dropna().astype(int))
    query_groups = f"SELECT DISTINCT SUBSTR(kod_uktzed, 1, 2) as group_code FROM `{TABLE_ID}` WHERE LENGTH(kod_uktzed) >= 2 ORDER BY group_code"
    options['groups'] = list(run_query(query_groups)['group_code'].dropna())
    return options

def reset_all_filters():
    st.session_state.selected_directions = []; st.session_state.selected_countries = []; st.session_state.selected_transports = []
    st.session_state.selected_years = []; st.session_state.selected_months = []; st.session_state.selected_groups = []
    st.session_state.selected_positions = []; st.session_state.weight_from = 0; st.session_state.weight_to = 0
    st.session_state.uktzed_input = ""; st.session_state.yedrpou_input = ""; st.session_state.company_input = ""
    # --- ИЗМЕНЕНИЕ 3: Очистка результатов поиска при сбросе фильтров ---
    if 'results_df' in st.session_state:
        del st.session_state.results_df

# --- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ ---

if not check_password():
    st.stop()

st.markdown("""
<style>
body { color: #111; }
.version-badge { position: fixed; top: 55px; right: 15px; padding: 5px 10px; border-radius: 8px; background-color: #f0f2f6; color: #31333F; font-size: 12px; z-index: 1000; }
</style>
""", unsafe_allow_html=True)
st.markdown(f'<p class="version-badge">{APP_VERSION}</p>', unsafe_allow_html=True)

st.title("Аналітика Митних Даних 📈")
initialize_clients()
if not st.session_state.get('client_ready', False):
    st.error("❌ Не вдалося підключитися до Google BigQuery."); st.stop()

st.header("🤖 AI-помічник по кодам УКТЗЕД")
ai_code_description = st.text_input("Введіть опис товару для пошуку реальних кодів у вашій базі:", key="ai_code_helper_input")
if st.button("💡 Запропонувати та перевірити коди", type="primary"):
    if ai_code_description:
        with st.spinner("AI підбирає коди, а ми перевіряємо їх у базі..."):
            validated_df, found, unfound = find_and_validate_codes(ai_code_description)
            st.session_state.validated_df = validated_df; st.session_state.found_ai_codes = found; st.session_state.unfound_ai_codes = unfound
    else: st.warning("Будь ласка, введіть опис товару.")

if 'validated_df' in st.session_state:
    validated_df = st.session_state.validated_df
    if validated_df is not None and not validated_df.empty:
        st.success(f"✅ Знайдено {len(validated_df)} релевантних кодів у вашій базі даних:")
        st.dataframe(validated_df, use_container_width=True)
        if st.session_state.found_ai_codes:
            st.info(f"Коди знайдено за цими пропозиціями AI: `{', '.join(st.session_state.found_ai_codes)}`")
    else: st.warning("🚫 У вашій базі даних не знайдено жодного коду, що відповідає пропозиціям AI.")
    if st.session_state.unfound_ai_codes:
        st.caption(f"Теоретичні коди від AI, для яких не знайдено збігів: `{', '.join(st.session_state.unfound_ai_codes)}`")
    if st.button("Очистити результат AI", type="secondary"):
        keys_to_delete = ['validated_df', 'found_ai_codes', 'unfound_ai_codes']
        for key in keys_to_delete:
            if key in st.session_state: del st.session_state[key]
        st.rerun()

st.divider()

# --- БЛОК РУЧНЫХ ФИЛЬТРОВ ---
filter_options = get_filter_options()
if 'selected_directions' not in st.session_state:
    reset_all_filters()

st.header("📊 Ручні фільтри")
st.button("Скинути всі фільтри", on_click=reset_all_filters, use_container_width=True, type="secondary")
st.markdown("---")

c1, c2, c3 = st.columns(3)
with c1: st.multiselect("Напрямок:", options=filter_options['direction'], key='selected_directions')
with c2: st.multiselect("Країна-партнер:", options=filter_options['countries'], key='selected_countries')
with c3: st.multiselect("Вид транспорту:", options=filter_options['transport'], key='selected_transports')

c4, c5, c6 = st.columns(3)
with c4: st.multiselect("Роки:", options=filter_options['years'], key='selected_years')
with c5: st.multiselect("Місяці:", options=filter_options['months'], key='selected_months')
with c6:
    w_col1, w_col2 = st.columns(2)
    w_col1.number_input("Вага від, кг", min_value=0, step=100, key="weight_from")
    w_col2.number_input("Вага до, кг", min_value=0, step=100, key="weight_to")

c7, c8, c9 = st.columns(3)
with c7: st.text_input("Код УКТЗЕД (через кому):", key='uktzed_input')
with c8: st.text_input("Код ЄДРПОУ (через кому):", key='yedrpou_input')
with c9: st.text_input("Назва компанії (через кому):", key='company_input')

st.markdown("---")
cg, cp = st.columns([1, 3])
with cg:
    group_options = [f"{g} - {GROUP_DESCRIPTIONS.get(g, 'Невідома група')}" for g in filter_options['groups']]
    st.multiselect("Товарна група (2 цифри):", options=group_options, key='selected_groups')
with cp:
    selected_group_codes = [g.split(' - ')[0] for g in st.session_state.get('selected_groups', [])]
    position_options = []
    if selected_group_codes:
        group_conditions = " OR ".join([f"STARTS_WITH(kod_uktzed, '{g}')" for g in selected_group_codes])
        query_positions = f"""
        WITH PositionCounts AS (
            SELECT SUBSTR(kod_uktzed, 1, 4) AS pos_code, opis_tovaru, COUNT(*) AS frequency
            FROM `{TABLE_ID}` WHERE ({group_conditions}) AND LENGTH(kod_uktzed) >= 4 GROUP BY pos_code, opis_tovaru
        ),
        RankedPositions AS (
            SELECT pos_code, opis_tovaru, ROW_NUMBER() OVER(PARTITION BY pos_code ORDER BY frequency DESC) AS rn
            FROM PositionCounts
        )
        SELECT pos_code, opis_tovaru AS pos_description FROM RankedPositions WHERE rn = 1 ORDER BY pos_code
        """
        position_df = run_query(query_positions)
        if not position_df.empty:
            for _, row in position_df.iterrows():
                position_options.append(f"{row['pos_code']} - {row['pos_description']}")
    st.multiselect("Товарна позиція (4 цифри):", options=position_options, key='selected_positions', disabled=not selected_group_codes)

st.markdown("---")
search_button_filters = st.button("🔍 Знайти за фільтрами", use_container_width=True, type="primary")

# --- ИЗМЕНЕНИЕ 4: Логика вынесена из-под кнопки в отдельный блок ---
# Сначала обрабатываем нажатие кнопки и сохраняем результат в сессию
if search_button_filters:
    query_parts = []; query_params = []
    def process_text_input(input_str): return [item.strip() for item in input_str.split(',') if item.strip()]
    if st.session_state.selected_directions:
        query_parts.append("napryamok IN UNNEST(@directions)"); query_params.append(ArrayQueryParameter("directions", "STRING", st.session_state.selected_directions))
    if st.session_state.selected_countries:
        query_parts.append("kraina_partner IN UNNEST(@countries)"); query_params.append(ArrayQueryParameter("countries", "STRING", st.session_state.selected_countries))
    if st.session_state.selected_transports:
        query_parts.append("vyd_transportu IN UNNEST(@transports)"); query_params.append(ArrayQueryParameter("transports", "STRING", st.session_state.selected_transports))
    if st.session_state.selected_years:
        query_parts.append("EXTRACT(YEAR FROM SAFE_CAST(data_deklaracii AS DATE)) IN UNNEST(@years)"); query_params.append(ArrayQueryParameter("years", "INT64", st.session_state.selected_years))
    if st.session_state.selected_months:
        query_parts.append("EXTRACT(MONTH FROM SAFE_CAST(data_deklaracii AS DATE)) IN UNNEST(@months)"); query_params.append(ArrayQueryParameter("months", "INT64", st.session_state.selected_months))
    if st.session_state.weight_from > 0:
        query_parts.append("SAFE_CAST(vaha_netto_kg AS FLOAT64) >= @weight_from"); query_params.append(ScalarQueryParameter("weight_from", "FLOAT64", st.session_state.weight_from))
    if st.session_state.weight_to > 0 and st.session_state.weight_to >= st.session_state.weight_from:
        query_parts.append("SAFE_CAST(vaha_netto_kg AS FLOAT64) <= @weight_to"); query_params.append(ScalarQueryParameter("weight_to", "FLOAT64", st.session_state.weight_to))
    
    selected_group_codes = [g.split(' - ')[0] for g in st.session_state.get('selected_groups', [])]
    if st.session_state.get('selected_positions', []):
        position_codes = [p.split(' - ')[0] for p in st.session_state.selected_positions]
        conditions = [f"STARTS_WITH(kod_uktzed, '{p}')" for p in position_codes]
        query_parts.append(f"({' OR '.join(conditions)})")
    elif selected_group_codes:
        conditions = [f"STARTS_WITH(kod_uktzed, '{g}')" for g in selected_group_codes]
        query_parts.append(f"({' OR '.join(conditions)})")
    
    uktzed_list = process_text_input(st.session_state.uktzed_input)
    if uktzed_list:
        conditions = []
        for i, item in enumerate(uktzed_list):
            param_name = f"uktzed{i}"; conditions.append(f"kod_uktzed LIKE @{param_name}")
            query_params.append(ScalarQueryParameter(param_name, "STRING", f"{item}%"))
        query_parts.append(f"({' OR '.join(conditions)})")
    yedrpou_list = process_text_input(st.session_state.yedrpou_input)
    if yedrpou_list:
        query_parts.append("kod_yedrpou IN UNNEST(@yedrpou)"); query_params.append(ArrayQueryParameter("yedrpou", "STRING", yedrpou_list))
    company_list = process_text_input(st.session_state.company_input)
    if company_list:
        conditions = []
        for i, item in enumerate(company_list):
            param_name = f"company{i}"; conditions.append(f"UPPER(nazva_kompanii) LIKE @{param_name}")
            query_params.append(ScalarQueryParameter(param_name, "STRING", f"%{item.upper()}%"))
        query_parts.append(f"({' OR '.join(conditions)})")
    
    if not query_parts:
        st.warning("Будь ласка, оберіть хоча б один фільтр.")
        st.session_state.results_df = pd.DataFrame() # Очищаем результаты, если фильтров нет
    else:
        where_clause = " AND ".join(query_parts)
        final_query = f"SELECT * FROM `{TABLE_ID}` WHERE {where_clause} LIMIT 5000"
        job_config = QueryJobConfig(query_parameters=query_params)
        with st.spinner("Виконується запит..."):
            st.session_state.results_df = run_query(final_query, job_config=job_config)

# Отображаем результаты, если они есть в сессии
if 'results_df' in st.session_state and st.session_state.results_df is not None:
    results_df = st.session_state.results_df.copy()
    st.success(f"Знайдено {len(results_df)} записів.")
    
    if not results_df.empty:
        ukrainian_column_names = {
            'data_deklaracii': 'Дата декларації', 'napryamok': 'Напрямок', 'nazva_kompanii': 'Назва компанії',
            'kod_yedrpou': 'Код ЄДРПОУ', 'kraina_partner': 'Країна-партнер', 'kod_uktzed': 'Код УКТЗЕД',
            'opis_tovaru': 'Опис товару', 'mytna_vartist_hrn': 'Митна вартість, грн', 'vaha_netto_kg': 'Вага нетто, кг',
            'vyd_transportu': 'Вид транспорту'
        }
        results_df = results_df.rename(columns=ukrainian_column_names)
        
        numeric_cols = ['Митна вартість, грн', 'Вага нетто, кг']
        for col in numeric_cols:
            if col in results_df.columns:
                results_df[col] = pd.to_numeric(results_df[col], errors='coerce')
        
        st.dataframe(results_df)

        excel_data = to_excel(results_df)
        st.download_button(
            label="📥 Завантажити в форматі Excel",
            data=excel_data,
            file_name='customs_data_export.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
