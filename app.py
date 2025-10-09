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
        # <<< ИЗМЕНЕНИЕ ЗДЕСЬ: Используем полное имя модели из списка
        model = genai.GenerativeModel('models/gemini-pro-latest')
        response = model.generate_content(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        response_json = json.loads(response_text)
        return response_json.get("sql_query")
    except Exception as e:
        st.error(f"Помилка при генерації SQL за допомогою AI: {e}")
        return None
