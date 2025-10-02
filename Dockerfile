# Используем официальный образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл с зависимостями
COPY requirements.txt ./requirements.txt

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код приложения
COPY . .

# Открываем порт 8080 для доступа извне
EXPOSE 8080

# Запускаем приложение при старте контейнера
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.enableCORS=false"]
