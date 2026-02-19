# -------------------------------
# Dockerfile для Streamlit сервиса
# -------------------------------

FROM python:3.10-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libfreetype6-dev \
    libpng-dev \
    libpq-dev \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ENABLECORS=false \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Рабочая директория
WORKDIR /app

# Копируем зависимости (пусть лежит в этой же папке streamlit/)
COPY requirements.txt /app/requirements.txt

# Устанавливаем зависимости Python
RUN pip install --upgrade pip && pip install --no-cache-dir -r /app/requirements.txt

# Копируем код приложения
COPY . /app

# Открываем порт Streamlit
EXPOSE 8501

# Запускаем Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
