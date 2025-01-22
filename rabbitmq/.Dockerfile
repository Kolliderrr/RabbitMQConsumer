# Используем базовый образ Python 3.12.1 slim
FROM python:3.12.1-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# # Копируем файл requirements.txt и устанавливаем зависимости
COPY src/requirements.txt .

# Устанавливаем необходимые пакеты
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта в рабочую директорию контейнера
COPY src/ .

# Указываем точку входа
RUN addgroup --system app && adduser --system --group app

USER app
