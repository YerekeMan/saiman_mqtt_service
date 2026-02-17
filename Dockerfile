# Используем легкий образ Python
FROM python:3.10-slim

# Устанавливаем рабочую папку в контейнере
WORKDIR /app

# Копируем список библиотек и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код в контейнер
COPY . .

# Запускаем скрипт
CMD ["python", "main.py"]