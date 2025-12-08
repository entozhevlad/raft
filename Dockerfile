# Используем официальный образ Python
FROM python:3.12-slim

# Не спрашивать вопросы при установке пакетов
ENV DEBIAN_FRONTEND=noninteractive

# Рабочая директория внутри контейнера
WORKDIR /app

# Устанавливаем системные зависимости (если вдруг что-то понадобится)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем uv (менеджер зависимостей)
RUN pip install --no-cache-dir uv

# Копируем файлы проекта
# Сначала только pyproject.toml/uv.lock (если есть), чтобы кэшировать зависимости
COPY pyproject.toml ./
# Если у тебя есть uv.lock — раскомментируй следующую строку
# COPY uv.lock ./

# Ставим зависимости в изолированное окружение (по умолчанию .venv)
RUN uv sync --frozen || uv sync

# Теперь копируем исходники
COPY src ./src

# Открываем порт (внутри контейнера всегда 8000)
EXPOSE 8000

# Команда запуска: uv -> uvicorn -> твой app
CMD ["uv", "run", "uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
