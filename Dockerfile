FROM python:3.9-slim
WORKDIR /app
COPY . /app
RUN pip install fastapi uvicorn httpx requests
EXPOSE 8000
ENTRYPOINT ["python", "main.py"]
