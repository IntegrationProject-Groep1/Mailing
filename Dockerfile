FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN useradd --create-home --uid 10001 app

COPY mailing_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mailing_service/ ./

USER app

CMD ["python", "-u", "main.py"]
