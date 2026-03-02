FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# чтоб все в логах было сразу
ENV PYTHONUNBUFFERED=1 

# gunicorn это WSGI-сервер. WSGI это правило, как веб-сервер должен общаться с python-приложением.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "__init__:app"]