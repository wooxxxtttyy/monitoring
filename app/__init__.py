from flask import Flask, request, jsonify
import psycopg2 # type: ignore
import redis # type: ignore
import json
import os
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST # type: ignore
import time

app = Flask(__name__)

REQUEST_COUNT = Counter(
    "app_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "http_status"]
)

REQUEST_LATENCY = Histogram(
    "app_request_latency_seconds",
    "Request latency",
    ["endpoint"]
)

DB_ERRORS = Counter(
    "db_errors_total",
    "Total DB errors"
)

@app.before_request
def start_timer():
    request.start_time = time.time()

@app.after_request
def record_metrics(response):
    resp_time = time.time() - request.start_time
    REQUEST_LATENCY.labels(request.path).observe(resp_time)
    REQUEST_COUNT.labels(request.method, request.path, response.status_code).inc()
    return response

@app.errorhandler(Exception)
def handle_error(error):
    endpoint = request.path
    status = "500"

    if hasattr(error, "code"):
        status = str(error.code)

    REQUEST_COUNT.labels(request.method, endpoint, status).inc()

    if hasattr(request, "start_time"):
        duration = time.time() - request.start_time
        REQUEST_LATENCY.labels(endpoint).observe(duration)

    if hasattr(error, "code"):
        return jsonify({"error": str(error)}), error.code
    return jsonify({"error": "Internal Server Error"}), 500

# подключаемся к бдшке 
def get_pg():
    return psycopg2.connect(
        host="postgres",
        database="appbd",
        user="wooxxtttyy",
        password="1"
    )

def get_cache():
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),  # Было "REDIS_HOST", стало "redis"
        port=6379, 
        db=0
    ) 
# redis это быстрая память 
# туда кладём данные, которые часто запрашивают, чтобы не обращаться каждый раз в бд
# функция get_cache() открывает доступ к redis

# инициализация бдшки
def bootstrap_db():
    conn = get_pg()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL
        )
    """)
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (name, email) VALUES (%s, %s), (%s, %s)",
                    ("Dasha", "gorunova@rtyu.com", "Regina", "ilalova@cvbn.com"))
    conn.commit()
    cur.close()
    conn.close()

# создаем юсера
@app.route("/participants", methods=["POST"])
def create_user():
    payload = request.get_json()
    conn = get_pg()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id, name, email",
        (payload["name"], payload["email"])
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    cache = get_cache()
    cache.delete("all_participants")

    return jsonify({"id": row[0], "name": row[1], "email": row[2]}), 201

# смотрим список, если он есть в кэше, то от туда, если нет, 
# то берем список из бд, и он заодно делает копию в кэш
@app.route("/participants", methods=["GET"])
def list_users():
    cache = get_cache()
    cached = cache.get("all_participants")
    if cached:
        return jsonify({"source": "cache", "data": json.loads(cached)})

    conn = get_pg()
    cur = conn.cursor()
    cur.execute("SELECT id, name, email FROM users")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = [{"id": r[0], "name": r[1], "email": r[2]} for r in rows]
    get_cache().setex("all_participants", 60, json.dumps(data))
    return jsonify({"source": "db", "data": data})

# кого-то конкретного ищем
@app.route("/participants/<int:user_id>", methods=["GET"])
def get_user(user_id):
    cache = get_cache()
    key = f"participant_{user_id}"
    cached = cache.get(key)
    if cached:
        return jsonify({"source": "cache", "data": json.loads(cached)})

    conn = get_pg()
    cur = conn.cursor()
    cur.execute("SELECT id, name, email FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "User not found"}), 404

    result = {"id": row[0], "name": row[1], "email": row[2]}
    get_cache().setex(key, 60, json.dumps(result))
    return jsonify({"source": "db", "data": result})

# если что-то поменялось, обнровляем данные и из кэша удаляем,
# чтоб там старая инфа не хранилась 
@app.route("/participants/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    payload = request.get_json()
    conn = get_pg()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET name=%s, email=%s WHERE id=%s RETURNING id, name, email",
        (payload["name"], payload["email"], user_id)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "User not found"}), 404

    cache = get_cache()
    cache.delete("all_participants")
    cache.delete(f"participant_{user_id}")

    return jsonify({"id": row[0], "name": row[1], "email": row[2]})

# удаляем ненужного кента
@app.route("/participants/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    conn = get_pg()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id=%s RETURNING id", (user_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "User not found"}), 404

    cache = get_cache()
    cache.delete("all_participants")
    cache.delete(f"participant_{user_id}")

    return jsonify({"message": f"User {user_id} deleted"})

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

bootstrap_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)