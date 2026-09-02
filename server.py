import sqlite3
import secrets
import asyncio
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

import uvicorn


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


# =========================
# DATABASE
# =========================

db = sqlite3.connect(
    "chat.db",
    check_same_thread=False
)

db.execute("""
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    token TEXT UNIQUE NOT NULL,
    created_at TEXT NOT NULL
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    username TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL
)
""")

db.commit()


# =========================
# CONNECTIONS
# =========================

connections = set()


# =========================
# USERS
# =========================

def create_user(username):

    user_id = "user_" + secrets.token_hex(6)

    token = secrets.token_hex(32)

    db.execute(
        """
        INSERT INTO users
        (id, username, token, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            token,
            datetime.now().isoformat()
        )
    )

    db.commit()

    return user_id, token


def get_user(token):

    if not token:
        return None

    return db.execute(
        """
        SELECT id, username
        FROM users
        WHERE token=?
        """,
        (token,)
    ).fetchone()


# =========================
# MAIN PAGE
# =========================

@app.get("/")
async def index():

    return FileResponse("index.html")


# =========================
# REGISTER
# =========================

@app.post("/register")
async def register(data: dict):

    username = data.get(
        "username",
        ""
    ).strip()

    if not username:

        return {
            "error": "Введите имя"
        }

    if len(username) > 20:

        return {
            "error": "Максимум 20 символов"
        }

    old = db.execute(
        """
        SELECT id
        FROM users
        WHERE username=?
        """,
        (username,)
    ).fetchone()

    if old:

        return {
            "error": "Это имя уже занято"
        }

    user_id, token = create_user(
        username
    )

    return {
        "id": user_id,
        "username": username,
        "token": token
    }


# =========================
# HISTORY
# =========================

@app.get("/history")
async def history():

    rows = db.execute("""
        SELECT
            id,
            user_id,
            username,
            text,
            created_at
        FROM messages
        ORDER BY id ASC
    """).fetchall()

    return [
        {
            "id": row[0],
            "userId": row[1],
            "sender": row[2],
            "text": row[3],
            "time": row[4]
        }
        for row in rows
    ]


# =========================
# WEBSOCKET
# =========================

@app.websocket("/ws")
async def websocket(
    websocket: WebSocket
):

    token = websocket.query_params.get(
        "token"
    )

    user = get_user(token)

    if not user:

        await websocket.close(
            code=1008
        )

        return

    user_id, username = user

    await websocket.accept()

    connections.add(
        websocket
    )

    try:

        while True:

            data = await websocket.receive_json()

            if data.get("type") != "message":
                continue

            text = str(
                data.get("text", "")
            ).strip()

            if not text:
                continue

            if len(text) > 500:
                continue

            created = datetime.now().isoformat()

            # Сохраняем СООБЩЕНИЕ НА СЕРВЕРЕ
            cursor = db.execute(
                """
                INSERT INTO messages
                (
                    user_id,
                    username,
                    text,
                    created_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    user_id,
                    username,
                    text,
                    created
                )
            )

            db.commit()

            message = {
                "type": "message",

                "id": cursor.lastrowid,

                "userId": user_id,

                "sender": username,

                "text": text,

                "time": created
            }

            # Отправляем ВСЕМ подключенным
            dead_connections = []

            for connection in list(connections):

                try:

                    await connection.send_json(
                        message
                    )

                except Exception:

                    dead_connections.append(
                        connection
                    )

            for connection in dead_connections:

                connections.discard(
                    connection
                )

    except WebSocketDisconnect:

        connections.discard(
            websocket
        )

    except Exception:

        connections.discard(
            websocket
        )


# =========================
# START
# =========================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
