import sqlite3
import secrets
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

db = sqlite3.connect("chat.db", check_same_thread=False)
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

connections = set()


def create_user(username):
    user_id = "user_" + secrets.token_hex(6)
    token = secrets.token_hex(32)

    db.execute(
        "INSERT INTO users VALUES (?, ?, ?, ?)",
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
    return db.execute(
        "SELECT id, username FROM users WHERE token=?",
        (token,)
    ).fetchone()


@app.get("/")
async def index():
    return FileResponse("index.html")


@app.post("/register")
async def register(data: dict):

    username = data.get("username", "").strip()

    if not username:
        return {"error": "Введите имя"}

    if len(username) > 20:
        return {"error": "Максимум 20 символов"}

    old = db.execute(
        "SELECT id FROM users WHERE username=?",
        (username,)
    ).fetchone()

    if old:
        return {"error": "Это имя уже занято"}

    user_id, token = create_user(username)

    return {
        "id": user_id,
        "username": username,
        "token": token
    }


@app.get("/history")
async def history():

    rows = db.execute("""
        SELECT id, user_id, username, text, created_at
        FROM messages
        ORDER BY id ASC
        LIMIT 5000
    """).fetchall()

    return [
        {
            "id": r[0],
            "userId": r[1],
            "sender": r[2],
            "text": r[3],
            "time": r[4]
        }
        for r in rows
    ]


@app.websocket("/ws")
async def websocket(websocket: WebSocket):

    token = websocket.query_params.get("token")

    user = get_user(token)

    if not user:
        await websocket.close()
        return

    user_id, username = user

    await websocket.accept()

    connections.add(websocket)

    try:

        while True:

            data = await websocket.receive_json()

            if data.get("type") != "message":
                continue

            text = str(data.get("text", "")).strip()

            if not text:
                continue

            if len(text) > 500:
                continue

            created = datetime.now().isoformat()

            cursor = db.execute(
                """
                INSERT INTO messages
                (user_id, username, text, created_at)
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

            dead = []

            for connection in connections:

                try:
                    await connection.send_json(message)

                except:
                    dead.append(connection)

            for connection in dead:
                connections.discard(connection)

    except WebSocketDisconnect:
        connections.discard(websocket)

    except:
        connections.discard(websocket)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
