import aiosqlite
import json
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime

DB_PATH = "chat.db"

# ---------- Работа с БД ----------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await db.commit()

async def save_message(username: str, content: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO messages (username, content, created_at) VALUES (?, ?, ?)",
            (username, content, datetime.now().strftime("%H:%M:%S")),
        )
        await db.commit()
        return cursor.lastrowid  # Возвращаем ID созданного сообщения

async def delete_message_from_db(msg_id: int, username: str):
    # Удаляем только если автор совпадает
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM messages WHERE id = ? AND username = ?",
            (msg_id, username),
        )
        await db.commit()
        return cursor.rowcount > 0  # True, если что-то удалилось

async def get_last_messages(limit: int = 50):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, username, content, created_at FROM messages ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [{"id": r[0], "username": r[1], "content": r[2], "created_at": r[3]} for r in reversed(rows)]


# ---------- Менеджер подключений ----------
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, username: str):
        await websocket.accept()
        self.active_connections[websocket] = username
        await self.broadcast_online_users()
         # Отправляем системное сообщение о входе
        await self.broadcast(json.dumps({"type": "system", "text": f"🟢 {username} зашёл в чат"}))

    async def change_username(self, websocket: WebSocket, new_username: str):
        if websocket in self.active_connections:
            self.active_connections[websocket] = new_username
            await self.broadcast_online_users()

    async def broadcast_typing(self, sender_ws: WebSocket, username: str):
        message = json.dumps({"type": "typing", "username": username})
        for connection in list(self.active_connections.keys()):
            if connection != sender_ws:
                try:
                    await connection.send_text(message)
                except Exception:
                    self.disconnect(connection)

    def disconnect(self, websocket: WebSocket):
        username = self.active_connections.get(websocket, "Неизвестный")  # Безопасно получаем имя
        if websocket in self.active_connections:
            del self.active_connections[websocket]
        asyncio.create_task(self.broadcast_online_users())
        # Создаем фоновую задачу для отправки уведомления о выходе, 
        # так как метод disconnect не является async
        asyncio.create_task(self.broadcast(json.dumps({"type": "system", "text": f"🔴 {username} покинул чат"})))
        

    async def broadcast(self, message: str):
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

    async def broadcast_online_users(self):
        users = list(self.active_connections.values())
        message = json.dumps({"type": "users", "data": users})
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()

# ---------- Приложение ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await manager.connect(websocket, username)

    # Отправляем историю (теперь в виде JSON)
    history = await get_last_messages(limit=50)
    for msg in history:
        await websocket.send_text(json.dumps({
            "type": "message",
            "id": msg["id"],
            "username": msg["username"],
            "content": msg["content"],
            "created_at": msg["created_at"]
        }))

    try:
        while True:
            data = await websocket.receive_text()
            
            try:
                parsed = json.loads(data)
                
                # Смена ника
                if parsed.get("type") == "change_name":
                    new_name = parsed.get("new_name")
                    if new_name and len(new_name) < 20:
                        await manager.change_username(websocket, new_name)
                        await manager.broadcast(json.dumps({"type": "system", "text": f"📢 {username} сменил ник на {new_name}"}))
                        username = new_name
                    continue
                
                # Индикатор печати
                elif parsed.get("type") == "typing":
                    await manager.broadcast_typing(websocket, username)
                    continue
                
                # Удаление сообщения
                elif parsed.get("type") == "delete":
                    msg_id = parsed.get("id")
                    success = await delete_message_from_db(msg_id, username)
                    if success:
                        # Рассылаем всем команду удалить этот элемент с экрана
                        await manager.broadcast(json.dumps({"type": "delete", "id": msg_id}))
                    continue
                    
            except json.JSONDecodeError:
                pass
            
            # Если это обычное сообщение:
            msg_id = await save_message(username, data)
            timestamp = datetime.now().strftime("%H:%M:%S")
            await manager.broadcast(json.dumps({
                "type": "message",
                "id": msg_id,
                "username": username,
                "content": data,
                "created_at": timestamp
            }))
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)