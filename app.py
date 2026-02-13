import os
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "messenger.db"
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

ACCESS_CODE = "7xTM[xN[K0FEG&wMKU6TYBbyZMu}H7?v*PLsHAyV"

app = FastAPI(title="LAN Messenger")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
db_lock = threading.Lock()

active_connections: dict[int, set[WebSocket]] = {}
call_rooms: dict[int, dict[int, WebSocket]] = {}
call_states: dict[int, dict[int, dict]] = {}


class GateIn(BaseModel):
    code: str


class RegisterIn(BaseModel):
    username: str
    password: str
    nickname: str


class LoginIn(BaseModel):
    username: str
    password: str


class ProfileIn(BaseModel):
    nickname: str
    about: str = ""


class FriendRequestIn(BaseModel):
    username: str


class DirectIn(BaseModel):
    user_id: int


class GroupIn(BaseModel):
    title: str
    members: list[int] = []


class SettingsIn(BaseModel):
    allow_friend_requests: str = "everyone"
    allow_calls_from: str = "friends"
    allow_group_invites: str = "friends"
    show_last_seen: str = "friends"


class PasswordChangeIn(BaseModel):
    old_password: str
    new_password: str


class AssetUploadIn(BaseModel):
    kind: str


VALID_SETTING_VALUES = {
    "allow_friend_requests": {"everyone", "nobody"},
    "allow_calls_from": {"everyone", "friends", "nobody"},
    "allow_group_invites": {"everyone", "friends", "nobody"},
    "show_last_seen": {"everyone", "friends", "nobody"},
}


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def serialize_user(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "username": row["username"],
        "nickname": row["nickname"],
        "avatar": row["avatar"],
        "about": row["about"] or "",
    }


def init_db():
    conn = get_db()
    with conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                nickname TEXT NOT NULL,
                avatar TEXT,
                about TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                allow_friend_requests TEXT NOT NULL DEFAULT 'everyone',
                allow_calls_from TEXT NOT NULL DEFAULT 'friends',
                allow_group_invites TEXT NOT NULL DEFAULT 'friends',
                show_last_seen TEXT NOT NULL DEFAULT 'friends',
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS blocked_users (
                blocker_id INTEGER NOT NULL,
                blocked_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(blocker_id, blocked_id),
                FOREIGN KEY(blocker_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(blocked_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS friend_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_user_id INTEGER NOT NULL,
                to_user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                UNIQUE(from_user_id, to_user_id),
                FOREIGN KEY(from_user_id) REFERENCES users(id),
                FOREIGN KEY(to_user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS friends (
                user_id INTEGER NOT NULL,
                friend_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, friend_id),
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(friend_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                title TEXT,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(created_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS chat_members (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                joined_at TEXT NOT NULL,
                UNIQUE(chat_id, user_id),
                FOREIGN KEY(chat_id) REFERENCES chats(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL DEFAULT 'text',
                text TEXT,
                file_path TEXT,
                file_name TEXT,
                mime_type TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(chat_id) REFERENCES chats(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS message_deleted_for (
                message_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(message_id, user_id),
                FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS custom_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                title TEXT,
                file_path TEXT NOT NULL,
                file_name TEXT,
                mime_type TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
    conn.close()


init_db()


def get_user_by_token(token: str) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute(
        """
        SELECT u.*
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token = ?
        """,
        (token,),
    ).fetchone()
    conn.close()
    return row


def get_current_user(request: Request) -> sqlite3.Row:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = auth.replace("Bearer ", "", 1).strip()
    user = get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user


def require_gate(request: Request):
    if request.cookies.get("lan_gate_ok") != "1":
        raise HTTPException(status_code=403, detail="Access code required")


def ensure_settings(conn: sqlite3.Connection, user_id: int):
    conn.execute(
        """
        INSERT OR IGNORE INTO user_settings(user_id, allow_friend_requests, allow_calls_from, allow_group_invites, show_last_seen)
        VALUES (?, 'everyone', 'friends', 'friends', 'friends')
        """,
        (user_id,),
    )


def get_settings(conn: sqlite3.Connection, user_id: int) -> dict:
    ensure_settings(conn, user_id)
    row = conn.execute("SELECT * FROM user_settings WHERE user_id = ?", (user_id,)).fetchone()
    return dict(row)


def is_friend(conn: sqlite3.Connection, a: int, b: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM friends WHERE user_id = ? AND friend_id = ?",
        (a, b),
    ).fetchone()
    return bool(row)


def is_blocked(conn: sqlite3.Connection, blocker_id: int, blocked_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM blocked_users WHERE blocker_id = ? AND blocked_id = ?",
        (blocker_id, blocked_id),
    ).fetchone()
    return bool(row)


def is_any_block(conn: sqlite3.Connection, a: int, b: int) -> bool:
    return is_blocked(conn, a, b) or is_blocked(conn, b, a)


def can_access_chat(conn: sqlite3.Connection, user_id: int, chat_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM chat_members WHERE user_id = ? AND chat_id = ?",
        (user_id, chat_id),
    ).fetchone()
    return bool(row)


def get_chat(conn: sqlite3.Connection, chat_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM chats WHERE id = ?", (chat_id,)).fetchone()


def get_direct_peer(conn: sqlite3.Connection, chat_id: int, user_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT u.*
        FROM chat_members cm
        JOIN users u ON u.id = cm.user_id
        WHERE cm.chat_id = ? AND cm.user_id != ?
        LIMIT 1
        """,
        (chat_id, user_id),
    ).fetchone()


def can_call_user(conn: sqlite3.Connection, caller_id: int, target_id: int) -> tuple[bool, str]:
    if is_any_block(conn, caller_id, target_id):
        return False, "Звонок недоступен из-за блокировки"
    target_settings = get_settings(conn, target_id)
    mode = target_settings["allow_calls_from"]
    if mode == "nobody":
        return False, "Пользователь запретил звонки"
    if mode == "friends" and not is_friend(conn, target_id, caller_id):
        return False, "Пользователь принимает звонки только от друзей"
    return True, ""


def can_invite_to_group(conn: sqlite3.Connection, inviter_id: int, target_id: int) -> tuple[bool, str]:
    if is_any_block(conn, inviter_id, target_id):
        return False, "Приглашение недоступно из-за блокировки"
    target_settings = get_settings(conn, target_id)
    mode = target_settings["allow_group_invites"]
    if mode == "nobody":
        return False, "Пользователь запретил приглашения в группы"
    if mode == "friends" and not is_friend(conn, target_id, inviter_id):
        return False, "Пользователь принимает приглашения только от друзей"
    return True, ""


def serialize_message(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "chat_id": row["chat_id"],
        "user_id": row["user_id"],
        "username": row["username"],
        "nickname": row["nickname"],
        "avatar": row["avatar"],
        "kind": row["kind"],
        "text": row["text"] or "",
        "file_url": f"/uploads/{row['file_path']}" if row["file_path"] else None,
        "file_name": row["file_name"],
        "mime_type": row["mime_type"],
        "created_at": row["created_at"],
    }


async def push_to_user(user_id: int, payload: dict):
    connections = active_connections.get(user_id, set()).copy()
    dead = []
    for ws in connections:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        active_connections.get(user_id, set()).discard(ws)


async def broadcast_to_chat(chat_id: int, payload: dict):
    conn = get_db()
    members = conn.execute("SELECT user_id FROM chat_members WHERE chat_id = ?", (chat_id,)).fetchall()
    conn.close()
    for m in members:
        await push_to_user(m["user_id"], payload)


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/gate/status")
async def gate_status(request: Request):
    return {"ok": request.cookies.get("lan_gate_ok") == "1"}


@app.post("/api/gate")
async def gate(data: GateIn):
    if data.code != ACCESS_CODE:
        raise HTTPException(status_code=403, detail="Неверный код доступа")
    response = JSONResponse({"ok": True})
    response.set_cookie("lan_gate_ok", "1", httponly=True, samesite="lax", max_age=60 * 60 * 24 * 30)
    return response


@app.post("/api/register")
async def register(data: RegisterIn, request: Request):
    require_gate(request)
    username = data.username.strip().lower()
    nickname = data.nickname.strip()
    if len(username) < 3 or len(username) > 32:
        raise HTTPException(status_code=400, detail="Username: 3-32 символа")
    if not username.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Username: буквы, цифры, _")
    if len(data.password) < 6:
        raise HTTPException(status_code=400, detail="Пароль слишком короткий")
    if not nickname:
        raise HTTPException(status_code=400, detail="Укажите ник")

    conn = get_db()
    try:
        with db_lock, conn:
            conn.execute(
                "INSERT INTO users(username, password_hash, nickname, created_at) VALUES (?, ?, ?, ?)",
                (username, hash_password(data.password), nickname, now_iso()),
            )
            user = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
            ensure_settings(conn, user["id"])
            token = secrets.token_urlsafe(32)
            conn.execute(
                "INSERT INTO sessions(token, user_id, created_at) VALUES (?, ?, ?)",
                (token, user["id"], now_iso()),
            )
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=409, detail="Username уже занят")

    conn.close()
    return {"token": token, "user": serialize_user(user)}


@app.post("/api/login")
async def login(data: LoginIn, request: Request):
    require_gate(request)
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE username = ?", (data.username.strip().lower(),)).fetchone()
    if not user or not verify_password(data.password, user["password_hash"]):
        conn.close()
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    ensure_settings(conn, user["id"])
    token = secrets.token_urlsafe(32)
    with db_lock, conn:
        conn.execute(
            "INSERT INTO sessions(token, user_id, created_at) VALUES (?, ?, ?)",
            (token, user["id"], now_iso()),
        )
    conn.close()
    return {"token": token, "user": serialize_user(user)}


@app.post("/api/logout")
async def logout(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth.replace("Bearer ", "", 1).strip()
        conn = get_db()
        with db_lock, conn:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.close()
    return {"ok": True}


@app.delete("/api/account")
async def delete_account(user=Depends(get_current_user)):
    user_id = user["id"]
    conn = get_db()
    with db_lock, conn:
        owned = conn.execute("SELECT id, type FROM chats WHERE created_by = ?", (user_id,)).fetchall()
        for ch in owned:
            new_owner = conn.execute(
                "SELECT user_id FROM chat_members WHERE chat_id = ? AND user_id != ? ORDER BY user_id LIMIT 1",
                (ch["id"], user_id),
            ).fetchone()
            if new_owner:
                conn.execute(
                    "UPDATE chats SET created_by = ? WHERE id = ?",
                    (new_owner["user_id"], ch["id"]),
                )
                if ch["type"] == "group":
                    conn.execute(
                        "UPDATE chat_members SET role = 'owner' WHERE chat_id = ? AND user_id = ?",
                        (ch["id"], new_owner["user_id"]),
                    )
            else:
                conn.execute("DELETE FROM chats WHERE id = ?", (ch["id"],))

        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM chat_members WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.close()

    # Close active ws sessions for this account.
    for ws in active_connections.get(user_id, set()).copy():
        try:
            await ws.close(code=1000)
        except Exception:
            pass
    active_connections.pop(user_id, None)
    return {"ok": True}


@app.get("/api/me")
async def me(user=Depends(get_current_user)):
    return serialize_user(user)


@app.post("/api/account/password")
async def change_password(data: PasswordChangeIn, user=Depends(get_current_user)):
    if len(data.new_password or "") < 6:
        raise HTTPException(status_code=400, detail="Новый пароль слишком короткий")

    conn = get_db()
    fresh = conn.execute("SELECT id, password_hash FROM users WHERE id = ?", (user["id"],)).fetchone()
    if not fresh or not verify_password(data.old_password, fresh["password_hash"]):
        conn.close()
        raise HTTPException(status_code=400, detail="Старый пароль неверный")

    with db_lock, conn:
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (hash_password(data.new_password), user["id"]),
        )
    conn.close()
    return {"ok": True}

@app.get("/api/settings")
async def get_my_settings(user=Depends(get_current_user)):
    conn = get_db()
    settings = get_settings(conn, user["id"])
    conn.close()
    return settings


@app.post("/api/settings")
async def update_settings(data: SettingsIn, user=Depends(get_current_user)):
    payload = data.model_dump()
    for key, value in payload.items():
        if value not in VALID_SETTING_VALUES[key]:
            raise HTTPException(status_code=400, detail=f"Неверное значение {key}")

    conn = get_db()
    with db_lock, conn:
        ensure_settings(conn, user["id"])
        conn.execute(
            """
            UPDATE user_settings
            SET allow_friend_requests = ?, allow_calls_from = ?, allow_group_invites = ?, show_last_seen = ?
            WHERE user_id = ?
            """,
            (
                payload["allow_friend_requests"],
                payload["allow_calls_from"],
                payload["allow_group_invites"],
                payload["show_last_seen"],
                user["id"],
            ),
        )
        settings = get_settings(conn, user["id"])
    conn.close()
    return settings


@app.get("/api/blocks")
async def list_blocks(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT u.id, u.username, u.nickname, u.avatar
        FROM blocked_users b
        JOIN users u ON u.id = b.blocked_id
        WHERE b.blocker_id = ?
        ORDER BY b.created_at DESC
        """,
        (user["id"],),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/users/{target_id}/block")
async def block_user(target_id: int, user=Depends(get_current_user)):
    if target_id == user["id"]:
        raise HTTPException(status_code=400, detail="Нельзя блокировать себя")
    conn = get_db()
    target = conn.execute("SELECT id FROM users WHERE id = ?", (target_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    with db_lock, conn:
        conn.execute(
            "INSERT OR IGNORE INTO blocked_users(blocker_id, blocked_id, created_at) VALUES (?, ?, ?)",
            (user["id"], target_id, now_iso()),
        )
        conn.execute(
            "DELETE FROM friends WHERE (user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)",
            (user["id"], target_id, target_id, user["id"]),
        )
        conn.execute(
            "DELETE FROM friend_requests WHERE (from_user_id = ? AND to_user_id = ?) OR (from_user_id = ? AND to_user_id = ?)",
            (user["id"], target_id, target_id, user["id"]),
        )
    conn.close()
    await push_to_user(target_id, {"type": "user:blocked", "payload": {"by": user["id"]}})
    return {"ok": True}


@app.delete("/api/users/{target_id}/block")
async def unblock_user(target_id: int, user=Depends(get_current_user)):
    conn = get_db()
    with db_lock, conn:
        conn.execute(
            "DELETE FROM blocked_users WHERE blocker_id = ? AND blocked_id = ?",
            (user["id"], target_id),
        )
    conn.close()
    return {"ok": True}


@app.post("/api/profile")
async def update_profile(data: ProfileIn, user=Depends(get_current_user)):
    nickname = data.nickname.strip()
    if not nickname:
        raise HTTPException(status_code=400, detail="Ник не может быть пустым")
    conn = get_db()
    with db_lock, conn:
        conn.execute(
            "UPDATE users SET nickname = ?, about = ? WHERE id = ?",
            (nickname, data.about.strip()[:250], user["id"]),
        )
        updated = conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()
    conn.close()
    return serialize_user(updated)


@app.post("/api/profile/avatar")
async def upload_avatar(file: UploadFile = File(...), user=Depends(get_current_user)):
    ext = Path(file.filename or "avatar.png").suffix or ".png"
    name = f"avatar_{user['id']}_{uuid.uuid4().hex}{ext}"
    target = UPLOAD_DIR / name
    content = await file.read()
    if len(content) > 7 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Файл до 7MB")
    target.write_bytes(content)

    conn = get_db()
    with db_lock, conn:
        conn.execute("UPDATE users SET avatar = ? WHERE id = ?", (name, user["id"]))
        updated = conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()
    conn.close()
    return serialize_user(updated)


@app.get("/api/users/search")
async def search_users(q: str = "", user=Depends(get_current_user)):
    q = q.strip().lower()
    if len(q) < 2:
        return []
    conn = get_db()
    rows = conn.execute(
        """
        SELECT id, username, nickname, avatar, about
        FROM users
        WHERE id != ?
          AND (username LIKE ? OR nickname LIKE ?)
          AND NOT EXISTS (
            SELECT 1 FROM blocked_users b
            WHERE (b.blocker_id = ? AND b.blocked_id = users.id)
               OR (b.blocker_id = users.id AND b.blocked_id = ?)
          )
        LIMIT 20
        """,
        (user["id"], f"%{q}%", f"%{q}%", user["id"], user["id"]),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/assets")
async def list_assets(kind: str = "", user=Depends(get_current_user)):
    conn = get_db()
    params: list = [user["id"]]
    query = """
        SELECT id, kind, title, file_path, file_name, mime_type, created_at
        FROM custom_assets
        WHERE user_id = ?
    """
    if kind in {"emoji", "sticker"}:
        query += " AND kind = ?"
        params.append(kind)
    query += " ORDER BY id DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "kind": r["kind"],
            "title": r["title"] or "",
            "file_url": f"/uploads/{r['file_path']}",
            "file_name": r["file_name"],
            "mime_type": r["mime_type"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


@app.post("/api/assets")
async def upload_asset(
    kind: str = Form(...),
    title: str = Form(""),
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
    if kind not in {"emoji", "sticker"}:
        raise HTTPException(status_code=400, detail="kind должен быть emoji или sticker")
    payload = await file.read()
    max_size = 2 * 1024 * 1024 if kind == "emoji" else 6 * 1024 * 1024
    if len(payload) > max_size:
        raise HTTPException(status_code=400, detail=f"Слишком большой файл (до {max_size // (1024*1024)}MB)")

    ext = Path(file.filename or "asset.bin").suffix
    safe_name = f"asset_{user['id']}_{uuid.uuid4().hex}{ext}"
    (UPLOAD_DIR / safe_name).write_bytes(payload)

    conn = get_db()
    with db_lock, conn:
        cur = conn.execute(
            """
            INSERT INTO custom_assets(user_id, kind, title, file_path, file_name, mime_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user["id"], kind, title.strip()[:40], safe_name, file.filename, file.content_type, now_iso()),
        )
        aid = cur.lastrowid
        row = conn.execute(
            "SELECT id, kind, title, file_path, file_name, mime_type, created_at FROM custom_assets WHERE id = ?",
            (aid,),
        ).fetchone()
    conn.close()
    return {
        "id": row["id"],
        "kind": row["kind"],
        "title": row["title"] or "",
        "file_url": f"/uploads/{row['file_path']}",
        "file_name": row["file_name"],
        "mime_type": row["mime_type"],
        "created_at": row["created_at"],
    }


@app.delete("/api/assets/{asset_id}")
async def delete_asset(asset_id: int, user=Depends(get_current_user)):
    conn = get_db()
    row = conn.execute(
        "SELECT id, file_path FROM custom_assets WHERE id = ? AND user_id = ?",
        (asset_id, user["id"]),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Стикер/эмодзи не найден")
    with db_lock, conn:
        conn.execute("DELETE FROM custom_assets WHERE id = ?", (asset_id,))
    conn.close()
    return {"ok": True}


@app.post("/api/friends/request")
async def send_friend_request(data: FriendRequestIn, user=Depends(get_current_user)):
    username = data.username.strip().lower()
    conn = get_db()
    target = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    if target["id"] == user["id"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Нельзя добавить себя")
    if is_any_block(conn, user["id"], target["id"]):
        conn.close()
        raise HTTPException(status_code=403, detail="Нельзя отправить заявку из-за блокировки")

    target_settings = get_settings(conn, target["id"])
    if target_settings["allow_friend_requests"] == "nobody":
        conn.close()
        raise HTTPException(status_code=403, detail="Пользователь запретил заявки в друзья")

    with db_lock, conn:
        existing_friend = conn.execute(
            "SELECT 1 FROM friends WHERE user_id = ? AND friend_id = ?",
            (user["id"], target["id"]),
        ).fetchone()
        if existing_friend:
            conn.close()
            return {"ok": True, "message": "Уже в друзьях"}
        conn.execute(
            """
            INSERT OR IGNORE INTO friend_requests(from_user_id, to_user_id, status, created_at)
            VALUES (?, ?, 'pending', ?)
            """,
            (user["id"], target["id"], now_iso()),
        )

    req = conn.execute(
        """
        SELECT fr.id, u.username, u.nickname, u.avatar
        FROM friend_requests fr
        JOIN users u ON u.id = fr.from_user_id
        WHERE fr.from_user_id = ? AND fr.to_user_id = ? AND fr.status = 'pending'
        """,
        (user["id"], target["id"]),
    ).fetchone()
    conn.close()
    if req:
        await push_to_user(target["id"], {"type": "friend:request", "payload": dict(req)})
    return {"ok": True}


@app.get("/api/friends/requests")
async def incoming_requests(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT fr.id, fr.created_at, u.id as user_id, u.username, u.nickname, u.avatar
        FROM friend_requests fr
        JOIN users u ON u.id = fr.from_user_id
        WHERE fr.to_user_id = ? AND fr.status = 'pending'
        ORDER BY fr.id DESC
        """,
        (user["id"],),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/friends/request/{request_id}/accept")
async def accept_request(request_id: int, user=Depends(get_current_user)):
    conn = get_db()
    req = conn.execute(
        "SELECT * FROM friend_requests WHERE id = ? AND to_user_id = ? AND status = 'pending'",
        (request_id, user["id"]),
    ).fetchone()
    if not req:
        conn.close()
        raise HTTPException(status_code=404, detail="Заявка не найдена")
    if is_any_block(conn, user["id"], req["from_user_id"]):
        conn.close()
        raise HTTPException(status_code=403, detail="Нельзя принять заявку из-за блокировки")

    with db_lock, conn:
        conn.execute("UPDATE friend_requests SET status = 'accepted' WHERE id = ?", (request_id,))
        conn.execute(
            "INSERT OR IGNORE INTO friends(user_id, friend_id, created_at) VALUES (?, ?, ?)",
            (user["id"], req["from_user_id"], now_iso()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO friends(user_id, friend_id, created_at) VALUES (?, ?, ?)",
            (req["from_user_id"], user["id"], now_iso()),
        )
    conn.close()

    await push_to_user(req["from_user_id"], {"type": "friend:accepted", "payload": {"by": user["username"]}})
    return {"ok": True}


@app.post("/api/friends/request/{request_id}/reject")
async def reject_request(request_id: int, user=Depends(get_current_user)):
    conn = get_db()
    with db_lock, conn:
        conn.execute(
            "UPDATE friend_requests SET status = 'rejected' WHERE id = ? AND to_user_id = ?",
            (request_id, user["id"]),
        )
    conn.close()
    return {"ok": True}


@app.get("/api/friends")
async def list_friends(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT u.id, u.username, u.nickname, u.avatar, u.about
        FROM friends f
        JOIN users u ON u.id = f.friend_id
        WHERE f.user_id = ?
          AND NOT EXISTS (
            SELECT 1 FROM blocked_users b
            WHERE (b.blocker_id = ? AND b.blocked_id = f.friend_id)
               OR (b.blocker_id = f.friend_id AND b.blocked_id = ?)
          )
        ORDER BY u.nickname
        """,
        (user["id"], user["id"], user["id"]),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/chats/direct")
async def open_direct(data: DirectIn, user=Depends(get_current_user)):
    conn = get_db()
    if is_any_block(conn, user["id"], data.user_id):
        conn.close()
        raise HTTPException(status_code=403, detail="Чат недоступен из-за блокировки")
    friend = conn.execute(
        "SELECT 1 FROM friends WHERE user_id = ? AND friend_id = ?",
        (user["id"], data.user_id),
    ).fetchone()
    if not friend:
        conn.close()
        raise HTTPException(status_code=403, detail="Только для друзей")

    existing = conn.execute(
        """
        SELECT c.id
        FROM chats c
        JOIN chat_members m1 ON m1.chat_id = c.id AND m1.user_id = ?
        JOIN chat_members m2 ON m2.chat_id = c.id AND m2.user_id = ?
        WHERE c.type = 'direct'
        """,
        (user["id"], data.user_id),
    ).fetchone()

    if existing:
        conn.close()
        return {"chat_id": existing["id"]}

    with db_lock, conn:
        cursor = conn.execute(
            "INSERT INTO chats(type, title, created_by, created_at) VALUES ('direct', NULL, ?, ?)",
            (user["id"], now_iso()),
        )
        chat_id = cursor.lastrowid
        conn.execute(
            "INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)",
            (chat_id, user["id"], now_iso()),
        )
        conn.execute(
            "INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)",
            (chat_id, data.user_id, now_iso()),
        )
    conn.close()
    return {"chat_id": chat_id}


@app.post("/api/groups")
async def create_group(data: GroupIn, user=Depends(get_current_user)):
    title = data.title.strip()[:80]
    if len(title) < 2:
        raise HTTPException(status_code=400, detail="Название группы слишком короткое")

    conn = get_db()
    member_ids = {m for m in data.members if isinstance(m, int)}
    member_ids.add(user["id"])
    final_members = {user["id"]}

    for uid in member_ids:
        if uid == user["id"]:
            continue
        if not is_friend(conn, user["id"], uid):
            continue
        allowed, _ = can_invite_to_group(conn, user["id"], uid)
        if allowed:
            final_members.add(uid)

    with db_lock, conn:
        cursor = conn.execute(
            "INSERT INTO chats(type, title, created_by, created_at) VALUES ('group', ?, ?, ?)",
            (title, user["id"], now_iso()),
        )
        chat_id = cursor.lastrowid
        for uid in final_members:
            conn.execute(
                "INSERT OR IGNORE INTO chat_members(chat_id, user_id, role, joined_at) VALUES (?, ?, ?, ?)",
                (chat_id, uid, "owner" if uid == user["id"] else "member", now_iso()),
            )
    conn.close()
    return {"chat_id": chat_id}


@app.delete("/api/groups/{chat_id}")
async def delete_group(chat_id: int, user=Depends(get_current_user)):
    conn = get_db()
    chat = get_chat(conn, chat_id)
    if not chat or chat["type"] != "group":
        conn.close()
        raise HTTPException(status_code=404, detail="Группа не найдена")
    if chat["created_by"] != user["id"]:
        conn.close()
        raise HTTPException(status_code=403, detail="Удалить группу может только создатель")

    members = conn.execute("SELECT user_id FROM chat_members WHERE chat_id = ?", (chat_id,)).fetchall()
    with db_lock, conn:
        conn.execute("DELETE FROM chats WHERE id = ?", (chat_id,))
    conn.close()

    for m in members:
        await push_to_user(m["user_id"], {"type": "group:deleted", "payload": {"chat_id": chat_id}})
    return {"ok": True}


@app.post("/api/groups/{chat_id}/invite")
async def invite_group_member(chat_id: int, data: DirectIn, user=Depends(get_current_user)):
    conn = get_db()
    chat = get_chat(conn, chat_id)
    if not chat or chat["type"] != "group":
        conn.close()
        raise HTTPException(status_code=404, detail="Группа не найдена")

    role = conn.execute(
        "SELECT role FROM chat_members WHERE chat_id = ? AND user_id = ?",
        (chat_id, user["id"]),
    ).fetchone()
    if not role:
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа")
    if role["role"] not in {"owner", "admin"}:
        conn.close()
        raise HTTPException(status_code=403, detail="Приглашать могут owner/admin")

    already = conn.execute(
        "SELECT 1 FROM chat_members WHERE chat_id = ? AND user_id = ?",
        (chat_id, data.user_id),
    ).fetchone()
    if already:
        conn.close()
        return {"ok": True, "message": "Уже в группе"}

    if not is_friend(conn, user["id"], data.user_id):
        conn.close()
        raise HTTPException(status_code=400, detail="Можно пригласить только друга")

    allowed, reason = can_invite_to_group(conn, user["id"], data.user_id)
    if not allowed:
        conn.close()
        raise HTTPException(status_code=403, detail=reason)

    with db_lock, conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_members(chat_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)",
            (chat_id, data.user_id, now_iso()),
        )
    conn.close()

    await push_to_user(data.user_id, {"type": "chat:added", "payload": {"chat_id": chat_id}})
    await broadcast_to_chat(chat_id, {"type": "group:member_added", "payload": {"chat_id": chat_id, "user_id": data.user_id}})
    return {"ok": True}


@app.get("/api/chats")
async def get_chats(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT c.id, c.type, c.title, c.created_by,
               (SELECT m.text FROM messages m WHERE m.chat_id = c.id ORDER BY m.id DESC LIMIT 1) as last_text,
               (SELECT m.created_at FROM messages m WHERE m.chat_id = c.id ORDER BY m.id DESC LIMIT 1) as last_at
        FROM chats c
        JOIN chat_members cm ON cm.chat_id = c.id
        WHERE cm.user_id = ?
        ORDER BY COALESCE(last_at, c.created_at) DESC
        """,
        (user["id"],),
    ).fetchall()

    items = []
    for r in rows:
        item = dict(r)
        if item["type"] == "direct":
            peer = conn.execute(
                """
                SELECT u.id, u.username, u.nickname, u.avatar
                FROM chat_members cm
                JOIN users u ON u.id = cm.user_id
                WHERE cm.chat_id = ? AND cm.user_id != ?
                LIMIT 1
                """,
                (item["id"], user["id"]),
            ).fetchone()
            if not peer:
                continue
            if is_any_block(conn, user["id"], peer["id"]):
                continue
            item["title"] = peer["nickname"]
            item["peer"] = dict(peer)
            can_call, _ = can_call_user(conn, user["id"], peer["id"])
            item["can_call"] = can_call
        else:
            item["can_call"] = True
            item["can_delete"] = item["created_by"] == user["id"]
        items.append(item)

    conn.close()
    return items


@app.get("/api/chats/{chat_id}/members")
async def chat_members(chat_id: int, user=Depends(get_current_user)):
    conn = get_db()
    if not can_access_chat(conn, user["id"], chat_id):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа")
    rows = conn.execute(
        """
        SELECT u.id, u.username, u.nickname, u.avatar, cm.role
        FROM chat_members cm
        JOIN users u ON u.id = cm.user_id
        WHERE cm.chat_id = ?
        ORDER BY CASE cm.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, u.nickname
        """,
        (chat_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/chats/{chat_id}/messages")
async def chat_messages(chat_id: int, limit: int = 100, user=Depends(get_current_user)):
    conn = get_db()
    if not can_access_chat(conn, user["id"], chat_id):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа")
    limit = min(max(limit, 1), 200)
    rows = conn.execute(
        """
        SELECT m.*, u.username, u.nickname, u.avatar
        FROM messages m
        JOIN users u ON u.id = m.user_id
        WHERE m.chat_id = ?
          AND NOT EXISTS (
            SELECT 1 FROM message_deleted_for d
            WHERE d.message_id = m.id AND d.user_id = ?
          )
        ORDER BY m.id DESC
        LIMIT ?
        """,
        (chat_id, user["id"], limit),
    ).fetchall()
    conn.close()
    return [serialize_message(r) for r in reversed(rows)]


@app.post("/api/chats/{chat_id}/messages")
async def send_message(
    chat_id: int,
    text: str = Form(""),
    kind: str = Form("text"),
    file: Optional[UploadFile] = File(None),
    user=Depends(get_current_user),
):
    if kind not in {"text", "image", "video", "voice", "file", "circle", "emoji", "sticker"}:
        kind = "file"

    conn = get_db()
    chat = get_chat(conn, chat_id)
    if not chat or not can_access_chat(conn, user["id"], chat_id):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа")

    if chat["type"] == "direct":
        peer = get_direct_peer(conn, chat_id, user["id"])
        if not peer or is_any_block(conn, user["id"], peer["id"]):
            conn.close()
            raise HTTPException(status_code=403, detail="Нельзя писать в этот чат")

    file_path = None
    file_name = None
    mime_type = None

    if file:
        ext = Path(file.filename or "file.bin").suffix
        safe_name = f"{uuid.uuid4().hex}{ext}"
        payload = await file.read()
        if len(payload) > 50 * 1024 * 1024:
            conn.close()
            raise HTTPException(status_code=400, detail="Файл до 50MB")
        (UPLOAD_DIR / safe_name).write_bytes(payload)
        file_path = safe_name
        file_name = file.filename
        mime_type = file.content_type

    if not text.strip() and not file_path:
        conn.close()
        raise HTTPException(status_code=400, detail="Пустое сообщение")

    with db_lock, conn:
        cursor = conn.execute(
            """
            INSERT INTO messages(chat_id, user_id, kind, text, file_path, file_name, mime_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (chat_id, user["id"], kind, text.strip(), file_path, file_name, mime_type, now_iso()),
        )
        msg_id = cursor.lastrowid

    row = conn.execute(
        """
        SELECT m.*, u.username, u.nickname, u.avatar
        FROM messages m
        JOIN users u ON u.id = m.user_id
        WHERE m.id = ?
        """,
        (msg_id,),
    ).fetchone()
    conn.close()
    data = serialize_message(row)
    await broadcast_to_chat(chat_id, {"type": "message:new", "payload": data})
    return data


@app.post("/api/chats/{chat_id}/messages/asset")
async def send_asset_message(chat_id: int, asset_id: int = Form(...), text: str = Form(""), user=Depends(get_current_user)):
    conn = get_db()
    chat = get_chat(conn, chat_id)
    if not chat or not can_access_chat(conn, user["id"], chat_id):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа")
    if chat["type"] == "direct":
        peer = get_direct_peer(conn, chat_id, user["id"])
        if not peer or is_any_block(conn, user["id"], peer["id"]):
            conn.close()
            raise HTTPException(status_code=403, detail="Нельзя писать в этот чат")

    asset = conn.execute(
        """
        SELECT id, kind, file_path, file_name, mime_type
        FROM custom_assets
        WHERE id = ? AND user_id = ?
        """,
        (asset_id, user["id"]),
    ).fetchone()
    if not asset:
        conn.close()
        raise HTTPException(status_code=404, detail="Стикер/эмодзи не найден")

    with db_lock, conn:
        cur = conn.execute(
            """
            INSERT INTO messages(chat_id, user_id, kind, text, file_path, file_name, mime_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chat_id,
                user["id"],
                asset["kind"],
                text.strip(),
                asset["file_path"],
                asset["file_name"],
                asset["mime_type"],
                now_iso(),
            ),
        )
        msg_id = cur.lastrowid

    row = conn.execute(
        """
        SELECT m.*, u.username, u.nickname, u.avatar
        FROM messages m
        JOIN users u ON u.id = m.user_id
        WHERE m.id = ?
        """,
        (msg_id,),
    ).fetchone()
    conn.close()
    data = serialize_message(row)
    await broadcast_to_chat(chat_id, {"type": "message:new", "payload": data})
    return data


@app.delete("/api/messages/{message_id}")
async def delete_message(message_id: int, mode: str = "me", user=Depends(get_current_user)):
    if mode not in {"me", "all"}:
        raise HTTPException(status_code=400, detail="mode должен быть me или all")

    conn = get_db()
    row = conn.execute(
        "SELECT id, chat_id, user_id FROM messages WHERE id = ?",
        (message_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Сообщение не найдено")

    chat_id = row["chat_id"]
    if not can_access_chat(conn, user["id"], chat_id):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа")

    if mode == "me":
        with db_lock, conn:
            conn.execute(
                "INSERT OR IGNORE INTO message_deleted_for(message_id, user_id, created_at) VALUES (?, ?, ?)",
                (message_id, user["id"], now_iso()),
            )
        conn.close()
        await push_to_user(
            user["id"],
            {"type": "message:deleted_me", "payload": {"chat_id": chat_id, "message_id": message_id}},
        )
        return {"ok": True}

    if row["user_id"] != user["id"]:
        conn.close()
        raise HTTPException(status_code=403, detail="Удалять у всех может только автор сообщения")

    with db_lock, conn:
        conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
    conn.close()
    await broadcast_to_chat(
        chat_id,
        {"type": "message:deleted_all", "payload": {"chat_id": chat_id, "message_id": message_id}},
    )
    return {"ok": True}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    token = ws.query_params.get("token", "")
    user = get_user_by_token(token)
    if not user:
        await ws.close(code=1008)
        return

    user_id = user["id"]
    await ws.accept()
    active_connections.setdefault(user_id, set()).add(ws)

    try:
        await ws.send_json({"type": "hello", "payload": {"user_id": user_id}})
        while True:
            msg = await ws.receive_json()
            msg_type = msg.get("type")

            if msg_type == "ping":
                await ws.send_json({"type": "pong"})
                continue

            if msg_type == "call:join":
                chat_id = int(msg.get("chat_id", 0))
                conn = get_db()
                chat = get_chat(conn, chat_id)
                if not chat or not can_access_chat(conn, user_id, chat_id):
                    conn.close()
                    continue

                if chat["type"] == "direct":
                    peer = get_direct_peer(conn, chat_id, user_id)
                    if not peer:
                        conn.close()
                        continue
                    allowed, _ = can_call_user(conn, user_id, peer["id"])
                    if not allowed:
                        conn.close()
                        continue

                room = call_rooms.setdefault(chat_id, {})
                room[user_id] = ws
                state = {
                    "mic": bool(msg.get("mic", True)),
                    "cam": bool(msg.get("cam", False)),
                    "screen": bool(msg.get("screen", False)),
                }
                call_states.setdefault(chat_id, {})[user_id] = state
                others = [uid for uid in room.keys() if uid != user_id]
                states = call_states.get(chat_id, {})
                conn.close()

                await ws.send_json(
                    {
                        "type": "call:participants",
                        "payload": {"chat_id": chat_id, "users": others, "states": states},
                    }
                )
                for uid, peer_ws in room.items():
                    if uid != user_id:
                        await peer_ws.send_json(
                            {
                                "type": "call:user_joined",
                                "payload": {"chat_id": chat_id, "user_id": user_id, "state": state},
                            }
                        )
                continue

            if msg_type == "call:leave":
                chat_id = int(msg.get("chat_id", 0))
                room = call_rooms.get(chat_id, {})
                if user_id in room:
                    room.pop(user_id, None)
                    call_states.get(chat_id, {}).pop(user_id, None)
                    for peer_ws in room.values():
                        await peer_ws.send_json(
                            {
                                "type": "call:user_left",
                                "payload": {"chat_id": chat_id, "user_id": user_id},
                            }
                        )
                    if not room:
                        call_rooms.pop(chat_id, None)
                        call_states.pop(chat_id, None)
                continue

            if msg_type == "call:state":
                chat_id = int(msg.get("chat_id", 0))
                room = call_rooms.get(chat_id, {})
                if user_id not in room:
                    continue
                state = {
                    "mic": bool(msg.get("mic", True)),
                    "cam": bool(msg.get("cam", False)),
                    "screen": bool(msg.get("screen", False)),
                }
                call_states.setdefault(chat_id, {})[user_id] = state
                for uid, peer_ws in room.items():
                    if uid != user_id:
                        await peer_ws.send_json(
                            {
                                "type": "call:user_state",
                                "payload": {"chat_id": chat_id, "user_id": user_id, "state": state},
                            }
                        )
                continue

            if msg_type == "call:signal":
                chat_id = int(msg.get("chat_id", 0))
                to_user = int(msg.get("to_user", 0))
                room = call_rooms.get(chat_id, {})
                target_ws = room.get(to_user)
                if target_ws:
                    await target_ws.send_json(
                        {
                            "type": "call:signal",
                            "payload": {
                                "chat_id": chat_id,
                                "from_user": user_id,
                                "signal": msg.get("signal"),
                            },
                        }
                    )
                continue

    except WebSocketDisconnect:
        pass
    finally:
        active_connections.get(user_id, set()).discard(ws)
        for chat_id in list(call_rooms.keys()):
            room = call_rooms[chat_id]
            if room.get(user_id) is ws:
                room.pop(user_id, None)
                call_states.get(chat_id, {}).pop(user_id, None)
                for peer_ws in room.values():
                    try:
                        await peer_ws.send_json(
                            {
                                "type": "call:user_left",
                                "payload": {"chat_id": chat_id, "user_id": user_id},
                            }
                        )
                    except Exception:
                        pass
            if not room:
                call_rooms.pop(chat_id, None)
                call_states.pop(chat_id, None)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host=host, port=port, reload=True)
