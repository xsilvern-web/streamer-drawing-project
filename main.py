import os
import asyncio
import sqlite3
import json
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn

CREATOR_PASSWORD = os.getenv("CREATOR_PASSWORD", "streamer777!")

app = FastAPI()

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

active_connections = []
drawing_queue = asyncio.Queue()

# ✨ 프로필 사진 칼럼이 추가된 새로운 장부(v4)
DB_FILE = "donation_ledger_v4.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            donor_email TEXT,
            donor_name TEXT NOT NULL,
            donor_profile_image TEXT,
            drawing_title TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            drawing_data TEXT,
            is_played BOOLEAN DEFAULT FALSE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY DEFAULT 1,
            is_donation_enabled BOOLEAN DEFAULT 1,
            blocked_emails TEXT DEFAULT '[]',
            display_duration INTEGER DEFAULT 8,
            daily_limit INTEGER DEFAULT 0
        )
    ''')
    cursor.execute("INSERT OR IGNORE INTO settings (id) VALUES (1)")
    
    # ✨ 기존 DB를 사용하는 경우를 위해 안전하게 새 컬럼 추가
    try:
        cursor.execute("ALTER TABLE settings ADD COLUMN display_duration INTEGER DEFAULT 8")
        cursor.execute("ALTER TABLE settings ADD COLUMN daily_limit INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # 이미 컬럼이 존재하면 무시합니다.

    conn.commit()
    conn.close()

init_db()

def get_db_settings():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.cursor().execute("SELECT * FROM settings WHERE id = 1").fetchone()
    conn.close()
    
    # row 객체를 딕셔너리로 안전하게 변환
    row_dict = dict(row)
    return {
        "is_donation_enabled": bool(row_dict.get("is_donation_enabled", 1)),
        "blocked_emails": json.loads(row_dict.get("blocked_emails", '[]')),
        "display_duration": row_dict.get("display_duration", 8),
        "daily_limit": row_dict.get("daily_limit", 0)
    }

def update_db_settings(is_enabled=None, blocked_emails=None, display_duration=None, daily_limit=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if is_enabled is not None:
        cursor.execute("UPDATE settings SET is_donation_enabled = ? WHERE id = 1", (int(is_enabled),))
    if blocked_emails is not None:
        cursor.execute("UPDATE settings SET blocked_emails = ? WHERE id = 1", (json.dumps(blocked_emails),))
    if display_duration is not None:
        cursor.execute("UPDATE settings SET display_duration = ? WHERE id = 1", (display_duration,))
    if daily_limit is not None:
        cursor.execute("UPDATE settings SET daily_limit = ? WHERE id = 1", (daily_limit,))
    conn.commit()
    conn.close()

@app.get("/draw")
async def serve_draw_page(): return FileResponse("draw.html")

@app.get("/")
@app.get("/index")
async def serve_index_page(): return FileResponse("index.html")

@app.get("/creator")
async def serve_creator_page(): return FileResponse("creator.html")

@app.get("/coin.mp3")
async def serve_coin_sound(): return FileResponse("coin.mp3")

class PasswordCheck(BaseModel):
    password: str

@app.post("/api/verify-password")
async def verify_password(data: PasswordCheck):
    if data.password == CREATOR_PASSWORD:
        return {"valid": True}
    raise HTTPException(status_code=401, detail="비밀번호가 틀렸습니다.")

@app.get("/api/settings")
async def get_settings(): 
    return get_db_settings()

@app.post("/api/toggle-donation")
async def toggle_donation(enable: bool):
    update_db_settings(is_enabled=enable)
    return {"message": "success"}

# ✨ 설정 업데이트 모델에 노출 시간과 일일 제한 변수 추가
class SettingsUpdate(BaseModel):
    add_blocked_email: str = None
    remove_blocked_email: str = None
    display_duration: int = None
    daily_limit: int = None

@app.post("/api/update-settings")
async def update_settings(data: SettingsUpdate):
    current_settings = get_db_settings()
    blocked = current_settings["blocked_emails"]
    changed = False
    
    if data.add_blocked_email and data.add_blocked_email not in blocked:
        blocked.append(data.add_blocked_email)
        changed = True
    if data.remove_blocked_email and data.remove_blocked_email in blocked:
        blocked.remove(data.remove_blocked_email)
        changed = True
        
    update_db_settings(
        blocked_emails=blocked if changed else None,
        display_duration=data.display_duration,
        daily_limit=data.daily_limit
    )
    return {"message": "success"}

@app.get("/api/recent-donations")
async def get_recent_donations():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.cursor().execute("SELECT id, donor_name, donor_email, drawing_title, timestamp FROM ledger ORDER BY id DESC LIMIT 10").fetchall()
    conn.close()
    return [
        {
            "id": r["id"], 
            "name": r["donor_name"], 
            "email": r["donor_email"],
            "title": r["drawing_title"], 
            "time": r["timestamp"]
        } for r in rows
    ]

@app.post("/api/replay-donation/{ledger_id}")
async def replay_donation(ledger_id: int):
    conn = sqlite3.connect(DB_FILE)
    row = conn.cursor().execute("SELECT donor_name, donor_profile_image, drawing_title, drawing_data FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
    conn.close()
    
    if not row: raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")
    await drawing_queue.put({
        "name": row[0], 
        "profileImage": row[1], 
        "title": row[2], 
        "drawingData": json.loads(row[3])
    })
    return {"message": "success"}

@app.post("/api/submit-drawing")
async def submit_drawing(request: Request):
    settings = get_db_settings()
    if not settings["is_donation_enabled"]:
        raise HTTPException(status_code=403, detail="현재 그림 받기가 닫혀있습니다.")

    # ✨ 새벽 6시 기점 일일 한도 체크 로직
    if settings.get("daily_limit", 0) > 0:
        now = datetime.now()
        # 현재 시간이 새벽 6시 이전이면, 초기화 기준일은 "어제 오전 6시"
        target_date = now - timedelta(days=1) if now.hour < 6 else now
        target_str = target_date.strftime('%Y-%m-%d 06:00:00')
        
        conn = sqlite3.connect(DB_FILE)
        count = conn.cursor().execute("SELECT COUNT(*) FROM ledger WHERE timestamp >= ?", (target_str,)).fetchone()[0]
        conn.close()
        
        if count >= settings["daily_limit"]:
            raise HTTPException(status_code=403, detail=f"오늘의 그림 받기 한도({settings['daily_limit']}개)가 초과되었습니다.")

    try:
        data = await request.json()
        email = data.get("email") 
        name = data.get("name")
        profile_image = data.get("profileImage", "")
        title = data.get("title", "제목없음")
        drawing_history = data.get("drawingData")

        if not email: raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
        if email in settings["blocked_emails"]: raise HTTPException(status_code=403, detail="차단된 계정입니다.")

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ledger (donor_email, donor_name, donor_profile_image, drawing_title, drawing_data) VALUES (?, ?, ?, ?, ?)",
            (email, name, profile_image, title, json.dumps(drawing_history))
        )
        conn.commit()
        conn.close()
        
        await drawing_queue.put(data)
        return {"status": "success"}
        
    except HTTPException as he: raise he
    except Exception as e:
        print(f"❌ 오류: {e}")
        raise HTTPException(status_code=500, detail="서버 오류 발생")

async def process_drawing_queue():
    while True:
        data = await drawing_queue.get()
        settings = get_db_settings()
        display_duration = settings.get("display_duration", 8)  # ✨ 설정된 노출 시간 적용 (기본값 8)
        
        for connection in active_connections:
            try:
                await connection.send_json({
                    "type": "alert", 
                    "name": data.get("name"), 
                    "profileImage": data.get("profileImage", ""),
                    "title": data.get("title", "")
                })
                await connection.send_json({"type": "clear"})
                for item in data.get("drawingData"):
                    item_type = item.get("type", "path")
                    color = item.get("color")
                    if item_type == "path":
                        points = item.get("points", [])
                        if not points: continue
                        await connection.send_json({"type": "start_path", "point": points[0], "color": color})
                        for i, point in enumerate(points[1:]):
                            await connection.send_json({"type": "draw_line", "point": point, "color": color})
                            if i % 5 == 0: await asyncio.sleep(0.01)
                    else:
                        payload = item.copy()
                        payload["type"] = "draw_shape"
                        payload["shape"] = item_type
                        await connection.send_json(payload)
                        await asyncio.sleep(0.2)
                await connection.send_json({"type": "done"})
            except Exception as e: pass
            
        # ✨ 제어판에서 입력한 노출 시간만큼 대기
        await asyncio.sleep(display_duration)
        
        for connection in active_connections:
            try: await connection.send_json({"type": "fade_out"})
            except: pass
        await asyncio.sleep(1.5)
        drawing_queue.task_done()

async def auto_delete_old_data():
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.cursor().execute("DELETE FROM ledger WHERE timestamp <= datetime('now', '-90 days')")
            conn.commit()
            conn.close()
        except: pass
        await asyncio.sleep(86400) 

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: pass
    finally: active_connections.remove(websocket)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_drawing_queue())
    asyncio.create_task(auto_delete_old_data())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)