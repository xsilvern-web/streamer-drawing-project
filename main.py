import os
import asyncio
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import uvicorn

load_dotenv()

TOSS_SECRET_KEY = os.getenv("TOSS_SECRET_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

app = FastAPI()

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

active_connections = []
drawing_queue = asyncio.Queue()

# ✨ DB 파일 경로 (클라우드타입 영구 저장소 경로를 위해 ./data/ 폴더 사용 권장, 아래 2단계 참고)
DB_FILE = "donation_ledger_v2.db"

# --- DB 초기화 (장부 + 설정 테이블) ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # 1. 후원 장부 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            donor_email TEXT,
            donor_name TEXT NOT NULL,
            drawing_title TEXT,
            amount INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            drawing_data TEXT,
            is_played BOOLEAN DEFAULT FALSE
        )
    ''')
    # ✨ 2. 크리에이터 설정 테이블 (메모리가 아닌 DB에 영구 저장)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY DEFAULT 1,
            is_donation_enabled BOOLEAN DEFAULT 1,
            min_amount INTEGER DEFAULT 1000,
            blocked_emails TEXT DEFAULT '[]'
        )
    ''')
    # 설정이 아예 없으면 기본값 한 줄 생성
    cursor.execute("INSERT OR IGNORE INTO settings (id) VALUES (1)")
    conn.commit()
    conn.close()

init_db()

# --- DB에서 설정값 불러오기/저장하기 도우미 함수 ---
def get_db_settings():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.cursor().execute("SELECT * FROM settings WHERE id = 1").fetchone()
    conn.close()
    return {
        "is_donation_enabled": bool(row["is_donation_enabled"]),
        "min_amount": row["min_amount"],
        "blocked_emails": json.loads(row["blocked_emails"])
    }

def update_db_settings(is_enabled=None, min_amount=None, blocked_emails=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if is_enabled is not None:
        cursor.execute("UPDATE settings SET is_donation_enabled = ? WHERE id = 1", (int(is_enabled),))
    if min_amount is not None:
        cursor.execute("UPDATE settings SET min_amount = ? WHERE id = 1", (min_amount,))
    if blocked_emails is not None:
        cursor.execute("UPDATE settings SET blocked_emails = ? WHERE id = 1", (json.dumps(blocked_emails),))
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

# --- 크리에이터 설정 API (DB 연동) ---
@app.get("/api/settings")
async def get_settings(): 
    return get_db_settings()

@app.post("/api/toggle-donation")
async def toggle_donation(enable: bool):
    update_db_settings(is_enabled=enable)
    return {"message": "success"}

class SettingsUpdate(BaseModel):
    min_amount: int = None
    add_blocked_email: str = None
    remove_blocked_email: str = None

@app.post("/api/update-settings")
async def update_settings(data: SettingsUpdate):
    current_settings = get_db_settings()
    
    if data.min_amount is not None: 
        update_db_settings(min_amount=data.min_amount)
        
    blocked = current_settings["blocked_emails"]
    changed = False
    if data.add_blocked_email and data.add_blocked_email not in blocked:
        blocked.append(data.add_blocked_email)
        changed = True
    if data.remove_blocked_email and data.remove_blocked_email in blocked:
        blocked.remove(data.remove_blocked_email)
        changed = True
        
    if changed:
        update_db_settings(blocked_emails=blocked)
        
    return {"message": "success"}

# --- 최근 후원 내역 및 다시보기 API ---
@app.get("/api/recent-donations")
async def get_recent_donations():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.cursor().execute("SELECT id, donor_name, drawing_title, amount, timestamp FROM ledger ORDER BY id DESC LIMIT 10").fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["donor_name"], "title": r["drawing_title"], "amount": r["amount"], "time": r["timestamp"]} for r in rows]

@app.post("/api/replay-donation/{ledger_id}")
async def replay_donation(ledger_id: int):
    conn = sqlite3.connect(DB_FILE)
    row = conn.cursor().execute("SELECT donor_name, drawing_title, amount, drawing_data FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
    conn.close()
    
    if not row: raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")
    await drawing_queue.put({"name": row[0], "title": row[1], "amount": row[2], "drawingData": json.loads(row[3])})
    return {"message": "success"}

# --- 데이터 수신 API ---
@app.post("/api/submit-drawing")
async def submit_drawing(request: Request):
    settings = get_db_settings()
    if not settings["is_donation_enabled"]:
        raise HTTPException(status_code=403, detail="현재 크리에이터가 후원을 닫아두었습니다.")

    try:
        data = await request.json()
        email = data.get("email") 
        name = data.get("name") 
        title = data.get("title", "제목없음")
        amount = int(data.get("amount", 0))
        drawing_history = data.get("drawingData")

        if not email: raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
        if email in settings["blocked_emails"]: raise HTTPException(status_code=403, detail="차단된 계정입니다.")
        if amount < settings["min_amount"]: raise HTTPException(status_code=400, detail=f"최소 후원 금액은 {settings['min_amount']}원 이상입니다.")

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ledger (donor_email, donor_name, drawing_title, amount, drawing_data) VALUES (?, ?, ?, ?, ?)",
            (email, name, title, amount, json.dumps(drawing_history))
        )
        conn.commit()
        conn.close()
        
        await drawing_queue.put(data)
        return {"status": "success"}
        
    except HTTPException as he: raise he
    except Exception as e:
        print(f"❌ 오류: {e}")
        raise HTTPException(status_code=500, detail="서버 오류 발생")

# --- OBS 전송 및 파기 스케줄러 ---
async def process_drawing_queue():
    while True:
        data = await drawing_queue.get()
        for connection in active_connections:
            try:
                await connection.send_json({"type": "alert", "name": data.get("name"), "title": data.get("title", ""), "amount": data.get("amount")})
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
                        await connection.send_json({"type": "draw_shape", "shape": item_type, "color": color, "width": item.get("width"), "start": item.get("start"), "end": item.get("end")})
                        await asyncio.sleep(0.2)
                await connection.send_json({"type": "done"})
            except Exception as e: pass
            
        await asyncio.sleep(8)
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