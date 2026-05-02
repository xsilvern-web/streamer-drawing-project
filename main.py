import os
import asyncio
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv  # 환경변수 로드용
import uvicorn

# .env 파일이 있다면 읽어오고, 클라우드타입 환경변수도 자동으로 읽어옵니다.
load_dotenv()

# 🚨 하드코딩된 키 대신 환경변수에서 불러오기 (보안 적용)
TOSS_SECRET_KEY = os.getenv("TOSS_SECRET_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

app = FastAPI()

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

active_connections = []
drawing_queue = asyncio.Queue()

# ✨ 크리에이터 설정 (메모리 저장)
CREATOR_SETTINGS = {
    "is_donation_enabled": True,
    "min_amount": 1000,
    "blocked_emails": []  # 이메일 기반 차단
}

# --- 1. SQLite 데이터베이스 초기화 ---
DB_FILE = "donation_ledger_v2.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
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
    conn.commit()
    conn.close()

init_db()

# --- 2. 웹페이지 서빙 ---
@app.get("/draw")
async def serve_draw_page(): return FileResponse("draw.html")

@app.get("/")
@app.get("/index")
async def serve_index_page(): return FileResponse("index.html")

@app.get("/creator")
async def serve_creator_page(): return FileResponse("creator.html")

@app.get("/coin.mp3")
async def serve_coin_sound(): return FileResponse("coin.mp3")

# --- 3. 크리에이터 설정 API ---
@app.get("/api/settings")
async def get_settings(): return CREATOR_SETTINGS

@app.post("/api/toggle-donation")
async def toggle_donation(enable: bool):
    CREATOR_SETTINGS["is_donation_enabled"] = enable
    return {"message": "success"}

class SettingsUpdate(BaseModel):
    min_amount: int = None
    add_blocked_email: str = None

@app.post("/api/update-settings")
async def update_settings(data: SettingsUpdate):
    if data.min_amount is not None: CREATOR_SETTINGS["min_amount"] = data.min_amount
    if data.add_blocked_email and data.add_blocked_email not in CREATOR_SETTINGS["blocked_emails"]:
        CREATOR_SETTINGS["blocked_emails"].append(data.add_blocked_email)
    return {"message": "success"}

# --- 4. ✨ 데이터 수신 (로그인 이메일 검증) ---
@app.post("/api/submit-drawing")
async def submit_drawing(request: Request):
    if not CREATOR_SETTINGS["is_donation_enabled"]:
        raise HTTPException(status_code=403, detail="현재 크리에이터가 후원을 닫아두었습니다.")

    try:
        data = await request.json()
        email = data.get("email") # 구글 로그인 이메일
        name = data.get("name")
        title = data.get("title", "제목없음")
        amount = int(data.get("amount", 0))
        drawing_history = data.get("drawingData")

        if not email: raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
        if email in CREATOR_SETTINGS["blocked_emails"]: raise HTTPException(status_code=403, detail="차단된 계정입니다.")
        if amount < CREATOR_SETTINGS["min_amount"]: raise HTTPException(status_code=400, detail=f"최소 후원 금액은 {CREATOR_SETTINGS['min_amount']}원 이상입니다.")

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ledger (donor_email, donor_name, drawing_title, amount, drawing_data) VALUES (?, ?, ?, ?, ?)",
            (email, name, title, amount, json.dumps(drawing_history))
        )
        ledger_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        data['ledger_id'] = ledger_id
        await drawing_queue.put(data)
        return {"status": "success"}
        
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"❌ 오류: {e}")
        raise HTTPException(status_code=500, detail="서버 처리 중 오류가 발생했습니다.")

# --- 5. OBS 전송 및 자동 파기 ---
async def process_drawing_queue():
    while True:
        data = await drawing_queue.get()
        
        # 1. 시청자 화면에 그리기 시작
        for connection in active_connections:
            try:
                await connection.send_json({"type": "alert", "name": data.get("name"), "title": data.get("title", ""), "amount": data.get("amount")})
                await connection.send_json({"type": "clear"}) # 프론트에서 화면을 깨끗하게 지우고 투명도 원복
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
            
        # ✨ 2. 그림이 다 그려진 후 8초 동안 감상할 시간을 줍니다.
        await asyncio.sleep(8)
        
        # ✨ 3. 8초 뒤, 화면에서 서서히 사라지라는 명령을 보냅니다.
        for connection in active_connections:
            try:
                await connection.send_json({"type": "fade_out"})
            except: pass
            
        # ✨ 4. 서서히 사라지는 애니메이션이 끝날 때까지 1.5초 대기 후, 다음 후원 그림으로 순서를 넘깁니다.
        await asyncio.sleep(1.5)
        drawing_queue.task_done()

async def auto_delete_old_data():
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ledger WHERE timestamp <= datetime('now', '-90 days')")
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