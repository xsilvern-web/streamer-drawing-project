import os
import asyncio
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn

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
    "blocked_emails": []  # 닉네임 대신 이메일 리스트로 변경
}

# --- 1. SQLite 데이터베이스 초기화 ---
DB_FILE = "donation_ledger.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
async def get_settings():
    return CREATOR_SETTINGS

@app.post("/api/toggle-donation")
async def toggle_donation(enable: bool):
    CREATOR_SETTINGS["is_donation_enabled"] = enable
    return {"message": "success"}

class SettingsUpdate(BaseModel):
    min_amount: int = None
    add_blocked_email: str = None

@app.post("/api/update-settings")
async def update_settings(data: SettingsUpdate):
    if data.min_amount is not None:
        CREATOR_SETTINGS["min_amount"] = data.min_amount
    if data.add_blocked_email:
        if data.add_blocked_email not in CREATOR_SETTINGS["blocked_emails"]:
            CREATOR_SETTINGS["blocked_emails"].append(data.add_blocked_email)
    return {"message": "success"}

# --- 4. ✨ 데이터 수신 (차단/최소금액 검증 적용) ---
@app.post("/api/submit-drawing")
@app.post("/api/submit-drawing")
async def submit_drawing(request: Request):
    if not CREATOR_SETTINGS["is_donation_enabled"]:
        raise HTTPException(status_code=403, detail="현재 크리에이터가 후원을 닫아두었습니다.")

    try:
        data = await request.json()
        name = data.get("name")
        email = data.get("email") # ✨ 클라이언트로부터 이메일 수신
        title = data.get("title", "제목없음")
        amount = int(data.get("amount", 0))
        drawing_history = data.get("drawingData")

        if not email:
            raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

        # 🚨 방어 로직: 차단된 이메일 검증
        if email in CREATOR_SETTINGS["blocked_emails"]:
            raise HTTPException(status_code=403, detail="크리에이터에 의해 차단된 계정입니다.")
        if amount < CREATOR_SETTINGS["min_amount"]:
            raise HTTPException(status_code=400, detail=f"최소 후원 금액은 {CREATOR_SETTINGS['min_amount']}원 이상입니다.")
        if not name or not drawing_history:
            raise HTTPException(status_code=400, detail="필수 데이터가 누락되었습니다.")

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ledger (donor_name, drawing_title, amount, drawing_data) VALUES (?, ?, ?, ?)",
            (name, title, amount, json.dumps(drawing_history))
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

# --- 5. OBS 전송 및 ✨ 데이터 자동 파기(90일) 로직 ---
async def process_drawing_queue():
    while True:
        data = await drawing_queue.get()
        # ...(기존과 동일한 웹소켓 전송 로직)...
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
                
            except Exception as e:
                print(f"❌ OBS 전송 에러: {e}")
        await asyncio.sleep(5)
        drawing_queue.task_done()

# ✨ 90일 지난 데이터 매일 청소하는 백그라운드 작업
async def auto_delete_old_data():
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            # 90일(약 3개월)이 지난 장부 기록 영구 삭제
            cursor.execute("DELETE FROM ledger WHERE timestamp <= datetime('now', '-90 days')")
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            if deleted_count > 0:
                print(f"🧹 개인정보보호법에 따라 90일이 경과한 {deleted_count}개의 데이터를 파기했습니다.")
        except Exception as e:
            print("데이터 파기 중 오류 발생:", e)
        
        await asyncio.sleep(86400) # 24시간(86400초)마다 1번씩 실행

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
    asyncio.create_task(auto_delete_old_data()) # 파기 스케줄러 시작

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)