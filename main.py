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

DB_FILE = "donation_ledger_v4.db"

# 상단 전역 변수 영역에 추가
skip_current_drawing = False

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
    try:
        cursor.execute("ALTER TABLE settings ADD COLUMN display_duration INTEGER DEFAULT 8")
        cursor.execute("ALTER TABLE settings ADD COLUMN daily_limit INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

init_db()

def get_db_settings():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.cursor().execute("SELECT * FROM settings WHERE id = 1").fetchone()
    conn.close()
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



# API 라우터 영역에 추가
@app.post("/api/skip")
async def skip_drawing():
    global skip_current_drawing
    skip_current_drawing = True  # 현재 진행 중인 그리기 루프 중단 신호
    
    # 즉시 모든 방송 화면(클라이언트)을 지우는 신호 전송
    for connection in active_connections:
        try:
            await connection.send_json({"type": "clear"})
        except:
            pass
            
    return {"status": "success", "message": "현재 그림이 스킵되었습니다."}

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
    if data.password == CREATOR_PASSWORD: return {"valid": True}
    raise HTTPException(status_code=401, detail="비밀번호가 틀렸습니다.")

@app.get("/api/settings")
async def get_settings(): 
    return get_db_settings()

@app.post("/api/toggle-donation")
async def toggle_donation(enable: bool):
    update_db_settings(is_enabled=enable)
    return {"message": "success"}

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
    update_db_settings(blocked_emails=blocked if changed else None, display_duration=data.display_duration, daily_limit=data.daily_limit)
    return {"message": "success"}

@app.get("/api/recent-donations")
async def get_recent_donations():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.cursor().execute("SELECT id, donor_name, donor_email, drawing_title, timestamp FROM ledger ORDER BY id DESC LIMIT 10").fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["donor_name"], "email": r["donor_email"], "title": r["drawing_title"], "time": r["timestamp"]} for r in rows]

@app.post("/api/replay-donation/{ledger_id}")
async def replay_donation(ledger_id: int):
    conn = sqlite3.connect(DB_FILE)
    row = conn.cursor().execute("SELECT donor_name, donor_profile_image, drawing_title, drawing_data FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
    conn.close()
    if not row: raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")
    await drawing_queue.put({"name": row[0], "profileImage": row[1], "title": row[2], "drawingData": json.loads(row[3])})
    return {"message": "success"}

@app.post("/api/submit-drawing")
async def submit_drawing(request: Request):
    settings = get_db_settings()
    if not settings["is_donation_enabled"]: raise HTTPException(status_code=403, detail="현재 그림 받기가 닫혀있습니다.")

    if settings.get("daily_limit", 0) > 0:
        now = datetime.now()
        target_date = now - timedelta(days=1) if now.hour < 6 else now
        target_str = target_date.strftime('%Y-%m-%d 06:00:00')
        conn = sqlite3.connect(DB_FILE)
        count = conn.cursor().execute("SELECT COUNT(*) FROM ledger WHERE timestamp >= ?", (target_str,)).fetchone()[0]
        conn.close()
        if count >= settings["daily_limit"]: raise HTTPException(status_code=403, detail=f"오늘 한도({settings['daily_limit']}개)가 초과되었습니다.")

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
        conn.cursor().execute("INSERT INTO ledger (donor_email, donor_name, donor_profile_image, drawing_title, drawing_data) VALUES (?, ?, ?, ?, ?)", (email, name, profile_image, title, json.dumps(drawing_history)))
        conn.commit()
        conn.close()
        
        await drawing_queue.put(data)
        return {"status": "success"}
    except HTTPException as he: raise he
    except Exception as e: raise HTTPException(status_code=500, detail="서버 오류 발생")

async def process_drawing_queue():
    global skip_current_drawing 
    
    while True:
        payload = await drawing_queue.get()
        skip_current_drawing = False 
        
        # ✨ 1. DB에서 설정값(화면 유지 시간) 불러오기
        settings = get_db_settings()
        display_duration = settings.get("display_duration", 8)
        
        # ✨ 2. 알림창 데이터 파싱
        name = payload.get("name", "익명")
        title = payload.get("title", "제목없음")
        profile_image = payload.get("profileImage", "")
        
        # ✨ 3. 화면을 싹 비우고(clear) 알림창(alert) 띄우기
        for connection in active_connections:
            try: 
                await connection.send_json({"type": "clear"})
                await connection.send_json({
                    "type": "alert", 
                    "name": name, 
                    "title": title, 
                    "profileImage": profile_image
                })
            except: pass
        
        # 4. 그리기 데이터 파싱
        drawing_data = payload.get("drawingData", [])
        is_animation = isinstance(drawing_data, dict) and drawing_data.get("isAnimation")

        # 5. 애니메이션 처리
        if is_animation:
            frames = drawing_data.get("frames", [])
            # ✨ 클라이언트에서 전송한 반복 횟수 추출 (서버단에서도 20회 상한선 이중 방어)
            repeat_count = drawing_data.get("repeatCount", 5)
            total_loops = min(20, max(1, int(repeat_count)))

            if frames:
                for _ in range(total_loops):
                    if skip_current_drawing: break
                    for frame in frames:
                        if skip_current_drawing: break
                        src = frame.get("src")
                        duration_sec = frame.get("duration", 500) / 1000.0 
                        for connection in active_connections:
                            try: await connection.send_json({"type": "draw_frame", "src": src})
                            except: pass
                        # 애니메이션은 프레임별로 무조건 쉬어주어야 정상 작동합니다.
                        await asyncio.sleep(duration_sec)
        
        # 6. 일반 그림 처리
        else:
            if drawing_data and isinstance(drawing_data, list):
                for item in drawing_data:
                    if skip_current_drawing: break
                    item_type = item.get("type", "path")
                    color = item.get("color")
                    
                    if item_type == "path":
                        points = item.get("points", [])
                        layer_id = item.get("layerId")
                        opacity = item.get("opacity", 1.0) 

                        if not points: continue
                        for connection in active_connections:
                            try: await connection.send_json({"type": "start_path", "point": points[0], "color": color, "layerId": layer_id, "opacity": opacity})
                            except: pass
                        
                        for i, point in enumerate(points[1:]):
                            if skip_current_drawing: break 
                            for connection in active_connections:
                                try: await connection.send_json({"type": "draw_line", "point": point, "color": color, "layerId": layer_id, "opacity": opacity})
                                except: pass
                            # 서버가 뻗지 않도록 50픽셀마다 아주 미세한 휴식(10배속)을 부여합니다.
                            if i % 50 == 0: await asyncio.sleep(0.01)
                        
                        if not skip_current_drawing:
                            for connection in active_connections:
                                try: await connection.send_json({"type": "end_path", "layerId": layer_id, "opacity": opacity})
                                except: pass
                    
                    elif item_type == "fill":
                        payload_msg = item.copy()
                        for connection in active_connections:
                            try: await connection.send_json(payload_msg)
                            except: pass
                        await asyncio.sleep(0.03)
                    
                    else:
                        payload_msg = item.copy()
                        payload_msg["type"] = "draw_shape"
                        payload_msg["shape"] = item_type
                        for connection in active_connections:
                            try: await connection.send_json(payload_msg)
                            except: pass
                        await asyncio.sleep(0.02)
            
        # ✨ 7. 그리기가 모두 끝난 뒤 DB 설정시간(display_duration) 만큼 대기
        if not skip_current_drawing:
            await asyncio.sleep(display_duration)
        
        # ✨ 8. 대기가 끝나면 화면 서서히 지우기 (fade_out)
        for connection in active_connections:
            try: 
                await connection.send_json({"type": "fade_out"})
            except: pass
            
        await asyncio.sleep(1.5) # 서서히 사라질 시간을 줌
        
        # ✨ 9. 완전히 사라지면 확실하게 내부 캔버스 데이터 클리어 (다음 그림과 섞임 방지)
        for connection in active_connections:
            try: 
                await connection.send_json({"type": "clear"})
            except: pass

        # 큐 작업 완료 보고
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