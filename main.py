import os
import asyncio
import json
import math
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

import psycopg2
from psycopg2.extras import DictCursor

# 방송인용 관리자 비밀번호 (이 부분도 환경변수 처리를 권장합니다)
CREATOR_PASSWORD = os.getenv("CREATOR_PASSWORD", "streamer777!")

# ✨ 하드코딩된 기본값을 완전히 삭제하고 환경변수에서만 불러옵니다.
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()
app.mount("/Fonts", StaticFiles(directory="Fonts"), name="Fonts")
app.mount("/Brushes", StaticFiles(directory="Brushes"), name="Brushes")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

active_connections = []
test_connections = []
drawing_queue = asyncio.Queue()

skip_current_drawing = False

def get_db_connection():
    # 환경변수가 없을 경우 서버가 에러를 띄워 명확하게 알려줍니다.
    if not DATABASE_URL:
        raise ValueError("🚨 DATABASE_URL 환경변수가 설정되지 않았습니다. 클라우드타입 대시보드에서 환경변수를 추가해 주세요!")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. 기본 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ledger (
            id SERIAL PRIMARY KEY,
            donor_email TEXT,
            donor_name TEXT NOT NULL,
            donor_profile_image TEXT,
            drawing_title TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            drawing_data TEXT,
            is_played BOOLEAN DEFAULT FALSE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY DEFAULT 1,
            is_donation_enabled BOOLEAN DEFAULT TRUE,
            blocked_emails TEXT DEFAULT '[]',
            display_duration INTEGER DEFAULT 8,
            daily_limit INTEGER DEFAULT 0,
            notice_text TEXT DEFAULT ''
        )
    ''')
    cursor.execute("INSERT INTO settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
    
    # ✨ 핵심 수정: 테이블을 만들자마자 '확정(commit)'을 지어주어, 이후 작업이 실패해도 테이블이 날아가지 않게 보호합니다.
    conn.commit()
    
    # 2. 업데이트 시 누락된 컬럼을 안전하게 추가하는 로직
    try:
        cursor.execute("ALTER TABLE settings ADD COLUMN display_duration INTEGER DEFAULT 8")
        conn.commit()
    except psycopg2.Error:
        conn.rollback() 

    try:
        cursor.execute("ALTER TABLE settings ADD COLUMN daily_limit INTEGER DEFAULT 0")
        conn.commit()
    except psycopg2.Error:
        conn.rollback()

    try:
        cursor.execute("ALTER TABLE settings ADD COLUMN notice_text TEXT DEFAULT ''")
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
        
    conn.close()

init_db()

def get_db_settings():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT * FROM settings WHERE id = 1")
    row = cursor.fetchone()
    conn.close()
    
    row_dict = dict(row) if row else {}
    return {
        "is_donation_enabled": bool(row_dict.get("is_donation_enabled", True)),
        "blocked_emails": json.loads(row_dict.get("blocked_emails", '[]')),
        "display_duration": row_dict.get("display_duration", 8),
        "daily_limit": row_dict.get("daily_limit", 0),
        "notice_text": row_dict.get("notice_text", "") # ✨ 추가
    }

def update_db_settings(is_enabled=None, blocked_emails=None, display_duration=None, daily_limit=None, notice_text=None): # ✨ 파라미터 추가
    conn = get_db_connection()
    cursor = conn.cursor()
    if is_enabled is not None:
        cursor.execute("UPDATE settings SET is_donation_enabled = %s WHERE id = 1", (bool(is_enabled),))
    if blocked_emails is not None:
        cursor.execute("UPDATE settings SET blocked_emails = %s WHERE id = 1", (json.dumps(blocked_emails),))
    if display_duration is not None:
        cursor.execute("UPDATE settings SET display_duration = %s WHERE id = 1", (display_duration,))
    if daily_limit is not None:
        cursor.execute("UPDATE settings SET daily_limit = %s WHERE id = 1", (daily_limit,))
    if notice_text is not None: # ✨ DB에 공지사항 저장 로직 추가
        cursor.execute("UPDATE settings SET notice_text = %s WHERE id = 1", (notice_text,))
    conn.commit()
    conn.close()

# ✨ 아래 헬퍼들은 동기(blocking) DB 작업을 모아둔 함수입니다.
# psycopg2는 동기 라이브러리라 async 엔드포인트 안에서 그냥 호출하면 이벤트 루프 전체가 멈춰
# (= 방송 화면의 그림 재생도 같이 멈춰) 버립니다. 그래서 asyncio.to_thread로 스레드에서 실행합니다.
def _fetch_recent_donations():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT id, donor_name, donor_email, drawing_title, timestamp FROM ledger ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["donor_name"], "email": r["donor_email"], "title": r["drawing_title"], "time": str(r["timestamp"])} for r in rows]

def _fetch_replay_row(ledger_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT donor_name, donor_profile_image, drawing_title, drawing_data FROM ledger WHERE id = %s", (ledger_id,))
    row = cursor.fetchone()
    conn.close()
    return row

def _count_since(target_str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ledger WHERE timestamp >= %s", (target_str,))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def _insert_ledger(email, name, profile_image, title, drawing_history):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ledger (donor_email, donor_name, donor_profile_image, drawing_title, drawing_data) VALUES (%s, %s, %s, %s, %s)",
        (email, name, profile_image, title, json.dumps(drawing_history))
    )
    conn.commit()
    conn.close()

def _fetch_donation(ledger_id):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT * FROM ledger WHERE id = %s", (ledger_id,))
    row = cursor.fetchone()
    conn.close()
    return row

def _fetch_donations_by_date(date):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT id, donor_name, drawing_title, timestamp FROM ledger WHERE DATE(timestamp) = %s", (date,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["donor_name"], "title": r["drawing_title"], "time": str(r["timestamp"])} for r in rows]

def _delete_old_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ledger WHERE timestamp <= NOW() - INTERVAL '2 days'")
    conn.commit()
    conn.close()

# ✨ test.html 서빙 라우터 추가
@app.get("/test")
async def serve_test_page(): return FileResponse("test.html")
@app.get("/api/fonts")
async def get_font_list():
    font_dir = "Fonts"
    # 폴더가 없으면 빈 목록 반환
    if not os.path.exists(font_dir):
        return []
    
    # 확장자가 .ttf 인 파일만 골라내서 확장자를 뗀 이름만 리스트로 만듭니다.
    fonts = [os.path.splitext(f)[0] for f in os.listdir(font_dir) if f.lower().endswith('.ttf')]
    return fonts
@app.get("/api/brushes")
async def get_brush_list():
    brush_dir = "Brushes"
    if not os.path.exists(brush_dir):
        return []
    # 확장자가 .png 인 파일만 이름 추출
    brushes = [os.path.splitext(f)[0] for f in os.listdir(brush_dir) if f.lower().endswith('.png')]
    return brushes
# ✨ 테스트 데이터 수신 엔드포인트
@app.post("/api/submit-test")
async def submit_test(request: Request):
    data = await request.json()
    data["is_test"] = True # 테스트용 데이터라는 꼬리표(플래그) 부착
    await drawing_queue.put(data)
    return {"status": "success"}

# ✨ 테스트 전용 웹소켓 (이곳으로 연결된 화면만 테스트 그림을 받음)
@app.websocket("/ws/test")
async def websocket_test_endpoint(websocket: WebSocket):
    await websocket.accept()
    test_connections.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: pass
    finally: test_connections.remove(websocket)
@app.post("/api/skip")
async def skip_drawing():
    global skip_current_drawing
    skip_current_drawing = True 
    
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
    return await asyncio.to_thread(get_db_settings)

@app.post("/api/toggle-donation")
async def toggle_donation(enable: bool):
    await asyncio.to_thread(update_db_settings, is_enabled=enable)
    return {"message": "success"}

class SettingsUpdate(BaseModel):
    add_blocked_email: str = None
    remove_blocked_email: str = None
    display_duration: int = None
    daily_limit: int = None
    notice_text: str = None # ✨ 추가

@app.post("/api/update-settings")
async def update_settings(data: SettingsUpdate):
    current_settings = await asyncio.to_thread(get_db_settings)
    blocked = current_settings["blocked_emails"]
    changed = False
    if data.add_blocked_email and data.add_blocked_email not in blocked:
        blocked.append(data.add_blocked_email)
        changed = True
    if data.remove_blocked_email and data.remove_blocked_email in blocked:
        blocked.remove(data.remove_blocked_email)
        changed = True
    
    # ✨ notice_text 추가 전송
    await asyncio.to_thread(
        update_db_settings,
        blocked_emails=blocked if changed else None,
        display_duration=data.display_duration,
        daily_limit=data.daily_limit,
        notice_text=data.notice_text
    )
    return {"message": "success"}

@app.get("/api/recent-donations")
async def get_recent_donations():
    return await asyncio.to_thread(_fetch_recent_donations)

@app.post("/api/replay-donation/{ledger_id}")
async def replay_donation(ledger_id: int):
    row = await asyncio.to_thread(_fetch_replay_row, ledger_id)
    if not row: raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")
    await drawing_queue.put({"name": row[0], "profileImage": row[1], "title": row[2], "drawingData": json.loads(row[3])})
    return {"message": "success"}

@app.post("/api/submit-drawing")
async def submit_drawing(request: Request):
    settings = await asyncio.to_thread(get_db_settings)
    if not settings["is_donation_enabled"]: raise HTTPException(status_code=403, detail="현재 그림 받기가 닫혀있습니다.")

    if settings.get("daily_limit", 0) > 0:
        now = datetime.now()
        target_date = now - timedelta(days=1) if now.hour < 6 else now
        target_str = target_date.strftime('%Y-%m-%d 06:00:00')
        count = await asyncio.to_thread(_count_since, target_str)
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

        await asyncio.to_thread(_insert_ledger, email, name, profile_image, title, drawing_history)
        
        await drawing_queue.put(data)
        return {"status": "success"}
    except HTTPException as he: raise he
    except Exception as e: 
        print(f"Submit Error: {e}")
        raise HTTPException(status_code=500, detail="서버 오류 발생")

async def process_drawing_queue():
    global skip_current_drawing 
    
    while True:
        try: 
            payload = await drawing_queue.get()
            skip_current_drawing = False 
            
            # 목적지 분기 처리 (테스트 플래그 확인)
            target_connections = test_connections if payload.get("is_test") else active_connections
            
            settings = await asyncio.to_thread(get_db_settings)
            display_duration = settings.get("display_duration", 8)
            
            name = payload.get("name", "익명")
            title = payload.get("title", "제목없음")
            profile_image = payload.get("profileImage", "")
            
            # 1. 화면 정리(clear)만 먼저 즉각 보냅니다.
            for connection in target_connections:
                try: await connection.send_json({"type": "clear"})
                except: pass
            
            drawing_data = payload.get("drawingData", [])
            is_animation = isinstance(drawing_data, dict) and drawing_data.get("isAnimation")

            if is_animation:
                # 움짤(GIF) 모드일 때는 다운로드 인디케이터가 있으므로 알림을 먼저 띄웁니다.
                for connection in target_connections:
                    try: 
                        await connection.send_json({
                            "type": "alert", 
                            "name": name, 
                            "title": title, 
                            "profileImage": profile_image
                        })
                    except: pass

                frames = drawing_data.get("frames", [])
                repeat_count = drawing_data.get("repeatCount", 5)
                total_loops = min(20, max(1, int(repeat_count)))

                if frames:
                    for connection in target_connections:
                        try: await connection.send_json({"type": "init_animation_cache", "totalFrames": len(frames)})
                        except: pass
                        
                    for i, frame in enumerate(frames):
                        if skip_current_drawing: break
                        for connection in target_connections:
                            try: await connection.send_json({
                                "type": "cache_frame", 
                                "src": frame.get("src"), 
                                "duration": frame.get("duration", 500)
                            })
                            except: pass
                        await asyncio.sleep(0.05) 

                    for connection in target_connections:
                        try: 
                            await connection.send_json({
                                "type": "play_animation", 
                                "repeatCount": total_loops
                            })
                        except: pass
                    
                    total_duration = sum(frame.get("duration", 500) for frame in frames) / 1000.0
                    total_sleep_time = total_duration * total_loops
                    sleep_intervals = int(total_sleep_time / 0.1)
                    
                    for _ in range(sleep_intervals):
                        if skip_current_drawing: break
                        await asyncio.sleep(0.1)
                    
                    if not skip_current_drawing:
                        await asyncio.sleep(total_sleep_time % 0.1)
            
            else:
                # 타임랩스 (일반 그림) 모드
                if drawing_data and isinstance(drawing_data, list):
                    init_item = next((item for item in drawing_data if item.get("type") == "init_layers"), None)
                    if init_item:
                        for connection in target_connections:
                            try: await connection.send_json(init_item)
                            except: pass

                    # 2. 알림 데이터를 타임랩스 그림 데이터에 '포함' 시켜서 하나의 보따리로 보냅니다!
                    for connection in target_connections:
                        try: 
                            await connection.send_json({
                                "type": "play_timelapse",
                                "alert": { "name": name, "title": title, "profileImage": profile_image },
                                "history": drawing_data
                            })
                        except: pass
                    if not skip_current_drawing:
                        # 클라이언트의 playbackDurationMs(6000)와 동일하게 6초 동안 
                        # 그림이 다 그려질 때까지 서버도 먼저 기다립니다.
                        await asyncio.sleep(8)
                
            if not skip_current_drawing:
                await asyncio.sleep(display_duration)
            
            for connection in target_connections:
                try: await connection.send_json({"type": "fade_out"})
                except: pass
                
            await asyncio.sleep(1.5) 
            
            for connection in target_connections:
                try: await connection.send_json({"type": "clear"})
                except: pass

        except Exception as e:
            print(f"Queue Processing Error: {e}")
        finally:
            drawing_queue.task_done()

async def auto_delete_old_data():
    while True:
        try:
            await asyncio.to_thread(_delete_old_data)
        except Exception as e: 
            print(f"Delete old data error: {e}")
        await asyncio.sleep(86400) # 24시간(86400초)마다 한 번씩 검사하여 삭제를 수행합니다.
# --- 기존 코드 (app.get("/api/recent-donations") 등) 아래 쯤에 추가 ---

@app.get("/api/donation/{ledger_id}")
async def get_donation_data(ledger_id: int):
    row = await asyncio.to_thread(_fetch_donation, ledger_id)
    if not row: raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")
    return {"id": row["id"], "name": row["donor_name"], "title": row["drawing_title"], "data": json.loads(row["drawing_data"]), "time": str(row["timestamp"])}

@app.get("/api/donations/by-date")
async def get_donations_by_date(date: str):
    return await asyncio.to_thread(_fetch_donations_by_date, date)
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