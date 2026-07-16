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

# ✨ 재생 완료 ack: 오버레이가 '다 그렸다'고 알려줄 때까지 서버가 기다리기 위한 상태
#    (레이어가 많아 렌더가 오래 걸려도 그리는 과정이 중간에 잘리지 않게 함)
current_playback_id = 0
playback_ack = {"id": 0}
playback_ack_event = asyncio.Event()

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

    # ✨ 문의사항(개발자에게 보내는 메시지) 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inquiries (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT,
            message TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_read BOOLEAN DEFAULT FALSE
        )
    ''')

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

    # ✨ 네이버 실제 메일 주소를 담을 컬럼 (donor_email은 예전부터 'getId() 식별자'를 담고 있어 이름과 달리 이메일이 아님).
    #    식별/차단은 계속 donor_email(식별자) 기준이고, 이 컬럼은 참고용으로만 추가한다.
    try:
        cursor.execute("ALTER TABLE ledger ADD COLUMN donor_naver_email TEXT")
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
    cursor.execute("SELECT id, donor_name, donor_email, donor_naver_email, drawing_title, timestamp FROM ledger ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    # email = 네이버 앱별 고유 식별자(차단 기준), naverEmail = 실제 메일 주소(참고용)
    return [{"id": r["id"], "name": r["donor_name"], "email": r["donor_email"],
             "naverEmail": r["donor_naver_email"] or "", "title": r["drawing_title"],
             "time": str(r["timestamp"])} for r in rows]

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

def _insert_ledger(email, name, profile_image, title, drawing_history, naver_email=""):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ledger (donor_email, donor_name, donor_profile_image, drawing_title, drawing_data, donor_naver_email) VALUES (%s, %s, %s, %s, %s, %s)",
        (email, name, profile_image, title, json.dumps(drawing_history), naver_email)
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

def _insert_inquiry(name, email, message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO inquiries (name, email, message) VALUES (%s, %s, %s)", (name, email, message))
    conn.commit()
    conn.close()

def _fetch_inquiries():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT id, name, email, message, timestamp, is_read FROM inquiries ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["name"], "email": r["email"], "message": r["message"], "time": str(r["timestamp"]), "is_read": bool(r["is_read"])} for r in rows]

def _delete_inquiry(inquiry_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM inquiries WHERE id = %s", (inquiry_id,))
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

def _handle_ws_message(msg):
    # ✨ 오버레이가 보내는 메시지 처리 (현재는 재생 완료 ack만)
    try:
        data = json.loads(msg)
        if data.get("type") == "playback_done":
            pid = data.get("playbackId") or 0
            if pid > playback_ack["id"]:
                playback_ack["id"] = pid
            playback_ack_event.set()
    except:
        pass

# ✨ 테스트 전용 웹소켓 (이곳으로 연결된 화면만 테스트 그림을 받음)
@app.websocket("/ws/test")
async def websocket_test_endpoint(websocket: WebSocket):
    await websocket.accept()
    test_connections.append(websocket)
    try:
        while True:
            msg = await websocket.receive_text()
            _handle_ws_message(msg)
    except: pass
    finally:
        if websocket in test_connections: test_connections.remove(websocket)
@app.post("/api/skip")
async def skip_drawing():
    global skip_current_drawing
    skip_current_drawing = True
    playback_ack_event.set()  # ✨ 재생 완료를 기다리는 중이면 즉시 깨워서 스킵이 바로 반영되게
    
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

# ✨ 문의사항(개발자에게 메시지) 접수/조회/삭제
class InquiryCreate(BaseModel):
    name: str = ""
    email: str = ""
    message: str

@app.post("/api/inquiry")
async def create_inquiry(data: InquiryCreate):
    message = (data.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="문의 내용을 입력해주세요.")
    if len(message) > 2000:
        message = message[:2000]  # 과도한 길이 방어
    name = (data.name or "").strip()[:100]
    email = (data.email or "").strip()[:200]
    await asyncio.to_thread(_insert_inquiry, name, email, message)
    return {"status": "success"}

@app.get("/api/inquiries")
async def get_inquiries():
    return await asyncio.to_thread(_fetch_inquiries)

@app.delete("/api/inquiry/{inquiry_id}")
async def delete_inquiry(inquiry_id: int):
    await asyncio.to_thread(_delete_inquiry, inquiry_id)
    return {"status": "success"}

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

    # ✨ 차단은 '입장 시점'에만 검사되므로, 이미 합작방에 들어와 있는 사람은 그대로 그리고 있게 된다.
    #    차단하는 즉시 해당 계정을 방에서 내보낸다.
    if data.add_blocked_email:
        await _kick_user_from_rooms(data.add_blocked_email)

    return {"message": "success"}

async def _kick_user_from_rooms(user_id):
    """차단된 계정을 모든 합작방에서 즉시 퇴장시킨다. (연결을 끊으면 WS finally가 정리·브로드캐스트를 처리)"""
    if not user_id:
        return
    for room in list(rooms.values()):
        for cid, p in list(room["participants"].items()):
            if p.get("userId") != user_id:
                continue
            try:
                await p["ws"].send_text(json.dumps({"type": "error", "message": "차단되어 합작방에서 나갑니다."}))
            except:
                pass
            try:
                await p["ws"].close()
            except:
                pass
            print(f"[ROOM] kicked banned user room={room['id']} client={cid} userId={user_id}")

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
        email = data.get("email")   # ⚠️ 이름은 email이지만 실제로는 네이버 앱별 고유 식별자(getId). 차단/식별의 기준이므로 절대 실제 메일로 바꾸지 말 것.
        name = data.get("name")
        profile_image = data.get("profileImage", "")
        title = data.get("title", "제목없음")
        drawing_history = data.get("drawingData")
        naver_email = (data.get("naverEmail") or "").strip()[:200]   # ✨ 실제 메일 주소(참고용)

        if not email: raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
        # 차단 판정은 계속 식별자 기준 (기존 블랙리스트가 그대로 유효)
        if email in settings["blocked_emails"]: raise HTTPException(status_code=403, detail="차단된 계정입니다.")

        await asyncio.to_thread(_insert_ledger, email, name, profile_image, title, drawing_history, naver_email)
        
        await drawing_queue.put(data)
        return {"status": "success"}
    except HTTPException as he: raise he
    except Exception as e: 
        print(f"Submit Error: {e}")
        raise HTTPException(status_code=500, detail="서버 오류 발생")

async def process_drawing_queue():
    global skip_current_drawing, current_playback_id

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
                    # ✨ 레이어가 많으면 payload(구운 바닥 이미지 등)가 수 MB라, 연결마다 send_json으로
                    #    매번 재직렬화하면 이벤트 루프가 그만큼 멈춥니다. 한 번만(스레드에서) 직렬화하고
                    #    같은 문자열을 send_text로 재사용해 블로킹과 중복 직렬화를 줄입니다.
                    current_playback_id += 1
                    pid = current_playback_id
                    playback_ack_event.clear()  # 이번 재생의 ack를 새로 기다리기 위해 초기화(보내기 직전에)

                    timelapse_text = await asyncio.to_thread(json.dumps, {
                        "type": "play_timelapse",
                        "playbackId": pid,   # ✨ 오버레이가 재생을 마치면 이 id로 완료 신호를 보냄
                        "alert": { "name": name, "title": title, "profileImage": profile_image },
                        "history": drawing_data
                    })
                    for connection in target_connections:
                        try:
                            await connection.send_text(timelapse_text)
                        except: pass

                    # ✨ 고정 sleep(8) 대신, 오버레이가 '다 그렸다(playback_done)'고 알릴 때까지 대기.
                    #    레이어가 많아 렌더가 오래 걸려도 그리는 과정이 잘리지 않고, 빨리 끝나면 바로 다음 단계로.
                    #    (상한 25초 · 보는 오버레이가 없으면 대기 생략 → 무한 대기 방지)
                    if not skip_current_drawing and target_connections:
                        async def _wait_playback_done():
                            while playback_ack["id"] < pid and not skip_current_drawing:
                                playback_ack_event.clear()
                                await playback_ack_event.wait()
                        try:
                            await asyncio.wait_for(_wait_playback_done(), timeout=25)
                        except asyncio.TimeoutError:
                            pass
                
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
        while True:
            msg = await websocket.receive_text()
            _handle_ws_message(msg)
    except: pass
    finally:
        if websocket in active_connections: active_connections.remove(websocket)

# ================== 협업 방 (마그마식 동시 그리기) ==================
# 설계 메모:
# - 방/참가자 상태는 인메모리. 그림 스냅샷은 서버에 저장하지 않고, 새로 입장한 사람에게는
#   호스트(가장 먼저 들어온 참가자)가 자기 화면 상태를 통째로 넘겨준다(서버는 릴레이만).
#   → 서버 메모리 폭증이 없고, 방에 아무도 없으면 그림도 사라진다(실시간 협업 방의 특성).
# - 참가자마다 고유 레이어 1개. 서버가 draw 메시지에 layerId를 강제 주입해 남의 레이어엔 못 그린다.
# - 프레임(애니메이션) 목록은 서버가 소유하고 변경 시 전원에게 브로드캐스트한다.
ROOM_PASSWORD = "3400"          # ✨ 지금은 임시로 모든 방 공통 고정 비밀번호
MAX_ROOMS = 30
MAX_PARTICIPANTS_PER_ROOM = 12
MAX_ROOM_FRAMES = 24

rooms = {}
_room_seq = 0
_client_seq = 0

class RoomCreate(BaseModel):
    title: str
    userId: str = ""

@app.get("/api/rooms")
async def list_rooms():
    return [
        {"id": r["id"], "title": r["title"], "count": len(r["participants"]), "frames": len(r["frames"])}
        for r in rooms.values()
    ]

@app.post("/api/rooms")
async def create_room(data: RoomCreate):
    global _room_seq
    title = (data.title or "").strip()[:60]
    if not title:
        raise HTTPException(status_code=400, detail="방 제목을 입력해주세요.")

    # ✨ 방 생성도 로그인 + 차단 검사. 입장만 막으면 차단된 사람이 빈 방을 계속 만들어
    #    방 목록을 오염시키고 MAX_ROOMS를 고갈시킬 수 있다.
    user_id = (data.userId or "").strip()[:100]
    if not user_id:
        raise HTTPException(status_code=401, detail="합작방은 로그인 후 이용할 수 있습니다.")
    settings = await asyncio.to_thread(get_db_settings)
    if user_id in settings.get("blocked_emails", []):
        raise HTTPException(status_code=403, detail="차단된 계정입니다.")

    if len(rooms) >= MAX_ROOMS:
        raise HTTPException(status_code=429, detail="방이 너무 많습니다. 잠시 후 다시 시도해주세요.")
    _room_seq += 1
    rid = f"room_{_room_seq}"
    rooms[rid] = {
        "id": rid,
        "title": title,
        "frames": [{"id": "frame_1", "duration": 500}],
        "participants": {},   # clientId -> {"name", "layerId", "ws"}
        "empty_since": datetime.now(),
    }
    return {"id": rid, "title": title}

async def _room_broadcast(room, text, exclude=None):
    for cid, p in list(room["participants"].items()):
        if exclude and cid == exclude:
            continue
        try:
            await p["ws"].send_text(text)
        except:
            pass

async def _room_maybe_send_go(room):
    # ✨ '다같이 보내기': 현재 참가자 전원이 동의하면 송출 시작을 알린다.
    ps = room.get("pending_send")
    if not ps or not room["participants"]:
        return
    if not set(room["participants"].keys()) <= ps["consents"]:
        return
    # 조립·전송은 '호스트(가장 먼저 들어온 사람)'가 맡는다. 방 시작부터의 전 과정을 갖고 있는 유일한 참가자이기 때문.
    assembler = next(iter(room["participants"]))
    await _room_broadcast(room, json.dumps({
        "type": "send_go",
        "assemblerId": assembler,
        "title": ps["title"],
        "participants": [{"clientId": cid, "name": p["name"], "layerId": p["layerId"]}
                         for cid, p in room["participants"].items()],
    }))
    room["pending_send"] = None

@app.websocket("/ws/room/{room_id}")
async def websocket_room_endpoint(websocket: WebSocket, room_id: str):
    global _client_seq
    await websocket.accept()
    room = rooms.get(room_id)
    if not room:
        try:
            await websocket.send_text(json.dumps({"type": "error", "message": "방을 찾을 수 없습니다."}))
            await websocket.close()
        except: pass
        return

    client_id = None
    try:
        # 1) 첫 메시지는 반드시 join (비밀번호 검증)
        first = json.loads(await websocket.receive_text())
        if first.get("type") != "join" or str(first.get("password", "")) != ROOM_PASSWORD:
            await websocket.send_text(json.dumps({"type": "error", "message": "비밀번호가 올바르지 않습니다."}))
            await websocket.close()
            return
        if len(room["participants"]) >= MAX_PARTICIPANTS_PER_ROOM:
            await websocket.send_text(json.dumps({"type": "error", "message": "방 인원이 가득 찼습니다."}))
            await websocket.close()
            return

        # 2) 참가자 등록 (고유 레이어 1개 배정)
        host_id = next(iter(room["participants"]), None)   # 기존 최초 참가자 = 상태 제공자
        _client_seq += 1
        client_id = f"c{_client_seq}"
        name = (first.get("name") or "").strip()[:20] or "익명"
        layer_id = f"rlayer_{client_id}"
        # ✨ userId(네이버 앱별 고유 식별자)는 서버에만 보관하고 다른 참가자에게는 브로드캐스트하지 않는다.
        #    (누가 방에 있었는지 남겨 악용 대응에 쓰기 위함)
        user_id = (first.get("userId") or "").strip()[:100]
        if not user_id:
            await websocket.send_text(json.dumps({"type": "error", "message": "합작방은 로그인 후 이용할 수 있습니다."}))
            await websocket.close()
            return

        # ✨ 크리에이터 페이지에서 차단된 계정은 합작방에도 들어올 수 없다.
        #    (blocked_emails는 이름과 달리 '네이버 앱별 고유 식별자' 목록 — 후원 전송과 같은 기준)
        try:
            room_settings = await asyncio.to_thread(get_db_settings)
            if user_id in room_settings.get("blocked_emails", []):
                await websocket.send_text(json.dumps({"type": "error", "message": "차단된 계정입니다."}))
                await websocket.close()
                return
        except Exception as e:
            # 차단 목록을 못 읽으면(DB 장애) 합작방을 통째로 막지는 않되, 로그는 남긴다.
            print(f"[ROOM] 차단 목록 조회 실패(입장은 허용): {e}")
        room["participants"][client_id] = {"name": name, "layerId": layer_id, "ws": websocket, "userId": user_id}
        print(f"[ROOM] join room={room['id']} client={client_id} name={name} userId={user_id}")
        room["empty_since"] = None

        await websocket.send_text(json.dumps({
            "type": "joined",
            "clientId": client_id, "layerId": layer_id,
            "roomId": room["id"], "roomTitle": room["title"],
            "frames": room["frames"],
            "participants": [{"clientId": cid, "name": p["name"], "layerId": p["layerId"]}
                             for cid, p in room["participants"].items()],
            "isHost": host_id is None,
        }))
        await _room_broadcast(room, json.dumps({
            "type": "participant_joined",
            "participant": {"clientId": client_id, "name": name, "layerId": layer_id}
        }), exclude=client_id)

        # 3) 기존 호스트에게 "현재 화면 상태를 이 사람에게 보내달라"고 요청
        if host_id and host_id in room["participants"]:
            try:
                await room["participants"][host_id]["ws"].send_text(json.dumps({
                    "type": "request_state", "forClientId": client_id
                }))
            except: pass

        # 4) 메시지 루프
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except:
                continue
            mtype = msg.get("type")

            if mtype == "draw":
                # ✨ 서버가 보낸이/레이어를 강제 주입 → 남의 레이어에 그리는 것을 원천 차단
                msg["senderId"] = client_id
                msg["layerId"] = layer_id
                await _room_broadcast(room, json.dumps(msg), exclude=client_id)

            elif mtype == "frame_op":
                op = msg.get("op")
                if op == "add" and len(room["frames"]) < MAX_ROOM_FRAMES:
                    room["frames"].append({
                        "id": f"frame_{_client_seq}_{len(room['frames'])}_{int(datetime.now().timestamp() * 1000)}",
                        "duration": max(10, int(msg.get("duration") or 500)),
                    })
                elif op == "delete" and len(room["frames"]) > 1:
                    room["frames"] = [f for f in room["frames"] if f["id"] != msg.get("frameId")]
                elif op == "duration":
                    for f in room["frames"]:
                        if f["id"] == msg.get("frameId"):
                            f["duration"] = max(10, int(msg.get("duration") or 500))
                await _room_broadcast(room, json.dumps({"type": "frames_updated", "frames": room["frames"]}))

            elif mtype == "room_state":
                # 호스트가 보낸 현재 상태를 요청자에게만 그대로 릴레이 (거대할 수 있어 재직렬화 없이 원문 전달)
                target = room["participants"].get(msg.get("forClientId"))
                if target:
                    try:
                        await target["ws"].send_text(raw)
                    except: pass

            elif mtype == "send_request":
                # ✨ 다같이 보내기 제안 — 전원 동의해야 송출
                if room.get("pending_send"):
                    try:
                        await websocket.send_text(json.dumps({"type": "send_cancel", "reason": "이미 송출 동의가 진행 중입니다."}))
                    except: pass
                    continue
                room["pending_send"] = {
                    "requesterId": client_id,
                    "title": (msg.get("title") or room["title"] or "합작").strip()[:60],
                    "consents": {client_id},   # 제안자는 자동 동의
                }
                await _room_broadcast(room, json.dumps({
                    "type": "send_request",
                    "requesterId": client_id, "requesterName": name,
                    "title": room["pending_send"]["title"],
                    "agreed": 1, "total": len(room["participants"]),
                }))
                await _room_maybe_send_go(room)

            elif mtype == "send_consent":
                ps = room.get("pending_send")
                if not ps:
                    continue
                if msg.get("accept"):
                    ps["consents"].add(client_id)
                    await _room_broadcast(room, json.dumps({
                        "type": "send_progress",
                        "agreed": len(ps["consents"] & set(room["participants"].keys())),
                        "total": len(room["participants"]),
                    }))
                    await _room_maybe_send_go(room)
                else:
                    room["pending_send"] = None
                    await _room_broadcast(room, json.dumps({
                        "type": "send_cancel", "reason": f"{name}님이 동의하지 않아 취소되었습니다."
                    }))
    except:
        pass
    finally:
        if client_id and client_id in room["participants"]:
            room["participants"].pop(client_id, None)

            if not room["participants"]:
                # ✨ 마지막 사람이 나가면 방을 '즉시' 삭제한다.
                #    방이 비면 그림 상태를 들고 있을 호스트가 없어 어차피 내용이 사라지므로,
                #    껍데기만 남은 0명 방을 로비에 띄워둘 이유가 없다.
                rooms.pop(room["id"], None)
                print(f"[ROOM] closed (empty) room={room['id']}")
            else:
                await _room_broadcast(room, json.dumps({"type": "participant_left", "clientId": client_id}))

                # ✨ 송출 동의 진행 중이었다면: 제안자가 나가면 취소, 아니면 남은 인원 기준으로 재판정
                ps = room.get("pending_send")
                if ps:
                    if ps["requesterId"] == client_id:
                        room["pending_send"] = None
                        await _room_broadcast(room, json.dumps({
                            "type": "send_cancel", "reason": "제안자가 나가서 취소되었습니다."
                        }))
                    else:
                        ps["consents"].discard(client_id)
                        await _room_maybe_send_go(room)

async def cleanup_empty_rooms():
    # 참가자가 다 나간 방은 퇴장 시점에 즉시 삭제되므로, 여기서는
    # '만들어놓고 아무도 들어오지 않은 유령 방'만 짧게 정리한다(방 목록 오염·MAX_ROOMS 고갈 방지).
    while True:
        await asyncio.sleep(60)
        try:
            now = datetime.now()
            for rid in [k for k, v in rooms.items()
                        if not v["participants"] and v.get("empty_since")
                        and (now - v["empty_since"]).total_seconds() > 120]:
                rooms.pop(rid, None)
                print(f"[ROOM] closed (never joined) room={rid}")
        except Exception as e:
            print(f"Room cleanup error: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_drawing_queue())
    asyncio.create_task(auto_delete_old_data())
    asyncio.create_task(cleanup_empty_rooms())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)