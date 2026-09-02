import os
import asyncio
import json
import math
from datetime import datetime, timedelta, timezone

# ✨ 한국 표준시(UTC+9, DST 없음). 배포 서버(예: 클라우드타입)는 보통 UTC라 DB의 timestamp가
#    UTC로 저장·반환되어 화면에 9시간 어긋나 보인다. tz 정보가 없는(naive) 값은 UTC로 간주하고 KST로 변환한다.
KST = timezone(timedelta(hours=9))
def _kst_str(ts):
    if ts is None:
        return ""
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
import httpx
import secrets as _secrets

import psycopg2
from psycopg2.extras import DictCursor

# 방송인용 관리자 비밀번호 (이 부분도 환경변수 처리를 권장합니다)
CREATOR_PASSWORD = os.getenv("CREATOR_PASSWORD", "streamer777!")

# ✨ 하드코딩된 기본값을 완전히 삭제하고 환경변수에서만 불러옵니다.
DATABASE_URL = os.getenv("DATABASE_URL")

# ✨ 치지직(CHZZK) 오픈 API 연동.
#    시청자용(유저 조회)과 방송인용(활동제한 쓰기)은 앱을 분리할 수 있다.
#    치지직은 인증 요청에 scope 파라미터가 없어 '앱에 등록된 scope 전체'를 한 번에 위임받는다.
#    → 시청자에게 활동제한 권한까지 요구하지 않으려면 앱을 2개로 나누는 편이 낫다.
#    STREAMER 쪽 값이 없으면 같은 앱을 쓴다.
CHZZK_CLIENT_ID = os.getenv("CHZZK_CLIENT_ID", "")
CHZZK_CLIENT_SECRET = os.getenv("CHZZK_CLIENT_SECRET", "")
CHZZK_STREAMER_CLIENT_ID = os.getenv("CHZZK_STREAMER_CLIENT_ID", "") or CHZZK_CLIENT_ID
CHZZK_STREAMER_CLIENT_SECRET = os.getenv("CHZZK_STREAMER_CLIENT_SECRET", "") or CHZZK_CLIENT_SECRET

CHZZK_AUTH_URL = "https://chzzk.naver.com/account-interlock"
CHZZK_API_BASE = "https://openapi.chzzk.naver.com"

def _chzzk_creds(role):
    if role == "streamer":
        return (CHZZK_STREAMER_CLIENT_ID, CHZZK_STREAMER_CLIENT_SECRET)
    return (CHZZK_CLIENT_ID, CHZZK_CLIENT_SECRET)

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

    # ✨ 치지직 계정 연결: 네이버 식별자(donor_email) ↔ 치지직 채널
    #    치지직 닉네임은 바뀔 수 있으므로 channel_id를 영구 키로 쓰고 nickname은 표시용으로 갱신한다.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chzzk_links (
            naver_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            nickname TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # ✨ 방송인 토큰(활동제한 호출용). role='streamer' 한 줄만 사용한다.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chzzk_tokens (
            role TEXT PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            expires_at TIMESTAMP
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

    # ✨ 치지직 연동 필수 여부. 기본 FALSE — 켜기 전까지는 기존과 똑같이 동작한다.
    try:
        cursor.execute("ALTER TABLE settings ADD COLUMN require_chzzk BOOLEAN DEFAULT FALSE")
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

    # ✨ 치지직 채널/닉네임 (연동한 사람만 채워진다. 기존 행은 NULL로 남고 표시는 기존 이름으로 폴백)
    for _col in ("chzzk_channel_id TEXT", "chzzk_nickname TEXT"):
        try:
            cursor.execute(f"ALTER TABLE ledger ADD COLUMN {_col}")
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
        "notice_text": row_dict.get("notice_text", ""), # ✨ 추가
        # ✨ 치지직 연동을 강제할지. CLIENT_ID가 없으면(=연동 자체가 불가) 강제하지 않는다.
        "require_chzzk": bool(row_dict.get("require_chzzk", False)) and bool(CHZZK_CLIENT_ID),
        "chzzk_configured": bool(CHZZK_CLIENT_ID)
    }

def update_db_settings(is_enabled=None, blocked_emails=None, display_duration=None, daily_limit=None, notice_text=None, require_chzzk=None): # ✨ 파라미터 추가
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
    if require_chzzk is not None:
        cursor.execute("UPDATE settings SET require_chzzk = %s WHERE id = 1", (bool(require_chzzk),))
    conn.commit()
    conn.close()

# ✨ 아래 헬퍼들은 동기(blocking) DB 작업을 모아둔 함수입니다.
# psycopg2는 동기 라이브러리라 async 엔드포인트 안에서 그냥 호출하면 이벤트 루프 전체가 멈춰
# (= 방송 화면의 그림 재생도 같이 멈춰) 버립니다. 그래서 asyncio.to_thread로 스레드에서 실행합니다.
def _fetch_recent_donations():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT id, donor_name, donor_email, donor_naver_email, drawing_title, timestamp, chzzk_channel_id, chzzk_nickname FROM ledger ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    # email = 네이버 앱별 고유 식별자(차단 기준), naverEmail = 실제 메일 주소(참고용)
    return [{"id": r["id"], "name": r["donor_name"], "email": r["donor_email"],
             "naverEmail": r["donor_naver_email"] or "", "title": r["drawing_title"],
             "time": _kst_str(r["timestamp"]),
             "chzzkNickname": r["chzzk_nickname"] or "", "chzzkChannelId": r["chzzk_channel_id"] or ""}
            for r in rows]

def _fetch_replay_row(ledger_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT donor_name, donor_profile_image, drawing_title, drawing_data FROM ledger WHERE id = %s", (ledger_id,))
    row = cursor.fetchone()
    conn.close()
    return row

def _fetch_replay_ids_from(ledger_id):
    # ✨ 순차재생: 이 id부터 최신까지의 id 목록만 가볍게 조회(그림 데이터는 재생 순서가 됐을 때 하나씩 불러온다)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM ledger WHERE id >= %s ORDER BY id ASC", (ledger_id,))
    ids = [r[0] for r in cursor.fetchall()]
    conn.close()
    return ids

def _count_since(target_str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ledger WHERE timestamp >= %s", (target_str,))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def _insert_ledger(email, name, profile_image, title, drawing_history, naver_email="",
                   chzzk_channel_id=None, chzzk_nickname=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ledger (donor_email, donor_name, donor_profile_image, drawing_title, drawing_data, donor_naver_email, chzzk_channel_id, chzzk_nickname) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (email, name, profile_image, title, json.dumps(drawing_history), naver_email,
         chzzk_channel_id, chzzk_nickname)
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
    return [{"id": r["id"], "name": r["donor_name"], "title": r["drawing_title"], "time": _kst_str(r["timestamp"])} for r in rows]

# ---------- 치지직 연동 DB 헬퍼 ----------
def _chzzk_save_link(naver_id, channel_id, nickname):
    """네이버 식별자 ↔ 치지직 채널 연결 저장(있으면 갱신). 과거 후원 기록에도 닉네임을 소급 반영한다."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO chzzk_links (naver_id, channel_id, nickname, updated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (naver_id) DO UPDATE
        SET channel_id = EXCLUDED.channel_id, nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP
    """, (naver_id, channel_id, nickname))
    # ✨ 소급 적용: 이 사람이 예전에 보낸 그림들에도 치지직 닉네임을 채운다.
    #    (연동 전 기록은 원래 알 수 없던 값이라, 연동하는 순간에만 채울 수 있다)
    cursor.execute(
        "UPDATE ledger SET chzzk_channel_id = %s, chzzk_nickname = %s WHERE donor_email = %s",
        (channel_id, nickname, naver_id)
    )
    backfilled = cursor.rowcount
    conn.commit()
    conn.close()
    return backfilled

def _chzzk_get_link(naver_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT channel_id, nickname FROM chzzk_links WHERE naver_id = %s", (naver_id,))
    row = cursor.fetchone()
    conn.close()
    return {"channelId": row[0], "nickname": row[1]} if row else None

def _chzzk_save_token(role, access_token, refresh_token, expires_in):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO chzzk_tokens (role, access_token, refresh_token, expires_at)
        VALUES (%s, %s, %s, NOW() + (%s * INTERVAL '1 second'))
        ON CONFLICT (role) DO UPDATE
        SET access_token = EXCLUDED.access_token, refresh_token = EXCLUDED.refresh_token,
            expires_at = EXCLUDED.expires_at
    """, (role, access_token, refresh_token, int(expires_in or 86400)))
    conn.commit()
    conn.close()

def _chzzk_get_token(role):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT access_token, refresh_token, (expires_at <= NOW() + INTERVAL '60 seconds') FROM chzzk_tokens WHERE role = %s",
        (role,))
    row = cursor.fetchone()
    conn.close()
    return {"accessToken": row[0], "refreshToken": row[1], "expired": bool(row[2])} if row else None

# ✨ 그림 보관 기간(일). 이 기간이 지난 원장 기록은 자동 삭제된다.
#    drawing_data(그림 전체)가 커서 무한 보관은 어렵고, 값만 바꾸면 기간을 조절할 수 있다.
DRAWING_RETENTION_DAYS = 7

def _delete_old_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM ledger WHERE timestamp <= NOW() - (%s * INTERVAL '1 day')",
        (DRAWING_RETENTION_DAYS,)
    )
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
    return [{"id": r["id"], "name": r["name"], "email": r["email"], "message": r["message"], "time": _kst_str(r["timestamp"]), "is_read": bool(r["is_read"])} for r in rows]

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
    chzzk_restrict: bool = False   # ✨ 차단/해제 시 치지직 활동제한도 함께 적용할지 (레거시)
    require_chzzk: bool = None     # ✨ 치지직 연동 필수 여부

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
        notice_text=data.notice_text,
        require_chzzk=data.require_chzzk
    )

    # ✨ 차단은 '입장 시점'에만 검사되므로, 이미 합작방에 들어와 있는 사람은 그대로 그리고 있게 된다.
    #    차단하는 즉시 해당 계정을 방에서 내보낸다.
    if data.add_blocked_email:
        await _kick_user_from_rooms(data.add_blocked_email)

    # ✨ 치지직 활동제한 동시 적용(선택). 치지직을 연동한 사람만 채널ID를 알 수 있어 가능하다.
    chzzk_result = None
    if data.chzzk_restrict and (data.add_blocked_email or data.remove_blocked_email):
        target_naver_id = data.add_blocked_email or data.remove_blocked_email
        link = await asyncio.to_thread(_chzzk_get_link, target_naver_id)
        if not link:
            chzzk_result = {"ok": False, "message": "이 사용자는 치지직 계정을 연동하지 않아 활동제한을 걸 수 없습니다."}
        else:
            ok, msg = await _chzzk_restrict(link["channelId"], remove=bool(data.remove_blocked_email))
            chzzk_result = {"ok": ok, "message": msg, "nickname": link.get("nickname") or ""}
            print(f"[CHZZK] 활동제한 {'해제' if data.remove_blocked_email else '등록'} "
                  f"channel={link['channelId']} ok={ok} msg={msg}")

    return {"message": "success", "chzzk": chzzk_result}

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

# ---------- 치지직 로그인 세션 ----------
# 치지직 인증은 서버에서 코드를 교환하므로, 결과를 브라우저에 유지하려면 세션이 필요하다.
# 별도 저장소 없이 서명된 쿠키 하나로 처리한다(재배포해도 로그인이 풀리지 않도록 비밀키는 환경변수 기반).
import hmac as _hmac, hashlib as _hashlib, base64 as _b64

SESSION_SECRET = (os.getenv("SESSION_SECRET") or CHZZK_CLIENT_SECRET or CREATOR_PASSWORD or "chzzk-fallback").encode()
CHZZK_COOKIE = "chzzk_session"

def _sign_session(payload: dict) -> str:
    raw = _b64.urlsafe_b64encode(json.dumps(payload, ensure_ascii=False).encode()).decode()
    sig = _hmac.new(SESSION_SECRET, raw.encode(), _hashlib.sha256).hexdigest()[:32]
    return f"{raw}.{sig}"

def _read_session(cookie: str):
    if not cookie or "." not in cookie:
        return None
    raw, _, sig = cookie.rpartition(".")
    expect = _hmac.new(SESSION_SECRET, raw.encode(), _hashlib.sha256).hexdigest()[:32]
    if not _hmac.compare_digest(sig, expect):
        return None      # 위조된 쿠키
    try:
        return json.loads(_b64.urlsafe_b64decode(raw.encode()).decode())
    except Exception:
        return None

async def _chzzk_channel_info(channel_id):
    """채널 이미지·팔로워 수 조회. Client 인증만 필요하므로 유저 토큰 없이 호출 가능."""
    if not (CHZZK_CLIENT_ID and CHZZK_CLIENT_SECRET):
        return {}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{CHZZK_API_BASE}/open/v1/channels",
                                 params={"channelIds": channel_id},
                                 headers={"Client-Id": CHZZK_CLIENT_ID,
                                          "Client-Secret": CHZZK_CLIENT_SECRET,
                                          "Content-Type": "application/json"})
        if r.status_code != 200:
            return {}
        body = r.json()
        data = body.get("content", body)
        items = data.get("data") if isinstance(data, dict) else data
        if isinstance(items, list) and items:
            return items[0]
    except Exception as e:
        print(f"[CHZZK] 채널 정보 조회 실패: {e}")
    return {}

# ---------- 치지직 OAuth / API ----------
# state는 서버 메모리에 잠깐 보관한다(재시작 시 사라지지만 사용자가 다시 누르면 되므로 무해).
_chzzk_pending = {}

def _chzzk_redirect_uri(request: Request):
    # 등록된 '로그인 리디렉션 URL'과 정확히 일치해야 한다.
    return str(request.base_url).rstrip("/") + "/chzzk/callback"

async def _chzzk_token_request(role, payload):
    client_id, client_secret = _chzzk_creds(role)
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="치지직 CLIENT_ID/SECRET 환경변수가 설정되지 않았습니다.")
    body = dict(payload, clientId=client_id, clientSecret=client_secret)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{CHZZK_API_BASE}/auth/v1/token", json=body,
                              headers={"Content-Type": "application/json"})
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"치지직 토큰 요청 실패({r.status_code}): {r.text[:200]}")
    data = r.json()
    return data.get("content", data)   # 응답이 content로 감싸져 오는 경우 대비

async def _chzzk_access_token(role):
    """저장된 토큰을 돌려주되, 만료됐으면 refresh로 갱신한다."""
    tok = await asyncio.to_thread(_chzzk_get_token, role)
    if not tok:
        return None
    if not tok["expired"]:
        return tok["accessToken"]
    content = await _chzzk_token_request(role, {
        "grantType": "refresh_token", "refreshToken": tok["refreshToken"]})
    await asyncio.to_thread(_chzzk_save_token, role, content.get("accessToken"),
                            content.get("refreshToken"), content.get("expiresIn"))
    return content.get("accessToken")

@app.get("/chzzk/login")
async def chzzk_login(request: Request, role: str = "viewer", userId: str = "", pw: str = ""):
    """치지직 인증 시작. viewer=시청자 계정 연결, streamer=방송인 권한 위임(활동제한용)."""
    if role not in ("viewer", "streamer"):
        raise HTTPException(status_code=400, detail="role이 올바르지 않습니다.")
    if role == "streamer" and pw != CREATOR_PASSWORD:
        raise HTTPException(status_code=403, detail="방송인 비밀번호가 올바르지 않습니다.")

    client_id, _ = _chzzk_creds(role)
    if not client_id:
        raise HTTPException(status_code=500, detail="치지직 CLIENT_ID 환경변수가 설정되지 않았습니다.")

    state = _secrets.token_urlsafe(16)
    _chzzk_pending[state] = {"role": role, "naverId": userId.strip()[:100]}
    if len(_chzzk_pending) > 500:      # 오래된 항목 정리(누수 방지)
        for k in list(_chzzk_pending)[:200]:
            _chzzk_pending.pop(k, None)

    from urllib.parse import urlencode
    q = urlencode({"clientId": client_id, "redirectUri": _chzzk_redirect_uri(request), "state": state})
    return RedirectResponse(f"{CHZZK_AUTH_URL}?{q}")

@app.get("/chzzk/callback")
async def chzzk_callback(request: Request, code: str = "", state: str = ""):
    pending = _chzzk_pending.pop(state, None)
    if not pending or not code:
        return HTMLResponse("<h3>인증 정보가 유효하지 않습니다. 다시 시도해 주세요.</h3>", status_code=400)
    role = pending["role"]

    content = await _chzzk_token_request(role, {
        "grantType": "authorization_code", "code": code, "state": state})
    access_token = content.get("accessToken")
    if not access_token:
        return HTMLResponse("<h3>치지직 토큰 발급에 실패했습니다.</h3>", status_code=502)

    await asyncio.to_thread(_chzzk_save_token, role, access_token,
                            content.get("refreshToken"), content.get("expiresIn"))

    # 본인 채널 정보 조회 (유저 조회 scope)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(f"{CHZZK_API_BASE}/open/v1/users/me",
                             headers={"Authorization": f"Bearer {access_token}"})
    if r.status_code != 200:
        return HTMLResponse(
            f"<h3>치지직 유저 정보 조회 실패({r.status_code})</h3>"
            f"<p>앱에 '유저 조회' 권한이 포함되어 있는지 확인해 주세요.</p>", status_code=502)
    me = r.json()
    me = me.get("content", me)
    channel_id, nickname = me.get("channelId"), me.get("channelName")

    if role == "streamer":
        print(f"[CHZZK] 방송인 연동 완료 channel={channel_id} name={nickname}")
        return HTMLResponse(
            f"<h3>치지직 방송인 연동이 완료되었습니다.</h3><p>채널: {nickname}</p>"
            "<p>이 창을 닫고 관리자 페이지로 돌아가세요.</p>")

    # 프로필 이미지·팔로워 수는 채널 조회로 보강 (users/me는 채널ID/이름만 준다)
    info = await _chzzk_channel_info(channel_id)
    profile_image = info.get("channelImageUrl") or ""

    # 기존 네이버 계정에서 넘어온 경우(naverId가 있으면) 연결을 저장해 과거 기록에 닉네임을 소급한다.
    backfilled = 0
    if pending.get("naverId"):
        backfilled = await asyncio.to_thread(_chzzk_save_link, pending["naverId"], channel_id, nickname)
    # 치지직 자체를 신원으로 쓰는 경우에도 링크를 남겨둔다(자기 자신 매핑 — 조회 경로 통일용)
    await asyncio.to_thread(_chzzk_save_link, channel_id, channel_id, nickname)

    print(f"[CHZZK] 로그인 channel={channel_id} name={nickname} 소급={backfilled}건")

    # ✨ 서명 쿠키로 로그인 상태를 유지하고 그리기 화면으로 돌려보낸다.
    resp = RedirectResponse(str(request.base_url).rstrip("/") + "/draw")
    resp.set_cookie(
        CHZZK_COOKIE,
        _sign_session({"channelId": channel_id, "channelName": nickname, "image": profile_image}),
        max_age=60 * 60 * 24 * 30, httponly=True, samesite="lax",
        secure=str(request.base_url).startswith("https"))
    return resp

@app.get("/api/chzzk/me")
async def chzzk_me(request: Request):
    """현재 로그인한 치지직 사용자. 그리기 화면이 이걸로 로그인 여부를 판단한다."""
    sess = _read_session(request.cookies.get(CHZZK_COOKIE, ""))
    if not sess:
        return {"loggedIn": False, "configured": bool(CHZZK_CLIENT_ID)}
    return {"loggedIn": True, "configured": bool(CHZZK_CLIENT_ID),
            "channelId": sess.get("channelId"), "channelName": sess.get("channelName"),
            "image": sess.get("image", "")}

@app.post("/api/chzzk/logout")
async def chzzk_logout():
    resp = JSONResponse({"message": "success"})
    resp.delete_cookie(CHZZK_COOKIE)
    return resp

@app.get("/api/chzzk/status")
async def chzzk_status(userId: str = ""):
    """시청자 본인의 연동 상태 조회(그리기 화면에서 버튼 표시용)."""
    link = await asyncio.to_thread(_chzzk_get_link, userId.strip()[:100]) if userId else None
    settings = await asyncio.to_thread(get_db_settings)
    return {"configured": bool(CHZZK_CLIENT_ID), "linked": bool(link),
            "required": bool(settings.get("require_chzzk")),
            "nickname": (link or {}).get("nickname", ""), "channelId": (link or {}).get("channelId", "")}

class ChzzkRestrictRequest(BaseModel):
    userId: str = ""        # 네이버 식별자(원장의 고유ID). 연동 정보를 통해 채널을 찾는다.
    channelId: str = ""     # 치지직 채널ID를 직접 아는 경우
    remove: bool = False    # True면 활동제한 해제

@app.post("/api/chzzk/restrict")
async def chzzk_restrict_api(data: ChzzkRestrictRequest):
    """✨ 그림툴 차단과 별개로, 치지직 활동제한만 단독으로 걸거나 푼다."""
    channel_id = (data.channelId or "").strip()
    nickname = ""
    if not channel_id:
        naver_id = (data.userId or "").strip()
        if not naver_id:
            raise HTTPException(status_code=400, detail="대상을 지정해주세요.")
        link = await asyncio.to_thread(_chzzk_get_link, naver_id)
        if not link:
            raise HTTPException(status_code=404,
                                detail="이 사용자는 치지직 계정을 연동하지 않아 활동제한을 걸 수 없습니다.")
        channel_id = link["channelId"]
        nickname = link.get("nickname") or ""

    ok, msg = await _chzzk_restrict(channel_id, remove=data.remove)
    print(f"[CHZZK] 단독 활동제한 {'해제' if data.remove else '등록'} channel={channel_id} ok={ok} msg={msg}")
    if not ok:
        raise HTTPException(status_code=502, detail=msg)
    return {"message": "success", "channelId": channel_id, "nickname": nickname, "removed": data.remove}

async def _chzzk_restrict(target_channel_id, remove=False):
    """방송인 채널에 활동제한 등록/해제. 성공 시 (True, 메시지)."""
    token = await _chzzk_access_token("streamer")
    if not token:
        return False, "방송인 치지직 연동이 되어 있지 않습니다."
    method = "DELETE" if remove else "POST"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.request(method, f"{CHZZK_API_BASE}/open/v1/restrict-channels",
                                 json={"targetChannelId": target_channel_id},
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    if r.status_code == 200:
        return True, "성공"
    return False, f"치지직 응답 {r.status_code}: {r.text[:150]}"

@app.post("/api/replay-donation/{ledger_id}")
async def replay_donation(ledger_id: int):
    row = await asyncio.to_thread(_fetch_replay_row, ledger_id)
    if not row: raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")
    await drawing_queue.put({"name": row[0], "profileImage": row[1], "title": row[2], "drawingData": json.loads(row[3])})
    return {"message": "success"}

@app.post("/api/replay-from/{ledger_id}")
async def replay_from(ledger_id: int):
    # ✨ 이 그림부터 최신 그림까지 순차 재생. 큐에는 가벼운 마커만 넣고, 처리기가 재생 순서가 됐을 때
    #    실제 그림 데이터를 하나씩 불러온다(전부 메모리에 올리지 않아 안전).
    ids = await asyncio.to_thread(_fetch_replay_ids_from, ledger_id)
    if not ids:
        raise HTTPException(status_code=404, detail="재생할 그림이 없습니다.")
    for i in ids:
        await drawing_queue.put({"__replay_id": i})
    return {"message": "success", "count": len(ids)}

@app.post("/api/replay-clear")
async def replay_clear():
    # ✨ 순차 재생 중지: 대기 중인 '재생 마커'만 큐에서 제거(정상 후원은 유지)하고 현재 재생을 스킵한다.
    global skip_current_drawing
    removed = 0
    kept = []
    try:
        while True:
            item = drawing_queue.get_nowait()
            drawing_queue.task_done()
            if isinstance(item, dict) and item.get("__replay_id"):
                removed += 1
            else:
                kept.append(item)
    except asyncio.QueueEmpty:
        pass
    for item in kept:
        drawing_queue.put_nowait(item)

    skip_current_drawing = True
    for connection in active_connections:
        try: await connection.send_json({"type": "clear"})
        except: pass
    return {"message": "success", "removed": removed}

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
        # ✨ 신원은 서버 세션(치지직 로그인)에서 읽는다 — 클라이언트가 보낸 값은 위조가 가능하다.
        #    치지직 미설정 시에는 기존(네이버 식별자) 방식으로 폴백해 배포 전에도 동작이 끊기지 않는다.
        _sess = _read_session(request.cookies.get(CHZZK_COOKIE, ""))
        if _sess:
            email = _sess["channelId"]          # 신원 = 치지직 채널ID (차단 기준)
            name = _sess.get("channelName") or "치지직 시청자"
        elif CHZZK_CLIENT_ID:
            raise HTTPException(status_code=401, detail="치지직 로그인이 필요합니다.")
        else:
            email = data.get("email")   # ⚠️ 이름은 email이지만 실제로는 네이버 앱별 고유 식별자(getId).
            name = data.get("name")
        # ✨ 프로필 이미지도 세션(치지직 채널 이미지)을 우선한다.
        #    클라이언트가 보낸 값을 그대로 쓰면 남의 프사로 바꿔 보낼 수 있다.
        #    단, '프사 전송' 체크를 끄면 클라이언트가 빈 값을 보내므로 그 의사는 존중한다.
        _client_img = data.get("profileImage", "")
        if _sess:
            profile_image = (_sess.get("image", "") if _client_img else "")
        else:
            profile_image = _client_img
        title = data.get("title", "제목없음")
        drawing_history = data.get("drawingData")
        naver_email = (data.get("naverEmail") or "").strip()[:200]   # ✨ 실제 메일 주소(참고용)

        if not email: raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
        # 차단 판정은 계속 식별자 기준 (기존 블랙리스트가 그대로 유효)
        if email in settings["blocked_emails"]: raise HTTPException(status_code=403, detail="차단된 계정입니다.")

        # ✨ 치지직을 연동한 사람이면 그 닉네임으로 표시한다(연동 안 했으면 기존 이름 그대로).
        chzzk_link = await asyncio.to_thread(_chzzk_get_link, email)
        # ✨ '치지직 연동 필수'가 켜져 있으면 연동하지 않은 사람은 보낼 수 없다.
        if settings.get("require_chzzk") and not chzzk_link:
            raise HTTPException(status_code=403,
                                detail="치지직 계정 연결이 필요합니다. 그리기 화면의 [치지직 연결] 버튼을 눌러주세요.")
        chzzk_channel_id = chzzk_nickname = None
        if chzzk_link:
            chzzk_channel_id = chzzk_link["channelId"]
            chzzk_nickname = chzzk_link["nickname"]
            if chzzk_nickname:
                name = chzzk_nickname
                data["name"] = chzzk_nickname   # 방송 화면 알림도 치지직 닉네임으로

        await asyncio.to_thread(_insert_ledger, email, name, profile_image, title, drawing_history,
                                naver_email, chzzk_channel_id, chzzk_nickname)

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

            # ✨ 순차재생 마커면 이 시점에 실제 그림 데이터를 불러온다(한 번에 하나만 메모리에 로드)
            if isinstance(payload, dict) and payload.get("__replay_id"):
                row = await asyncio.to_thread(_fetch_replay_row, payload["__replay_id"])
                if not row:
                    continue   # 삭제된 그림이면 건너뜀 (finally에서 task_done 처리)
                payload = {"name": row[0], "profileImage": row[1], "title": row[2], "drawingData": json.loads(row[3])}

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
    return {"id": row["id"], "name": row["donor_name"], "title": row["drawing_title"], "data": json.loads(row["drawing_data"]), "time": _kst_str(row["timestamp"])}

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
async def create_room(data: RoomCreate, request: Request):
    global _room_seq
    title = (data.title or "").strip()[:60]
    if not title:
        raise HTTPException(status_code=400, detail="방 제목을 입력해주세요.")

    # ✨ 방 생성도 로그인 + 차단 검사. 입장만 막으면 차단된 사람이 빈 방을 계속 만들어
    #    방 목록을 오염시키고 MAX_ROOMS를 고갈시킬 수 있다.
    _csess = _read_session(request.cookies.get(CHZZK_COOKIE, "")) if request else None
    if _csess:
        user_id = _csess["channelId"]
    elif CHZZK_CLIENT_ID:
        raise HTTPException(status_code=401, detail="치지직 로그인이 필요합니다.")
    else:
        user_id = (data.userId or "").strip()[:100]
    if not user_id:
        raise HTTPException(status_code=401, detail="합작방은 로그인 후 이용할 수 있습니다.")
    settings = await asyncio.to_thread(get_db_settings)
    if user_id in settings.get("blocked_emails", []):
        raise HTTPException(status_code=403, detail="차단된 계정입니다.")

    # ✨ 계정당 '아직 아무도 안 들어온 방'은 1개까지. 없으면 유령 방을 30개까지 찍어
    #    MAX_ROOMS를 고갈시켜 로비를 마비시킬 수 있다. 기존 미사용 방이 있으면 그걸 돌려준다.
    for r in rooms.values():
        if not r["participants"] and r.get("creator") == user_id:
            return {"id": r["id"], "title": r["title"]}

    if len(rooms) >= MAX_ROOMS:
        raise HTTPException(status_code=429, detail="방이 너무 많습니다. 잠시 후 다시 시도해주세요.")
    _room_seq += 1
    rid = f"room_{_room_seq}"
    rooms[rid] = {
        "id": rid,
        "title": title,
        "frames": [{"id": "frame_1", "duration": 500}],
        "participants": {},   # clientId -> {"name", "layerId", "ws", "userId"}
        "empty_since": datetime.now(),
        "creator": user_id,
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
    # ✨ '다같이 보내기': 제안 시점의 명단(roster) 전원이 동의해야 송출한다.
    #    '현재 참가자' 기준으로 판정하면, 동의하지 않은 사람이 나가는 순간 분모가 줄어
    #    제안자 혼자만 남아도 조건이 자동 충족돼 버린다(= 안 누르고 나가면 오히려 통과).
    ps = room.get("pending_send")
    if not ps or not room["participants"]:
        return
    roster = ps["roster"] & set(room["participants"].keys())   # 아직 방에 남아 있는 명단
    if not roster or not roster <= ps["consents"]:
        return
    # 조립·전송은 '호스트(가장 먼저 들어온 사람)'가 맡는다. 방 시작부터의 전 과정을 갖고 있는 유일한 참가자이기 때문.
    assembler = next(iter(room["participants"]))

    # ✨ 발사 직전 참가자 전원의 차단 여부를 다시 확인한다.
    #    합작 송출은 조립자(호스트) 1인의 식별자로 제출되므로, 차단된 사람이 방에 남아 있으면
    #    그 사람 그림이 호스트 이름으로 방송에 나가는 '밴 세탁'이 된다.
    try:
        s = await asyncio.to_thread(get_db_settings)
        blocked = set(s.get("blocked_emails", []))
        if blocked:
            for cid, p in room["participants"].items():
                if p.get("userId") in blocked:
                    room["pending_send"] = None
                    await _room_broadcast(room, json.dumps({
                        "type": "send_cancel", "reason": f"차단된 참가자({p['name']})가 있어 송출을 취소했습니다."
                    }))
                    return
    except Exception as e:
        print(f"[ROOM] 송출 전 차단 확인 실패(진행): {e}")

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
        # ✨ userId(네이버 앱별 고유 식별자)는 서버에만 보관하고 다른 참가자에게는 브로드캐스트하지 않는다.
        #    (누가 방에 있었는지 남겨 악용 대응에 쓰기 위함)
        # ✨ 신원은 서버 세션(치지직)에서 읽는다 — 클라이언트가 보낸 userId는 위조가 가능하다.
        _rsess = _read_session(websocket.cookies.get(CHZZK_COOKIE, ""))
        if _rsess:
            user_id = _rsess["channelId"]
        elif CHZZK_CLIENT_ID:
            await websocket.send_text(json.dumps({"type": "error", "message": "치지직 로그인이 필요합니다."}))
            await websocket.close()
            return
        else:
            user_id = (first.get("userId") or "").strip()[:100]
        if not user_id:
            await websocket.send_text(json.dumps({"type": "error", "message": "합작방은 로그인 후 이용할 수 있습니다."}))
            await websocket.close()
            return

        # ✨ '치지직 연동 필수'가 켜져 있으면 합작방도 연동해야 들어올 수 있다(제출 경로와 동일 기준).
        try:
            _rs = await asyncio.to_thread(get_db_settings)
            if _rs.get("require_chzzk"):
                _lk = await asyncio.to_thread(_chzzk_get_link, user_id)
                if not _lk:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "치지직 계정 연결이 필요합니다. 그리기 화면의 [치지직 연결] 버튼을 눌러주세요."}))
                    await websocket.close()
                    return
        except Exception as e:
            print(f"[ROOM] 치지직 필수 검사 실패(입장 허용): {e}")

        # ✨ 크리에이터 페이지에서 차단된 계정은 합작방에도 들어올 수 없다.
        #    (blocked_emails는 이름과 달리 '네이버 앱별 고유 식별자' 목록 — 후원 전송과 같은 기준)
        #    ※ 이 DB 조회는 await이므로, 아래 '등록'까지의 검사는 반드시 이 뒤에서 다시 해야 한다.
        try:
            room_settings = await asyncio.to_thread(get_db_settings)
            if user_id in room_settings.get("blocked_emails", []):
                await websocket.send_text(json.dumps({"type": "error", "message": "차단된 계정입니다."}))
                await websocket.close()
                return
        except Exception as e:
            # 차단 목록을 못 읽으면(DB 장애) 합작방을 통째로 막지는 않되, 로그는 남긴다.
            print(f"[ROOM] 차단 목록 조회 실패(입장은 허용): {e}")

        # 2) 참가자 등록 — 여기부터 등록까지 await가 없어야 원자적이다.
        #    (위 DB await 동안 마지막 참가자가 나가 방이 삭제됐을 수 있고, 정원도 바뀔 수 있다)
        if rooms.get(room_id) is not room:
            await websocket.send_text(json.dumps({"type": "error", "message": "방이 종료되었습니다."}))
            await websocket.close()
            return

        # 같은 계정이 여러 탭으로 중복 입장하면 정원·레이어·송출 정족수를 혼자 잠식하므로,
        # 기존 세션을 정리하고 새 접속으로 교체한다(새로고침 복구도 이 경로로 자연스럽게 동작).
        for old_cid, old_p in list(room["participants"].items()):
            if old_p.get("userId") == user_id:
                room["participants"].pop(old_cid, None)
                try: await old_p["ws"].close()
                except: pass
                print(f"[ROOM] replaced duplicate session room={room['id']} old={old_cid} userId={user_id}")

        if len(room["participants"]) >= MAX_PARTICIPANTS_PER_ROOM:
            await websocket.send_text(json.dumps({"type": "error", "message": "방 인원이 가득 찼습니다."}))
            await websocket.close()
            return

        host_id = next(iter(room["participants"]), None)   # 기존 최초 참가자 = 상태 제공자
        _client_seq += 1
        client_id = f"c{_client_seq}"
        name = (first.get("name") or "").strip()[:20] or "익명"
        # ✨ 치지직을 연동했다면 합작방 이름도 치지직 닉네임으로 고정한다.
        #    (합작 송출은 참가자 이름이 그대로 방송에 나가므로, 여기서 막지 않으면 이름을 바꿔 보낼 수 있다)
        try:
            _link = await asyncio.to_thread(_chzzk_get_link, user_id)
            if _link and _link.get("nickname"):
                name = _link["nickname"][:20]
        except Exception as e:
            print(f"[ROOM] 치지직 닉네임 조회 실패(입력값 사용): {e}")
        layer_id = f"rlayer_{client_id}"
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
                    # ✨ 제안 시점 명단을 고정한다. 이후 들어온 사람은 동의창을 못 봤으므로 정족수에서 제외하고,
                    #    명단에 있는 사람이 동의 없이 나가면 '통과'가 아니라 '취소'로 처리한다.
                    "roster": set(room["participants"].keys()),
                }
                await _room_broadcast(room, json.dumps({
                    "type": "send_request",
                    "requesterId": client_id, "requesterName": name,
                    "title": room["pending_send"]["title"],
                    "agreed": 1, "total": len(room["pending_send"]["roster"]),
                }))
                await _room_maybe_send_go(room)

            elif mtype == "send_consent":
                ps = room.get("pending_send")
                if not ps:
                    continue
                if msg.get("accept"):
                    ps["consents"].add(client_id)
                    roster_now = ps["roster"] & set(room["participants"].keys())
                    await _room_broadcast(room, json.dumps({
                        "type": "send_progress",
                        "agreed": len(ps["consents"] & roster_now),
                        "total": len(roster_now),
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
                    elif client_id in ps["roster"] and client_id not in ps["consents"]:
                        # ✨ 동의하지 않은 사람이 나갔다 → 그대로 두면 분모가 줄어 자동 통과가 되므로 취소한다.
                        room["pending_send"] = None
                        await _room_broadcast(room, json.dumps({
                            "type": "send_cancel", "reason": "동의하지 않은 참가자가 나가서 취소되었습니다."
                        }))
                    else:
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