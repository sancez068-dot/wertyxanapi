import os, json, hmac, hashlib, secrets, time, asyncio, re, traceback
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode
from typing import Optional, Any

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Header, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse
from pydantic import BaseModel, Field
from urllib.parse import urlparse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ParseMode
from telegram.ext import Application as TelegramApplication, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest
from telegram.error import TimedOut

APP_DIR = os.path.dirname(os.path.abspath(__file__))
from pathlib import Path as _Path

def _load_local_env():
    _env_file = _Path(__file__).with_name("1.txt")
    if not _env_file.exists():
        return
    for _raw in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _raw.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _key, _value = _line.split("=", 1)
        _key = _key.strip()
        _value = _value.strip()
        if _key and _key not in os.environ:
            os.environ[_key] = _value

_load_local_env()

PORT = int(os.getenv("PORT", "8085"))
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = ""
TELEGRAM_WEBHOOK_ENABLED = os.getenv("TELEGRAM_WEBHOOK_ENABLED", "false").lower() == "true"
DEV_MODE = os.getenv("DEV_MODE", "false").lower() == "true"
DEV_USER_ID = int(os.getenv("DEV_USER_ID", "123456789"))
BOOTSTRAP_OWNER = int(os.getenv("BOOTSTRAP_OWNER", "611243766"))
BOOTSTRAP_ADMIN = int(os.getenv("BOOTSTRAP_ADMIN", "8373707271"))
ESCROW_USERNAME = "FunRelayer"
TG_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
TG_WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")
TG_INITDATA_MAX_AGE = int(os.getenv("TG_INITDATA_MAX_AGE", "86400"))
TON_API_URL = os.getenv("TON_API_URL", "https://tonapi.io").rstrip("/")
TON_API_KEY = os.getenv("TON_API_KEY", "")
TON_DEPOSIT_ADDRESS = os.getenv("TON_DEPOSIT_ADDRESS", "")
TRON_API_URL = os.getenv("TRON_API_URL", "https://api.trongrid.io").rstrip("/")
TRON_API_KEY = os.getenv("TRON_API_KEY", "")
USDT_DEPOSIT_ADDRESS = os.getenv("USDT_DEPOSIT_ADDRESS", "")
USDT_CONTRACT = os.getenv("USDT_CONTRACT", "")
CHAIN_SCAN_MS = max(int(os.getenv("CHAIN_SCAN_MS", "30000")), 10000)
CURRENCIES = ["TON","USDT","STARS","RUB","USD","EUR","GBP","CNY","JPY","TRY","UAH","KZT","BTC","ETH"]
LANGUAGES = ["ru","en","es","zh","ar","hi","pt","ja","de","fr","uk","tr"]
DEFAULT_RATES = {"TON":500,"USDT":95,"STARS":1.5,"BTC":6000000,"ETH":250000,"RUB":1,"USD":95,"EUR":103,"GBP":120,"CNY":13,"JPY":0.6,"TRY":2.8,"UAH":2.3,"KZT":0.2}

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL or SUPABASE_DATABASE_URL is required")

app = FastAPI(title="Wertyxan API", version="4.0.0", docs_url="/docs", redoc_url="/redoc")
app.add_middleware(CORSMiddleware, allow_origins=[x.strip() for x in os.getenv("CORS_ORIGINS", "*").split(",") if x.strip()] or ["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

LOG_FILE = _Path(APP_DIR) / "9999.txt"
def write_log(message):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now(timezone.utc).isoformat()}] {message}\n")
    except Exception:
        pass

@app.middleware("http")
async def request_logger(request: Request, call_next):
    try:
        response = await call_next(request)
        if response.status_code >= 400:
            write_log(f"HTTP {response.status_code} {request.method} {request.url.path} query={dict(request.query_params)}")
        return response
    except Exception as exc:
        write_log(f"EXCEPTION {request.method} {request.url.path}: {exc}\n{traceback.format_exc()}")
        raise

@app.get("/9999.txt")
async def download_logs():
    if not LOG_FILE.exists():
        LOG_FILE.write_text("No logs yet\n", encoding="utf-8")
    return PlainTextResponse(LOG_FILE.read_text(encoding="utf-8", errors="replace"))


pool: Optional[asyncpg.Pool] = None
sse_clients: dict[int, set[Any]] = {}
ws_clients: dict[int, set[WebSocket]] = {}
rate_buckets: dict[str, tuple[float,int]] = {}

class AnyBody(BaseModel):
    model_config = {"extra":"allow"}

class StatusBody(BaseModel):
    status: str
    resolution: Optional[str] = None

class SettingBody(BaseModel):
    key: Optional[str] = None
    value: Any = None

class BroadcastBody(BaseModel):
    text: str
    target: str = "all"
    parse_mode: str = "HTML"

class MessageBody(BaseModel):
    message: str


def dec(v, default=Decimal("0")):
    try: return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError): return default

def jsonable(v):
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, datetime): return v.isoformat()
    if isinstance(v, dict): return {k:jsonable(x) for k,x in v.items()}
    if isinstance(v, list): return [jsonable(x) for x in v]
    return v

def rowdict(r): return jsonable(dict(r)) if r else None

def clean(v, n=4000): return str(v or "").strip()[:n]
def valid_currency(c): return str(c or "").upper() in CURRENCIES

def deal_public(r):
    d = rowdict(r) or {}
    for k in ("id","creator_id","user_id","seller_id","buyer_id","dispute_id"):
        if d.get(k) is not None: d[k] = int(d[k])
    for k in ("amount","fee","escrow_amount","payment_amount","payment_fee"):
        if d.get(k) is not None: d[k] = float(d[k])
    return d

async def db():
    return pool
async def q(sql, *args):
    return await pool.fetch(sql, *args)
async def one(sql, *args):
    return await pool.fetchrow(sql, *args)
async def execute(sql, *args):
    return await pool.execute(sql, *args)

async def tx(fn):
    async with pool.acquire() as c:
        async with c.transaction(): return await fn(c)

async def ensure_user(u):
    if not u: return None
    r = await one("""INSERT INTO users(id,username,first_name,last_name,language,photo_url,last_seen_at)
        VALUES($1,$2,$3,$4,$5,$6,NOW()) ON CONFLICT(id) DO UPDATE SET
        username=COALESCE(EXCLUDED.username,users.username), first_name=COALESCE(EXCLUDED.first_name,users.first_name),
        last_name=COALESCE(EXCLUDED.last_name,users.last_name), language=COALESCE(EXCLUDED.language,users.language),
        photo_url=COALESCE(EXCLUDED.photo_url,users.photo_url), last_seen_at=NOW() RETURNING *""",
        int(u["id"]),u.get("username"),u.get("first_name"),u.get("last_name"),u.get("language_code") or u.get("language") or "en",u.get("photo_url"))
    return rowdict(r)

def tg_user(raw):
    if not raw: return None
    return {"id":int(raw["id"]),"username":raw.get("username"),"first_name":raw.get("first_name"),"last_name":raw.get("last_name"),"language_code":raw.get("language_code") or "en","photo_url":raw.get("photo_url")}

def verify_initdata(initdata):
    if not BOT_TOKEN or not initdata: return None
    try:
        pairs = dict(parse_qsl(initdata, keep_blank_values=True)); received = pairs.pop("hash", "")
        check = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(received, expected): return None
        auth_date = int(pairs.get("auth_date", "0"));
        if not auth_date or abs(time.time()-auth_date) > TG_INITDATA_MAX_AGE: return None
        return json.loads(pairs.get("user", "{}"))
    except Exception: return None

async def current_user(request: Request, required=False):
    init = request.headers.get("X-Telegram-InitData") or request.headers.get("X-Telegram-Data") or ""
    u = tg_user(verify_initdata(init))
    if not u and DEV_MODE:
        try: uid = int(request.headers.get("X-Debug-User-Id") or DEV_USER_ID)
        except: uid = DEV_USER_ID
        u = {"id":uid,"username":"debug","first_name":"Debug","language_code":"ru"}
    if not u and required: raise HTTPException(401,"Telegram authentication required")
    if u and await one("SELECT 1 FROM banned_users WHERE user_id=$1", u["id"]): raise HTTPException(status_code=403, detail={"code":"BANNED","message":"User is banned"})
    if u: await ensure_user(u)
    return u

async def require(request): return await current_user(request, True)
async def admin(request):
    u=await require(request)
    if u["id"] != BOOTSTRAP_OWNER and not await one("SELECT 1 FROM admins WHERE user_id=$1",u["id"]): raise HTTPException(403,"Forbidden")
    return u
async def owner(request):
    u=await require(request)
    if u["id"] != BOOTSTRAP_OWNER: raise HTTPException(403,"Forbidden")
    return u
async def target(request, ident):
    u=await require(request)
    tid=int(ident)
    if tid != u["id"] and not DEV_MODE: raise HTTPException(403,"You can only modify your own account")
    return u,tid

@app.middleware("http")
async def limiter(request, call_next):
    key=f"{request.client.host if request.client else 'unknown'}:{request.url.path}"
    limit=20 if any(x in request.url.path for x in ("/withdraw","/stars-invoice","/deposit")) else 120
    now=time.monotonic(); start,count=rate_buckets.get(key,(now,0))
    if now-start>=60: start,count=now,0
    count+=1; rate_buckets[key]=(start,count)
    if count>limit: return JSONResponse({"detail":"Too many requests"},status_code=429)
    try: return await call_next(request)
    except HTTPException as e: return JSONResponse({"detail":e.detail},status_code=e.status_code)
    except Exception as e:
        print("ERROR",repr(e)); return JSONResponse({"detail":"Internal server error"},status_code=500)

@app.middleware("http")
async def security_headers(request:Request, call_next):
    response=await call_next(request)
    response.headers.setdefault("X-Content-Type-Options","nosniff")
    response.headers.setdefault("X-Frame-Options","DENY")
    response.headers.setdefault("Referrer-Policy","no-referrer")
    response.headers.setdefault("Permissions-Policy","camera=(), microphone=(), geolocation=()")
    return response

async def settings():
    rows=await q("SELECT key,value FROM settings"); return {r["key"]:r["value"] for r in rows}
async def active_escrow_username():
    r=await one("SELECT username FROM escrow_accounts WHERE active=true ORDER BY created_at DESC LIMIT 1")
    if r and r["username"]: return str(r["username"]).lstrip("@")
    st=await settings()
    return str(st.get("escrow_username") or ESCROW_USERNAME).lstrip("@")
async def balances(uid):
    rows=await q("SELECT currency,balance FROM balances WHERE user_id=$1",uid); out={c:0 for c in CURRENCIES}
    for r in rows: out[r["currency"]]=float(r["balance"])
    return out
async def audit(uid, action, et=None, eid=None, meta=None):
    await execute("INSERT INTO audit_logs(user_id,action,entity_type,entity_id,metadata) VALUES($1,$2,$3,$4,$5::jsonb)",uid,action,et,str(eid) if eid is not None else None,json.dumps(jsonable(meta or {}),ensure_ascii=False))
async def ledger(c,uid,currency,amount,entry_type,description,ref_type=None,ref_id=None,metadata=None):
    amount=dec(amount)

    # Debit atomically and never allow the balances CHECK constraint to be
    # reached with a negative value. This also protects against concurrent
    # payment requests racing each other.
    if amount < 0:
        result = await c.execute(
            "UPDATE balances SET balance=balance+$3 WHERE user_id=$1 AND currency=$2 AND balance+$3>=0",
            uid,currency,amount
        )
        if result != "UPDATE 1":
            raise HTTPException(402,"Insufficient funds")
    else:
        await c.execute(
            "INSERT INTO balances(user_id,currency,balance) VALUES($1,$2,$3) "
            "ON CONFLICT(user_id,currency) DO UPDATE SET balance=balances.balance+EXCLUDED.balance",
            uid,currency,amount
        )

    await c.execute(
        "INSERT INTO ledger_entries(user_id,currency,amount,entry_type,description,reference_type,reference_id,metadata) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",
        uid,currency,amount,entry_type,description,ref_type,
        str(ref_id) if ref_id is not None else None,
        json.dumps(jsonable(metadata or {}),ensure_ascii=False)
    )
async def transaction(c,uid,typ,status,currency,amount,description,deal_tag=None,metadata=None):
    await c.execute("INSERT INTO transactions(user_id,type,status,currency,amount,description,deal_tag,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",uid,typ,status,currency,dec(amount),description,deal_tag,json.dumps(jsonable(metadata or {}),ensure_ascii=False))
async def claim_idempotency(c, user_id, endpoint, key):
    key=clean(key,128)
    if not key: return None
    existing=await c.fetchrow("SELECT response,status_code FROM idempotency_keys WHERE key=$1 FOR UPDATE",key)
    if existing:
        return {"response":existing["response"],"status_code":existing["status_code"],"replay":True}
    await c.execute("INSERT INTO idempotency_keys(key,user_id,endpoint) VALUES($1,$2,$3)",key,user_id,endpoint)
    return {"replay":False,"key":key}

async def finish_idempotency(c,key,response,status_code=200):
    if key:
        await c.execute("UPDATE idempotency_keys SET response=$1,status_code=$2 WHERE key=$3",response,status_code,key)

async def event(uid,event,payload):
    data=json.dumps(jsonable({"event":event,"payload":payload,"ts":int(time.time()*1000)}),ensure_ascii=False)
    for r in list(sse_clients.get(uid,set())):
        try: r.put_nowait(data)
        except: pass
    for ws in list(ws_clients.get(uid,set())):
        try: await ws.send_text(data)
        except: pass
async def bot_send(chat_id,text,parse_mode=None,reply_markup=None):
    if not BOT_TOKEN:return False
    try:
        payload={"chat_id":int(chat_id),"text":str(text),"disable_web_page_preview":True}
        if parse_mode: payload["parse_mode"]=parse_mode
        if reply_markup is not None: payload["reply_markup"]=reply_markup
        async with httpx.AsyncClient(timeout=15) as x:
            r=await x.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",json=payload)
            if r.is_success: return True
            if parse_mode:
                payload.pop("parse_mode",None)
                r=await x.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",json=payload)
                return r.is_success
            return False
    except Exception:return False
async def notify(uid,typ,body,title="Wertyxan",data=None):
    r=await one("INSERT INTO notifications(user_id,type,title,body,data) VALUES($1,$2,$3,$4,$5::jsonb) RETURNING id",uid,typ,title,body,json.dumps(jsonable(data or {}),ensure_ascii=False))
    sent=await bot_send(uid,body,None)
    if sent: await execute("UPDATE notifications SET telegram_sent=true WHERE id=$1",r["id"])
    await event(uid,"notification",{"id":int(r["id"]),"type":typ,"title":title,"body":body,"data":data or {},"telegram_sent":sent})
    return sent

@app.get("/health")
@app.get("/api/health")
async def health():
    try: await one("SELECT 1"); return {"ok":True,"db":True,"time":datetime.now(timezone.utc).isoformat()}
    except: raise HTTPException(503,"Database unavailable")

@app.post("/api/auth/initdata")
async def auth_initdata(request:Request):
    body=await request.json(); init=body.get("initData") or body.get("init_data") or request.headers.get("X-Telegram-InitData","")
    u=tg_user(verify_initdata(init))
    if not u and DEV_MODE: u={"id":int(body.get("user_id") or DEV_USER_ID),"username":"debug","first_name":"Debug","language_code":"ru"}
    if not u: raise HTTPException(401,"Invalid Telegram initData")
    if await one("SELECT 1 FROM banned_users WHERE user_id=$1",u["id"]):
        raise HTTPException(status_code=403, detail={"code":"BANNED","message":"User is banned"})
    dbu=await ensure_user(u); return {"success":True,"user":dbu}

@app.get("/api/user")
async def get_user(request:Request): return await require(request)
@app.get("/api/user/{ident}")
async def get_user_id(request:Request,ident:int):
    u=await require(request)
    if ident != u["id"] and u["id"] != BOOTSTRAP_OWNER and not await one("SELECT 1 FROM admins WHERE user_id=$1",u["id"]):
        raise HTTPException(403,"Forbidden")
    r=await one("SELECT id,username,first_name,last_name,language,photo_url,ton_address,usdt_address,card_requisite,created_at,last_seen_at FROM users WHERE id=$1",ident)
    if not r: raise HTTPException(404,"User not found")
    d=rowdict(r)
    req=await q("SELECT requisite_key,requisite_value FROM user_requisites WHERE user_id=$1",ident)
    d.update(language_code=d["language"],ton_connect_wallet=d["ton_address"],wallet=d["ton_address"],card=d["card_requisite"] or "",usdt_address=d["usdt_address"] or "",requisites={x["requisite_key"]:x["requisite_value"] for x in req})
    return d
@app.get("/api/user/{ident}/balance")
async def get_balance(request:Request,ident:int): await target(request,ident); return {"balance":await balances(ident)}
@app.get("/api/user/{ident}/wallet")
async def get_wallet(request:Request,ident:int): await target(request,ident); return {"wallet":await balances(ident)}
@app.get("/api/user/{ident}/ton_wallet")
async def get_ton(request:Request,ident:int): await target(request,ident); r=await one("SELECT ton_address FROM users WHERE id=$1",ident); return {"ton_wallet":r["ton_address"] or ""}
@app.put("/api/user/{ident}/ton_wallet")
async def put_ton(request:Request,ident:int):
    await target(request,ident); b=await request.json(); a=clean(b.get("address"),128)
    if a and not re.fullmatch(r"(EQ|UQ)[A-Za-z0-9_-]{46}",a): raise HTTPException(400,"Invalid TON address")
    await execute("UPDATE users SET ton_address=$1 WHERE id=$2",a or None,ident); return {"success":True,"ton_wallet":a or ""}
@app.delete("/api/user/{ident}/ton_wallet")
async def del_ton(request:Request,ident:int): await target(request,ident); await execute("UPDATE users SET ton_address=NULL WHERE id=$1",ident); return {"success":True}
@app.get("/api/user/{ident}/usdt-address")
async def get_usdt(request:Request,ident:int): await target(request,ident); r=await one("SELECT usdt_address FROM users WHERE id=$1",ident); return {"usdt_address":r["usdt_address"] or ""}
@app.post("/api/user/{ident}/usdt-address")
async def put_usdt(request:Request,ident:int):
    await target(request,ident); b=await request.json(); a=clean(b.get("usdt_address"),128)
    if a and not re.fullmatch(r"T[A-Za-z0-9]{33}",a): raise HTTPException(400,"TRC20 address must be 34 characters and start with T")
    await execute("UPDATE users SET usdt_address=$1 WHERE id=$2",a or None,ident); return {"success":True}
@app.get("/api/user/{ident}/card")
async def get_card(request:Request,ident:int): await target(request,ident); r=await one("SELECT card_requisite FROM users WHERE id=$1",ident); return {"card":r["card_requisite"] or ""}
@app.put("/api/user/{ident}/wallet")
async def put_wallet(request:Request,ident:int): await target(request,ident); b=await request.json(); await execute("UPDATE users SET ton_address=$1 WHERE id=$2",clean(b.get("address"),128) or None,ident); return {"success":True}
@app.put("/api/user/{ident}/card")
async def put_card(request:Request,ident:int): await target(request,ident); b=await request.json(); await execute("UPDATE users SET card_requisite=$1 WHERE id=$2",clean(b.get("details"),128) or None,ident); return {"success":True}
@app.get("/api/user/{ident}/requisite")
async def get_req(request:Request,ident:int):
    await target(request,ident); rows=await q("SELECT requisite_key,requisite_value FROM user_requisites WHERE user_id=$1",ident); return {"requisites":{r["requisite_key"]:r["requisite_value"] for r in rows}}
@app.put("/api/user/{ident}/requisite")
async def put_req(request:Request,ident:int):
    await target(request,ident); b=await request.json(); k=clean(b.get("key"),100); v=clean(b.get("value"),1000)
    if not k: raise HTTPException(400,"key required")
    await execute("INSERT INTO user_requisites(user_id,requisite_key,requisite_value) VALUES($1,$2,$3) ON CONFLICT(user_id,requisite_key) DO UPDATE SET requisite_value=EXCLUDED.requisite_value",ident,k,v); return {"success":True}
@app.put("/api/user/{ident}/language")
async def put_lang(request:Request,ident:int):
    await target(request,ident); b=await request.json(); lang=str(b.get("language") or "")
    if lang not in LANGUAGES: raise HTTPException(400,"Unsupported language")
    await execute("UPDATE users SET language=$1 WHERE id=$2",lang,ident); return {"success":True,"language":lang}

@app.get("/api/user/{ident}/deals")
async def user_deals(request:Request,ident:int):
    await target(request,ident); limit=min(max(int(request.query_params.get("limit",50)),1),200); offset=max(int(request.query_params.get("offset",0)),0)
    rows=await q("SELECT * FROM deals WHERE seller_id=$1 OR buyer_id=$1 OR creator_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",ident,limit+1,offset); rows2=rows[:limit]; seller={}; buyer={}
    for r in rows2:
        d=deal_public(r)
        if r["seller_id"]==ident:seller[r["tag"]]=d
        if r["buyer_id"]==ident:buyer[r["tag"]]=d
        if r["creator_id"]==ident and r["tag"] not in seller and r["tag"] not in buyer:(buyer if r["deal_type"]=="buy" else seller)[r["tag"]]=d
    more=len(rows)>limit; return {"as_seller":seller,"as_buyer":buyer,"pagination":{"limit":limit,"offset":offset,"has_more":more,"next_offset":offset+limit if more else None}}
@app.get("/api/user/{ident}/transactions")
async def user_tx(request:Request,ident:int):
    await target(request,ident); limit=min(max(int(request.query_params.get("limit",50)),1),200); offset=max(int(request.query_params.get("offset",0)),0)
    rows=await q("SELECT id,type,status,currency,amount,description,deal_tag,created_at,metadata FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",ident,limit+1,offset); more=len(rows)>limit
    return {"transactions":[rowdict(x) for x in rows[:limit]],"pagination":{"limit":limit,"offset":offset,"has_more":more,"next_offset":offset+limit if more else None}}
@app.get("/api/user/{ident}/notifications")
async def notifications(request:Request,ident:int):
    await target(request,ident); limit=min(max(int(request.query_params.get("limit",50)),1),200); offset=max(int(request.query_params.get("offset",0)),0); rows=await q("SELECT * FROM notifications WHERE user_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",ident,limit+1,offset); more=len(rows)>limit
    return {"items":[rowdict(x) for x in rows[:limit]],"pagination":{"limit":limit,"offset":offset,"has_more":more,"next_offset":offset+limit if more else None}}
@app.post("/api/user/notifications/{nid}/read")
async def notification_read(request:Request,nid:int):
    u=await require(request); r=await one("UPDATE notifications SET read_at=COALESCE(read_at,NOW()) WHERE id=$1 AND user_id=$2 RETURNING id",nid,u["id"])
    if not r: raise HTTPException(404,"Notification not found")
    return {"ok":True}

@app.get("/api/config")
async def config():
    s=await settings(); escrow=await active_escrow_username(); return {"currencies":CURRENCIES,"min_amounts":{"TON":0.1,"USDT":1,"STARS":50},"languages":LANGUAGES,"escrow_username":escrow,"support_username":escrow,"bot_username":BOT_USERNAME or s.get("bot_username","") or "","ton_address":"","usdt_address":"","banner_url":s.get("banner_url",""),"logs_channel":s.get("logs_channel",""),"deposit_cards":{},"usdt_networks":{"trc20":{"label":"TRC20"}}}
@app.get("/api/rates")
async def rates():
    s=await settings(); r=s.get("rates",DEFAULT_RATES)
    if not isinstance(r, dict): r=DEFAULT_RATES
    return {**r,"rates":r}

@app.post("/api/deals/create")
async def create_deal(request:Request):
    u=await require(request); b=await request.json(); typ=str(b.get("deal_type","sell")).lower(); cur=str(b.get("currency","")).upper(); amount=dec(b.get("amount"),Decimal("-1"))
    if typ not in ("sell","buy") or not valid_currency(cur) or amount<=0: raise HTTPException(400,"Invalid deal data")
    tag=secrets.token_hex(5).upper()
    async def work(c):
        r=await c.fetchrow("""INSERT INTO deals(tag,deal_type,creator_id,seller_id,buyer_id,user_id,seller_username,seller_first_name,user_username,user_first_name,amount,currency,description,status,escrow_currency)
        VALUES($1,$2,$3,$4,$5,$3,$6,$7,$6,$7,$8,$9,$10,'wait_payment',$9) RETURNING *""",tag,typ,u["id"],u["id"] if typ=="sell" else None,u["id"] if typ=="buy" else None,u.get("username"),u.get("first_name"),amount,cur,clean(b.get("description")))
        await c.execute("INSERT INTO audit_logs(user_id,action,entity_type,entity_id,metadata) VALUES($1,$2,$3,$4,$5::jsonb)",u["id"],"deal.create","deal",tag,json.dumps({"amount":float(amount),"currency":cur}))
        return r
    r=await tx(work)
    return JSONResponse({"success":True,"deal_tag":tag,"deal":deal_public(r)},status_code=201)
@app.get("/api/deals/{tag}")
async def get_deal(tag:str):
    r=await one("SELECT * FROM deals WHERE upper(tag)=upper($1)",tag)
    if not r: raise HTTPException(404,"Deal not found")
    return {"deal":deal_public(r)}
@app.post("/api/deals/{tag}/join")
async def join_deal(request:Request,tag:str):
    u=await require(request)
    async def work(c):
        d=await c.fetchrow("SELECT * FROM deals WHERE upper(tag)=upper($1) FOR UPDATE",tag)
        if not d: raise HTTPException(404,"Deal not found")
        if d["status"]!="wait_payment": raise HTTPException(409,"deal_taken")
        if d["creator_id"]==u["id"]: return d,True
        if (d["deal_type"]=="sell" and d["buyer_id"]) or (d["deal_type"]=="buy" and d["seller_id"]): raise HTTPException(409,"deal_taken")
        if d["deal_type"]=="sell": d=await c.fetchrow("UPDATE deals SET buyer_id=$1,buyer_username=$2,buyer_first_name=$3,joined_at=NOW() WHERE id=$4 RETURNING *",u["id"],u.get("username"),u.get("first_name"),d["id"])
        else: d=await c.fetchrow("UPDATE deals SET seller_id=$1,seller_username=$2,seller_first_name=$3,joined_at=NOW() WHERE id=$4 RETURNING *",u["id"],u.get("username"),u.get("first_name"),d["id"])
        return d,False
    d,own=await tx(work)
    if own: raise HTTPException(400,"own_deal")
    other=d["seller_id"] if d["deal_type"]=="sell" else d["buyer_id"]
    if other: asyncio.create_task(notify(int(other),"deal_joined",f"Пользователь @{u.get('username') or u['id']} присоединился к сделке {d['tag']}."))
    return {"deal":deal_public(d),"first_time":True}

async def deal_change(request,tag,action,owner_force=False):
    u=await (owner(request) if owner_force else require(request))
    escrow_name=await active_escrow_username()
    async def work(c):
        d=await c.fetchrow("SELECT * FROM deals WHERE upper(tag)=upper($1) FOR UPDATE",tag)
        if not d: raise HTTPException(404,"Deal not found")
        uid=u["id"]; seller=d["seller_id"]==uid; buyer=d["buyer_id"]==uid; creator=d["creator_id"]==uid
        if not owner_force and not (seller or buyer or creator): raise HTTPException(403,"Forbidden")
        fee=(dec(d["amount"])*Decimal("0.01")); next_status=d["status"]
        d_payment_currency=d["payment_currency"] or d["currency"]
        d_payment_fee=dec(d["payment_fee"] or 0)
        d_payment_amount=dec(d["payment_amount"] or 0)
        if d_payment_amount <= 0 and d["escrow_status"] == "locked":
            d_payment_amount=dec(d["amount"])+d_payment_fee
        if action in ("pay","paid","owner_pay"):
            if action=="owner_pay" and owner_force and d["status"] == "wait_payment":
                if not d["buyer_id"]: raise HTTPException(409,"Buyer missing")
                # Owner/admin payment is a forced escrow action and must not
                # require the buyer to have funds on their wallet.
                d_payment_currency=d["currency"]
                d_payment_amount=dec(d["amount"])+fee
                d_payment_fee=fee
                next_status="paid"
            elif action=="owner_pay" and owner_force and d["status"] in ("paid","sent"):
                if not d["seller_id"]: raise HTTPException(409,"Seller missing")
                payout=dec(d["escrow_amount"] or 0) or dec(d["amount"])
                if d["escrow_status"]=="locked" and payout>0:
                    await ledger(c,int(d["seller_id"]),d["currency"],payout,"escrow_release","Owner escrow release","deal",d["id"],{"escrow":True,"owner_force":True})
                    await transaction(c,int(d["seller_id"]),"deal_release","completed",d["currency"],payout,"Owner escrow release",d["tag"],{"escrow":True,"owner_force":True})
                d_payment_amount=dec(d["payment_amount"] or d["amount"]); next_status="completed"
            else:
                if not d["buyer_id"]: raise HTTPException(409,"Buyer missing")
                if not owner_force and not buyer: raise HTTPException(403,"Only buyer can pay")
                if d["status"]!="wait_payment": raise HTTPException(409,"Invalid deal status")
                total=dec(d["payment_amount"] or d["amount"])+d_payment_fee
                if total <= 0: total=dec(d["amount"])+fee
                if total <= 0: raise HTTPException(400,"Invalid payment amount")
                await ledger(c,int(d["buyer_id"]),d_payment_currency,-total,"deal_payment","Deal payment","deal",d["id"],{"fee":float(fee),"escrow":True})
                await transaction(c,int(d["buyer_id"]),"deal_payment","completed",d_payment_currency,-total,"Deal payment",d["tag"],{"fee":float(fee),"escrow":True})
                d_payment_currency=d["currency"]; d_payment_amount=total; d_payment_fee=fee
                next_status="paid"
        elif action=="sent":
            if not seller: raise HTTPException(403,"Only seller can mark sent")
            if d["status"]!="paid": raise HTTPException(409,"Invalid action")
            next_status="sent"
        elif action=="confirm":
            if not buyer: raise HTTPException(403,"Only buyer can confirm")
            if d["status"]!="sent": raise HTTPException(409,"Invalid action")
            if not d["seller_id"]: raise HTTPException(409,"Seller missing")
            await ledger(c,int(d["seller_id"]),d["currency"],dec(d["amount"]),"escrow_release","Escrow release","deal",d["id"],{"escrow":True})
            await transaction(c,int(d["seller_id"]),"deal_release","completed",d["currency"],dec(d["amount"]),"Escrow release",d["tag"],{"escrow":True})
            next_status="completed"
        elif action=="cancel":
            if d["status"] in ("completed","cancelled"): raise HTTPException(409,"Deal already closed")
            if d["escrow_status"]=="locked" and d_payment_amount>0:
                await ledger(c,int(d["buyer_id"]),d_payment_currency,d_payment_amount,"escrow_refund","Escrow refund","deal",d["id"],{"escrow":True,"payment_currency":d_payment_currency})
                await transaction(c,int(d["buyer_id"]),"deal_refund","completed",d_payment_currency,d_payment_amount,"Escrow refund",d["tag"],{"escrow":True,"payment_currency":d_payment_currency})
            next_status="cancelled"
        else: raise HTTPException(400,"Invalid action")
        es="locked" if next_status in ("paid","sent") else "released" if next_status=="completed" else "cancelled_locked" if next_status=="cancelled" and d["escrow_status"]=="locked" else d["escrow_status"]
        ea=dec(d["amount"]) if next_status in ("paid","sent") else Decimal("0")
        extra={"paid":"paid_at","sent":"sent_at","completed":"completed_at","cancelled":"cancelled_at"}.get(next_status)
        sets="status=$1,fee=$2,escrow_status=$3,escrow_amount=$4,escrow_account=$5,payment_currency=$6,payment_amount=$7,payment_fee=$8"; args=[next_status,fee,es,ea,escrow_name,d_payment_currency,d_payment_amount,d_payment_fee]
        if extra: sets+=f",{extra}=NOW()"
        args.append(d["id"]); return await c.fetchrow(f"UPDATE deals SET {sets} WHERE id=$9 RETURNING *",*args)
    out=await tx(work); await audit(u["id"],f"deal.{action}","deal",tag,{"status":out["status"],"escrow_status":out["escrow_status"]})
    for uid in [out["buyer_id"],out["seller_id"]]:
        if uid and int(uid)!=u["id"]: asyncio.create_task(notify(int(uid),f"deal_{action}",f"Статус сделки {tag}: {out['status']}."))
    return {"success":True,"status":out["status"],"deal":deal_public(out)}
@app.post("/api/deals/{tag}/cancel")
async def deal_cancel(request: Request, tag: str): return await deal_change(request, tag, "cancel")
@app.post("/api/deals/{tag}/pay")
async def deal_pay(request: Request, tag: str): return await deal_change(request, tag, "pay")
@app.post("/api/deals/{tag}/paid")
async def deal_paid(request: Request, tag: str): return await deal_change(request, tag, "paid")
@app.post("/api/deals/{tag}/sent")
async def deal_sent(request: Request, tag: str): return await deal_change(request, tag, "sent")
@app.post("/api/deals/{tag}/confirm")
async def deal_confirm(request: Request, tag: str): return await deal_change(request, tag, "confirm")

@app.get("/api/deals/{tag}/pay-quotes")
async def pay_quotes(request:Request,tag:str):
    u=await require(request); d=await one("SELECT * FROM deals WHERE upper(tag)=upper($1)",tag)
    if not d: raise HTTPException(404,"Deal not found")
    s=await settings(); rates=s.get("rates",DEFAULT_RATES); b=await balances(u["id"]); dst=dec(rates.get(d["currency"],1)); quotes=[]
    for src in CURRENCIES:
        sr=dec(rates.get(src,1)); converted=dec(d["amount"])*dst/sr; total=converted*Decimal("1.01"); have=dec(b.get(src,0))
        quotes.append({"src_currency":src,"dst_currency":d["currency"],"deal_currency":d["currency"],"rate":float(dst/sr),"fee":0.01,"amount":float(d["amount"]),"src_amount":float(converted),"src_fee":float(converted*Decimal("0.01")),"conv_fee":0.0,"src_total":float(total),"required":float(total),"have":float(have),"sufficient":have>=total or (src=="TON" and have*Decimal("100000")>=total)})
    return {"quotes":quotes}
@app.post("/api/deals/{tag}/pay-with")
async def pay_with(request:Request,tag:str):
    u=await require(request); b=await request.json(); src=str(b.get("source_currency","")).upper();
    if not valid_currency(src): raise HTTPException(400,"Invalid source_currency")
    s=await settings(); rates=s.get("rates",DEFAULT_RATES)
    async def work(c):
        d=await c.fetchrow("SELECT * FROM deals WHERE upper(tag)=upper($1) FOR UPDATE",tag)
        if not d: raise HTTPException(404,"Deal not found")
        if d["buyer_id"]!=u["id"]: raise HTTPException(403,"Only buyer can pay")
        if d["status"]!="wait_payment": raise HTTPException(409,"Invalid deal status")
        dst_rate=dec(rates.get(d["currency"],1)); src_rate=dec(rates.get(src,1))
        if dst_rate<=0 or src_rate<=0: raise HTTPException(503,"Currency rate unavailable")
        total=dec(d["amount"])*dst_rate/src_rate*Decimal("1.01")
        bal=dec(await c.fetchval("SELECT balance FROM balances WHERE user_id=$1 AND currency=$2 FOR UPDATE",u["id"],src) or 0)
        if bal < total: raise HTTPException(402,"Insufficient funds")
        fee=total- total/Decimal("1.01"); target=dec(d["amount"])
        await ledger(c,u["id"],src,-total,"deal_payment","Deal payment (converted)","deal",d["id"],{"fee":float(fee),"source_currency":src,"target_currency":d["currency"],"escrow":True})
        await transaction(c,u["id"],"deal_payment","completed",src,-total,"Deal payment (converted)",d["tag"],{"fee":float(fee),"source_currency":src,"target_currency":d["currency"],"escrow":True})
        escrow_name=await active_escrow_username()
        return await c.fetchrow("UPDATE deals SET status='paid',fee=$1,escrow_status='locked',escrow_amount=$2,escrow_currency=$3,escrow_account=$4,payment_currency=$5,payment_amount=$6,payment_fee=$7,paid_at=NOW() WHERE id=$8 RETURNING *",fee,target,d["currency"],escrow_name,src,total,fee,d["id"])
    try:
        out=await tx(work)
    except Exception as exc:
        write_log(f"PAY_WITH_FAILED tag={tag} user={u.get('id')} source={src} error={exc}\n{traceback.format_exc()}")
        raise
    return {"success":True,"status":out["status"],"deal":deal_public(out)}
@app.get("/api/deals/{tag}/share")
async def share(tag:str,lang:str="ru"):
    d=await one("SELECT tag,deal_type,amount,currency,description,status FROM deals WHERE upper(tag)=upper($1)",tag)
    if not d: raise HTTPException(404,"Deal not found")
    s=await settings(); bot_user=BOT_USERNAME or s.get("bot_username",""); text=(f"Funpay Service deal {d['tag']}: {d['amount']} {d['currency']}" if lang=="en" else f"Сделка Funpay Service {d['tag']}: {d['amount']} {d['currency']}"); return {"tag":d["tag"],"text":text,"url":f"https://t.me/{bot_user}?start=deal_{d['tag']}" if bot_user else ""}

@app.post("/api/log/action")
async def log_action(request:Request):
    u=await current_user(request); b=await request.json()
    if u: await audit(u["id"],clean(b.get("action"),100) or "unknown",clean(b.get("entity_type"),100) or None,clean(b.get("entity_id"),200) or None,b)
    return {"ok":True}

@app.get("/api/admin/me")
async def admin_me(request:Request):
    u=await current_user(request); return {"is_admin":bool(u and (u["id"]==BOOTSTRAP_OWNER or await one("SELECT 1 FROM admins WHERE user_id=$1",u["id"])))}
@app.get("/api/admin/list")
async def admin_list(request:Request): await admin(request); rows=await q("SELECT * FROM admins ORDER BY added_at DESC"); return {"admins":[rowdict(x) for x in rows]}
@app.post("/api/admin/add")
async def admin_add(request:Request):
    u=await owner(request); b=await request.json(); uid=int(b.get("user_id")); await execute("INSERT INTO admins(user_id,username) VALUES($1,$2) ON CONFLICT DO NOTHING",uid,clean(b.get("username"),200) or None); return {"ok":True}
@app.post("/api/admin/remove")
async def admin_remove(request:Request):
    await owner(request); b=await request.json(); await execute("DELETE FROM admins WHERE user_id=$1",int(b.get("user_id"))); return {"ok":True}
@app.get("/api/admin/stats")
async def admin_stats(request:Request):
    await admin(request)
    a=await one("SELECT COUNT(*) n FROM users")
    d=await one("SELECT COUNT(*) n FROM deals")
    v=await one("SELECT COALESCE(SUM(amount),0) n FROM transactions WHERE type IN ('deal_payment','deposit') AND status='completed'")
    counts=await one("SELECT COUNT(*) FILTER (WHERE status IN ('wait_payment','paid','sent')) active, COUNT(*) FILTER (WHERE status='completed') completed, COUNT(*) FILTER (WHERE status='cancelled') cancelled, COUNT(*) total FROM deals")
    volume=float(v["n"])
    st=await settings()
    override=st.get("volume_override")
    if override is not None:
        try: volume=float(override)
        except (TypeError,ValueError): pass
    co=st.get("deal_counts_override")
    if isinstance(co,dict):
        for k in ("active","completed","cancelled","total"):
            if co.get(k) is not None:
                try: counts[k]=int(co[k])
                except (TypeError,ValueError): pass
    return {"users":int(a["n"]),"deals":int(d["n"]),"volume":volume,"volume_usd":volume,"trading_volume_usd":volume,"deal_counts":{k:int(counts[k] or 0) for k in ("active","completed","cancelled","total")}}
@app.post("/api/admin/credit-self")
async def credit_self(request:Request):
    u=await admin(request); b=await request.json(); cur=str(b.get("currency","")).upper(); amount=dec(b.get("amount"));
    if not valid_currency(cur) or amount<=0: raise HTTPException(400,"Invalid data")
    async def work(c): await ledger(c,u["id"],cur,amount,"admin_credit","Admin credit",metadata={"admin":True}); await transaction(c,u["id"],"admin_credit","completed",cur,amount,"Admin credit",metadata={"admin":True})
    await tx(work); return {"ok":True,"balance":await balances(u["id"])}
@app.post("/api/admin/volume-override")
async def volume_override(request:Request):
    await admin(request); b=await request.json(); raw=b.get("value", b.get("amount_usd", 0));
    try: value=float(raw)
    except (TypeError,ValueError): raise HTTPException(400,"Invalid volume")
    if value<0: raise HTTPException(400,"Invalid volume")
    await execute("INSERT INTO settings(key,value) VALUES('volume_override',$1::jsonb) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",json.dumps(value)); return {"ok":True,"value":value}
@app.post("/api/admin/deal-counts-override")
async def deal_counts_override(request:Request):
    await admin(request)
    b=await request.json()
    raw=b.get("value") if isinstance(b.get("value"),dict) else b
    if not isinstance(raw,dict):
        raise HTTPException(400,"Invalid deal counts")

    # The frontend sends the counters directly as {active, completed, cancelled, total}.
    # Also accept {value: {...}} for backwards compatibility.
    out={}
    for k in ("active","completed","cancelled","total"):
        if k not in raw or raw[k] in (None,""):
            out[k]=None
        else:
            try:
                value=int(raw[k])
            except (TypeError,ValueError):
                raise HTTPException(400,"Invalid deal counts")
            if value < 0:
                raise HTTPException(400,"Invalid deal counts")
            out[k]=value

    await execute(
        "INSERT INTO settings(key,value) VALUES('deal_counts_override',$1::jsonb) "
        "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
        json.dumps(out)
    )
    return {"ok":True,"value":out}

@app.get("/api/owner/me")
async def owner_me(request:Request): await owner(request); return {"is_owner":True,"user_id":BOOTSTRAP_OWNER}
@app.get("/api/owner/stats")
async def owner_stats(request:Request):
    await owner(request)
    a=await one("SELECT COUNT(*) n FROM users")
    d=await one("SELECT COUNT(*) n FROM deals")
    b=await one("SELECT COUNT(*) n FROM banned_users")
    w=await one("SELECT COUNT(*) n FROM admins")
    return {"users":int(a["n"]),"deals":int(d["n"]),"banned":int(b["n"]),"workers":int(w["n"]),"admins":int(w["n"])}
@app.get("/api/owner/settings")
async def owner_settings(request:Request): await owner(request); return await settings()
@app.post("/api/owner/settings")
async def owner_settings_put(request:Request):
    await owner(request); b=await request.json()
    payload=b.get("settings") if isinstance(b.get("settings"),dict) else b
    if isinstance(payload,dict):
        ignored={"key","value","settings"}
        for k,v in payload.items():
            if k in ignored: continue
            if k in {"logs_channel","escrow_username","banner_url","ton_address","usdt_address","bot_username"}:
                v=clean(v,1000) if v is not None else ""
            await execute("INSERT INTO settings(key,value) VALUES($1,$2::jsonb) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",clean(k,100),json.dumps(v,ensure_ascii=False))
    elif b.get("key"): await execute("INSERT INTO settings(key,value) VALUES($1,$2::jsonb) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",clean(b["key"],100),json.dumps(b.get("value"),ensure_ascii=False))
    return {"ok":True,"settings":await settings()}
@app.post("/api/owner/validate-chat")
async def validate_chat(request:Request):
    await owner(request); b=await request.json(); chat=b.get("chat_id")
    if not BOT_TOKEN or not chat:return {"valid":False}
    try:
        async with httpx.AsyncClient(timeout=10) as x:r=await x.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getChat",params={"chat_id":chat}); return {"valid":r.is_success,"chat":r.json().get("result") if r.is_success else None}
    except:return {"valid":False}
@app.get("/api/owner/escrow-accounts")
async def escrow_accounts(request:Request):
    await owner(request); rows=await q("SELECT * FROM escrow_accounts ORDER BY active DESC, created_at"); active=next((x["phone"] for x in rows if x["active"]),None); return {"accounts":[rowdict(x) for x in rows],"active":active}
@app.post("/api/owner/escrow-accounts/active")
async def escrow_active(request:Request):
    await owner(request); b=await request.json(); phone=clean(b.get("phone"),100)
    if not phone: raise HTTPException(400,"Phone required")
    if not await one("SELECT 1 FROM escrow_accounts WHERE phone=$1",phone): raise HTTPException(404,"Escrow account not found")
    await execute("UPDATE escrow_accounts SET active=false")
    await execute("UPDATE escrow_accounts SET active=true WHERE phone=$1",phone)
    return {"ok":True,"active":phone}
@app.delete("/api/owner/escrow-accounts/{phone}")
async def escrow_del(request:Request,phone:str): await owner(request); await execute("DELETE FROM escrow_accounts WHERE phone=$1",phone); return {"ok":True}
@app.post("/api/owner/escrow-accounts/{phone}")
async def escrow_add(request:Request,phone:str):
    await owner(request); phone=clean(phone,100)
    if not phone: raise HTTPException(400,"Phone required")
    b=await request.json(); await execute("INSERT INTO escrow_accounts(phone,first_name,username,active) VALUES($1,$2,$3,false) ON CONFLICT(phone) DO UPDATE SET first_name=EXCLUDED.first_name,username=EXCLUDED.username",phone,clean(b.get("first_name"),200) or None,clean(b.get("username"),200).lstrip("@") or None); return {"ok":True,"phone":phone}
@app.get("/api/owner/whitelist")
async def whitelist(request:Request): await owner(request); rows=await q("SELECT w.user_id,u.username,u.first_name FROM whitelist w LEFT JOIN users u ON u.id=w.user_id ORDER BY w.created_at"); return {"whitelist":[rowdict(x) for x in rows]}
@app.post("/api/owner/whitelist/add")
async def whitelist_add(request:Request):
    await owner(request)
    b=await request.json()
    uid=int(b.get("user_id"))
    user=await one("SELECT id,username FROM users WHERE id=$1",uid)
    if not user: raise HTTPException(404,"User not found")
    await execute("INSERT INTO whitelist(user_id) VALUES($1) ON CONFLICT DO NOTHING",uid)
    await execute("INSERT INTO admins(user_id,username) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET username=COALESCE(EXCLUDED.username,admins.username)",uid,user["username"])
    return {"ok":True,"user_id":uid,"is_admin":True}
@app.post("/api/owner/whitelist/remove")
async def whitelist_remove(request:Request):
    await owner(request)
    b=await request.json(); uid=int(b.get("user_id"))
    await execute("DELETE FROM whitelist WHERE user_id=$1",uid)
    await execute("DELETE FROM admins WHERE user_id=$1",uid)
    return {"ok":True,"removed":uid}
@app.post("/api/owner/resolve-user")
async def resolve_user(request:Request):
    await owner(request)
    b=await request.json(); s=clean(b.get("query"),200).lstrip("@").strip()
    r=await one("SELECT id,username,first_name,last_name FROM users WHERE username ILIKE $1 OR id::text=$2 LIMIT 1",s,s)
    if not r and s.isdigit():
        r=await one("SELECT id,username,first_name,last_name FROM users WHERE id=$1",int(s))
    if not r: return {"ok":False,"user_id":0,"username":None,"first_name":None,"last_name":None,"in_whitelist":False,"is_admin":False}
    uid=int(r["id"])
    in_white=bool(await one("SELECT 1 FROM whitelist WHERE user_id=$1",uid))
    is_admin=bool(await one("SELECT 1 FROM admins WHERE user_id=$1",uid))
    bal=await balances(uid)
    return {"ok":True,"user_id":uid,"username":r["username"],"first_name":r["first_name"],"last_name":r["last_name"],"in_whitelist":in_white,"is_admin":is_admin,"balance":bal,"balances":bal}
@app.get("/api/owner/broadcast/stats")
async def broadcast_stats(request:Request):
    await owner(request); a=await one("SELECT COUNT(*) n FROM users"); b=await one("SELECT COUNT(*) n FROM admins"); return {"total":int(a["n"]),"sent":0,"failed":0,"users":int(a["n"]),"admins":int(b["n"])}
@app.post("/api/owner/broadcast")
async def broadcast(request:Request):
    u=await owner(request); b=await request.json(); text=clean(b.get("text"),4096); target=b.get("target","all"); pm=b.get("parse_mode","HTML")
    if not text: raise HTTPException(400,"text required")
    rows=await q("SELECT user_id AS id FROM admins") if target=="admins" else await q("SELECT id FROM users")
    br=await one("INSERT INTO broadcasts(text,target,parse_mode) VALUES($1,$2,$3) RETURNING id",text,target,pm); sent=failed=0
    for r in rows:
        if await bot_send(r["id"],text,pm): sent+=1
        else: failed+=1
    await execute("UPDATE broadcasts SET sent=$1,failed=$2 WHERE id=$3",sent,failed,br["id"]); return {"ok":True,"sent":sent,"failed":failed,"broadcast_id":int(br["id"])}
@app.get("/api/owner/banned")
async def banned(request:Request):
    await owner(request); rows=await q("SELECT b.*,u.username,u.first_name,u.last_name FROM banned_users b LEFT JOIN users u ON u.id=b.user_id ORDER BY b.created_at DESC"); return {"items":[rowdict(x) for x in rows]}
@app.post("/api/owner/ban")
async def ban(request:Request):
    u=await owner(request); b=await request.json(); key=clean(b.get("identifier"),200).lstrip("@").strip(); r=await one("SELECT id,username FROM users WHERE id::text=$1 OR username ILIKE $1 LIMIT 1",key);
    if not r: raise HTTPException(404,"User not found")
    uid=int(r["id"])
    if uid==u["id"]: raise HTTPException(400,"Cannot ban owner")
    await execute("INSERT INTO banned_users(user_id,reason,banned_by) VALUES($1,$2,$3) ON CONFLICT(user_id) DO UPDATE SET reason=EXCLUDED.reason,banned_by=EXCLUDED.banned_by",uid,clean(b.get("reason"),1000),u["id"]); await bot_send(uid,"Ваш аккаунт заблокирован в Funpay Service."); return {"ok":True,"user_id":uid,"username":r["username"]}
@app.post("/api/owner/unban")
async def unban(request:Request):
    await owner(request); b=await request.json(); key=clean(b.get("identifier"),200).lstrip("@").strip(); r=await one("SELECT id FROM users WHERE id::text=$1 OR username ILIKE $1 LIMIT 1",key)
    if not r: raise HTTPException(404,"User not found")
    await execute("DELETE FROM banned_users WHERE user_id=$1",int(r["id"])); return {"ok":True,"user_id":int(r["id"])}
@app.post("/api/owner/set-balance")
async def set_balance(request:Request):
    await owner(request); b=await request.json(); ident=clean(b.get("identifier"),200).lstrip("@").strip(); cur=str(b.get("currency","")).upper(); amount=dec(b.get("amount")); r=await one("SELECT id FROM users WHERE id::text=$1 OR username ILIKE $1 LIMIT 1",ident); uid=int(r["id"] if r else (int(ident) if ident.isdigit() else 0))
    if not uid or not valid_currency(cur) or amount<0: raise HTTPException(400,"Invalid data")
    if not await one("SELECT id FROM users WHERE id=$1",uid): raise HTTPException(404,"User not found")
    # Change balance through the same ledger used by deposits/payments.
    # This keeps wallet, balance API and deal payment in sync.
    async def work(c):
        current=dec(await c.fetchval("SELECT balance FROM balances WHERE user_id=$1 AND currency=$2 FOR UPDATE",uid,cur) or 0)
        delta=amount-current
        if delta:
            await ledger(c,uid,cur,delta,"admin_balance_set","Admin balance correction",metadata={"admin":True,"set_to":str(amount)})
            await transaction(c,uid,"admin_balance_set","completed",cur,delta,"Admin balance correction",metadata={"admin":True,"set_to":str(amount)})
    await tx(work)
    bal=await balances(uid)
    return {"ok":True,"user_id":uid,"currency":cur,"amount":float(amount),"new_balance":float(bal.get(cur,0)),"balance":bal,"balances":bal}
@app.post("/api/owner/zero-balance")
async def zero_balance(request:Request):
    await owner(request); b=await request.json(); ident=clean(b.get("identifier"),200).lstrip("@").strip(); r=await one("SELECT id FROM users WHERE id::text=$1 OR username ILIKE $1 LIMIT 1",ident); uid=int(r["id"] if r else (int(ident) if ident.isdigit() else 0));
    if not uid: raise HTTPException(404,"User not found")
    cur=b.get("currency"); await execute("UPDATE balances SET balance=0 WHERE user_id=$1"+(" AND currency=$2" if cur else ""),uid,*([str(cur).upper()] if cur else [])); return {"ok":True}
@app.post("/api/owner/deal-pay")
async def owner_deal_pay(request:Request): b=await request.json(); return await deal_change(request,clean(b.get("tag") or b.get("deal_tag")),"owner_pay",True)
@app.post("/api/owner/deal-cancel")
async def owner_deal_cancel(request:Request): b=await request.json(); return await deal_change(request,clean(b.get("tag") or b.get("deal_tag")),"cancel",True)

@app.post("/api/user/deposit")
async def deposit(request:Request):
    await require(request)
    raise HTTPException(503,"Пополнение временно недоступно")
@app.post("/api/user/{ident}/deposit/ton")
async def deposit_ton(request:Request,ident:int):
    await target(request,ident)
    raise HTTPException(503,"Пополнение TON временно недоступно")
@app.post("/api/user/usdt/deposit-claim")
async def deposit_usdt(request:Request):
    await require(request)
    raise HTTPException(503,"Пополнение USDT временно недоступно")
@app.post("/api/user/stars-invoice")
async def stars_invoice(request:Request):
    await require(request)
    raise HTTPException(503,"Пополнение Stars временно недоступно")
@app.post("/api/user/withdraw")
async def withdraw(request:Request):
    u=await require(request); b=await request.json(); cur=str(b.get("currency","")).upper(); amount=dec(b.get("amount")); idem=clean(request.headers.get("X-Idempotency-Key") or b.get("idempotency_key"),128)
    if not valid_currency(cur) or amount<=0: raise HTTPException(400,"Invalid withdrawal")
    if not idem: raise HTTPException(400,"X-Idempotency-Key is required")
    async def work(c):
        claim=await claim_idempotency(c,u["id"],"withdraw",idem)
        if claim["replay"]:
            return json.loads(claim["response"]), int(claim["status_code"] or 200)
        bal=dec(await c.fetchval("SELECT balance FROM balances WHERE user_id=$1 AND currency=$2 FOR UPDATE",u["id"],cur) or 0)
        if bal<amount: raise HTTPException(402,"Insufficient funds")
        await ledger(c,u["id"],cur,-amount,"withdrawal_hold","Withdrawal hold","withdrawal",None)
        w=await c.fetchrow("INSERT INTO withdrawal_requests(user_id,currency,amount,method,destination,network,status,comment,client_request_id) VALUES($1,$2,$3,$4,$5,$6,'pending',$7,$8) RETURNING *",u["id"],cur,amount,b.get("method") or b.get("withdraw_mode") or "default",clean(b.get("destination"),256) or None,b.get("network"),"Заявка принята и ожидает обработки.",idem)
        response={"success":True,"demo":True,"message":"Заявка на вывод принята. Баланс списан.","withdrawal":rowdict(w)}
        await finish_idempotency(c,idem,response,200)
        return response,200
    out,status=await tx(work); return JSONResponse(out,status_code=status)
@app.post("/api/user/withdraw/cancel")
async def withdraw_cancel(request:Request):
    u=await require(request); b=await request.json(); wid=int(b.get("withdrawal_id"))
    async def work(c):
        w=await c.fetchrow("SELECT * FROM withdrawal_requests WHERE id=$1 AND user_id=$2 FOR UPDATE",wid,u["id"])
        if not w: raise HTTPException(404,"Withdrawal not found")
        if w["status"]!="pending": raise HTTPException(409,"Cannot cancel")
        await ledger(c,u["id"],w["currency"],dec(w["amount"]),"withdrawal_refund","Withdrawal cancellation","withdrawal",w["id"]); await c.execute("UPDATE withdrawal_requests SET status='cancelled',cancelled_at=NOW() WHERE id=$1",wid); return w
    await tx(work); return {"success":True,"withdrawal_id":wid}

async def process_deposit(did,status,reason=None):
    async def work(c):
        d=await c.fetchrow("SELECT * FROM deposit_requests WHERE id=$1 FOR UPDATE",did)
        if not d: raise HTTPException(404,"Deposit not found")
        if d["status"] not in ("pending","processing"): return d
        if status=="completed":
            await ledger(c,int(d["user_id"]),d["currency"],dec(d["amount"]),"deposit","Deposit","deposit",d["id"],{"tx_hash":d["tx_hash"],"network":d["network"]}); await transaction(c,int(d["user_id"]),"deposit","completed",d["currency"],dec(d["amount"]),"Deposit",metadata={"request_id":int(d["id"]),"tx_hash":d["tx_hash"]})
        return await c.fetchrow("UPDATE deposit_requests SET status=$1,processed_at=NOW(),comment=COALESCE($2,comment) WHERE id=$3 RETURNING *",status,reason,did)
    return await tx(work)
@app.get("/api/admin/deposits")
async def admin_deposits(request:Request):
    await admin(request); limit=min(max(int(request.query_params.get("limit",50)),1),200); offset=max(int(request.query_params.get("offset",0)),0); status=request.query_params.get("status"); params=[]; where=""
    if status: params=[status]; where="WHERE d.status=$1"
    rows=await q(f"SELECT d.*,u.username,u.first_name FROM deposit_requests d JOIN users u ON u.id=d.user_id {where} ORDER BY d.created_at DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}",*params,limit+1,offset); more=len(rows)>limit; return {"items":[rowdict(x) for x in rows[:limit]],"pagination":{"limit":limit,"offset":offset,"has_more":more,"next_offset":offset+limit if more else None}}
@app.post("/api/admin/deposits/{did}/approve")
async def approve_dep(request:Request,did:int): await admin(request); d=await process_deposit(did,"completed"); asyncio.create_task(notify(int(d["user_id"]),"deposit_completed",f"Пополнение {d['amount']} {d['currency']} зачислено на баланс.")); return {"ok":True,"deposit":rowdict(d)}
@app.post("/api/admin/deposits/{did}/reject")
async def reject_dep(request:Request,did:int): await admin(request); d=await process_deposit(did,"rejected",clean((await request.json()).get("reason"),500)); asyncio.create_task(notify(int(d["user_id"]),"deposit_rejected",f"Пополнение {d['amount']} {d['currency']} отклонено.")); return {"ok":True,"deposit":rowdict(d)}
@app.get("/api/admin/withdrawals")
async def admin_withdrawals(request:Request):
    await admin(request); limit=min(max(int(request.query_params.get("limit",50)),1),200); offset=max(int(request.query_params.get("offset",0)),0); status=request.query_params.get("status"); params=[]; where=""
    if status: params=[status]; where="WHERE w.status=$1"
    rows=await q(f"SELECT w.*,u.username,u.first_name FROM withdrawal_requests w JOIN users u ON u.id=w.user_id {where} ORDER BY w.created_at DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}",*params,limit+1,offset); more=len(rows)>limit; return {"items":[rowdict(x) for x in rows[:limit]],"pagination":{"limit":limit,"offset":offset,"has_more":more,"next_offset":offset+limit if more else None}}
@app.post("/api/admin/withdrawals/{wid}/reject")
async def reject_withdraw(request:Request,wid:int):
    await admin(request)
    async def work(c):
        w=await c.fetchrow("SELECT * FROM withdrawal_requests WHERE id=$1 FOR UPDATE",wid)
        if not w or w["status"]!="pending": raise HTTPException(409,"Invalid withdrawal status")
        await ledger(c,int(w["user_id"]),w["currency"],dec(w["amount"]),"withdrawal_refund","Withdrawal rejected","withdrawal",wid); return await c.fetchrow("UPDATE withdrawal_requests SET status='rejected',processed_at=NOW() WHERE id=$1 RETURNING *",wid)
    w=await tx(work); asyncio.create_task(notify(int(w["user_id"]),"withdrawal_rejected",f"Вывод {w['amount']} {w['currency']} отклонён.")); return {"ok":True}
@app.post("/api/admin/withdrawals/{wid}/processing")
async def processing_withdraw(request:Request,wid:int):
    await admin(request)
    r=await one("UPDATE withdrawal_requests SET status='processing' WHERE id=$1 AND status='pending' RETURNING *",wid)
    if not r: raise HTTPException(409,"Invalid withdrawal status")
    return {"ok":True,"withdrawal":rowdict(r)}
@app.post("/api/admin/withdrawals/{wid}/complete")
async def complete_withdraw(request:Request,wid:int):
    await admin(request); b=await request.json(); r=await one("UPDATE withdrawal_requests SET status='completed',tx_hash=$1,processed_at=NOW() WHERE id=$2 AND status IN ('pending','processing') RETURNING *",clean(b.get("tx_hash"),256) or None,wid)
    if not r: raise HTTPException(409,"Invalid withdrawal status")
    asyncio.create_task(notify(int(r["user_id"]),"withdrawal_completed",f"Вывод {r['amount']} {r['currency']} выполнен.")); return {"ok":True,"withdrawal":rowdict(r)}

@app.get("/api/deals/{tag}/dispute")
async def get_dispute(request:Request,tag:str):
    u=await require(request); d=await one("SELECT p.*,d.tag,d.status deal_status FROM disputes p JOIN deals d ON d.id=p.deal_id WHERE upper(d.tag)=upper($1)",tag)
    if not d: raise HTTPException(404,"Dispute not found")
    if not await one("SELECT 1 FROM deals WHERE id=$1 AND (buyer_id=$2 OR seller_id=$2)",d["deal_id"],u["id"]) and not (u["id"]==BOOTSTRAP_OWNER or await one("SELECT 1 FROM admins WHERE user_id=$1",u["id"])): raise HTTPException(403,"Forbidden")
    m=await q("SELECT * FROM dispute_messages WHERE dispute_id=$1 ORDER BY created_at",d["id"]); return {"dispute":rowdict(d),"messages":[rowdict(x) for x in m]}
@app.post("/api/deals/{tag}/dispute")
async def open_dispute(request:Request,tag:str):
    u=await require(request); b=await request.json(); reason=clean(b.get("reason"),2000)
    if not reason: raise HTTPException(400,"reason required")
    async def work(c):
        d=await c.fetchrow("SELECT * FROM deals WHERE upper(tag)=upper($1) FOR UPDATE",tag)
        if not d: raise HTTPException(404,"Deal not found")
        if u["id"] not in (d["buyer_id"],d["seller_id"]): raise HTTPException(403,"Forbidden")
        if d["status"] in ("completed","cancelled"): raise HTTPException(409,"Deal already closed")
        p=await c.fetchrow("INSERT INTO disputes(deal_id,opened_by,reason) VALUES($1,$2,$3) ON CONFLICT(deal_id) DO UPDATE SET status='open',reason=EXCLUDED.reason,opened_by=EXCLUDED.opened_by RETURNING *",d["id"],u["id"],reason); await c.execute("UPDATE deals SET status='disputed',dispute_id=$1 WHERE id=$2",p["id"],d["id"]); return d,p
    d,p=await tx(work)
    for uid in (d["buyer_id"],d["seller_id"]):
        if uid and int(uid)!=u["id"]: asyncio.create_task(notify(int(uid),"dispute_opened",f"По сделке {d['tag']} открыт спор."))
    return {"ok":True,"dispute":rowdict(p)}
@app.post("/api/disputes/{did}/messages")
async def dispute_message(request:Request,did:int):
    u=await require(request); b=await request.json(); msg=clean(b.get("message"),4000)
    if not msg: raise HTTPException(400,"message required")
    d=await one("SELECT p.*,d.buyer_id,d.seller_id,d.tag FROM disputes p JOIN deals d ON d.id=p.deal_id WHERE p.id=$1",did)
    if not d: raise HTTPException(404,"Dispute not found")
    if u["id"] not in (d["buyer_id"],d["seller_id"]) and u["id"]!=BOOTSTRAP_OWNER and not await one("SELECT 1 FROM admins WHERE user_id=$1",u["id"]): raise HTTPException(403,"Forbidden")
    r=await one("INSERT INTO dispute_messages(dispute_id,user_id,message) VALUES($1,$2,$3) RETURNING *",did,u["id"],msg)
    for uid in (d["buyer_id"],d["seller_id"]):
        if uid and int(uid)!=u["id"]: asyncio.create_task(notify(int(uid),"dispute_message",f"Новое сообщение по спору сделки {d['tag']}: {msg}"))
    return {"ok":True,"message":rowdict(r)}
@app.get("/api/admin/disputes")
async def admin_disputes(request:Request):
    await admin(request); limit=min(max(int(request.query_params.get("limit",50)),1),200); offset=max(int(request.query_params.get("offset",0)),0); status=request.query_params.get("status"); params=[]; where=""
    if status:params=[status];where="WHERE p.status=$1"
    rows=await q(f"SELECT p.*,d.tag,d.amount,d.currency,d.status deal_status,u.username opened_by_username FROM disputes p JOIN deals d ON d.id=p.deal_id JOIN users u ON u.id=p.opened_by {where} ORDER BY p.created_at DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}",*params,limit+1,offset); more=len(rows)>limit; return {"items":[rowdict(x) for x in rows[:limit]],"pagination":{"limit":limit,"offset":offset,"has_more":more,"next_offset":offset+limit if more else None}}
@app.post("/api/admin/disputes/{did}/status")
async def dispute_status(request:Request,did:int):
    await admin(request); b=await request.json(); st=str(b.get("status",""));
    if st not in ("open","investigating","resolved","closed"): raise HTTPException(400,"Invalid status")
    r=await one("UPDATE disputes SET status=$1,resolution=$2 WHERE id=$3 RETURNING *",st,clean(b.get("resolution"),4000) or None,did)
    if not r: raise HTTPException(404,"Dispute not found")
    return {"ok":True,"dispute":rowdict(r)}
@app.post("/api/admin/disputes/{did}/resolve")
async def resolve_dispute(request:Request,did:int):
    u=await admin(request); b=await request.json(); resolution=clean(b.get("resolution"),4000)
    async def work(c):
        p=await c.fetchrow("SELECT p.id AS dispute_id,p.status AS dispute_status,p.resolution,p.resolved_by,p.deal_id,d.tag,d.buyer_id,d.seller_id,d.currency,d.escrow_status,d.escrow_amount,d.payment_currency,d.payment_amount,d.payment_fee,d.status AS deal_status FROM disputes p JOIN deals d ON d.id=p.deal_id WHERE p.id=$1 FOR UPDATE",did)
        if not p: raise HTTPException(404,"Dispute not found")
        if p["dispute_status"] in ("resolved","closed"): raise HTTPException(409,"Already resolved")
        # Default resolution: release locked escrow to seller. Explicit refund=true returns it to buyer.
        refund=bool(b.get("refund",False))
        if p["escrow_status"]=="locked" and dec(p["escrow_amount"])>0:
            uid=int(p["buyer_id"] if refund else p["seller_id"]); typ="escrow_refund" if refund else "escrow_release"; desc="Dispute refund" if refund else "Dispute escrow release"
            payout_currency=(p["payment_currency"] or p["currency"]) if refund else p["currency"]
            payout_amount=(dec(p["payment_amount"] or 0) or (dec(p["escrow_amount"])+dec(p["payment_fee"] or 0))) if refund else dec(p["escrow_amount"])
            await ledger(c,uid,payout_currency,payout_amount,typ,desc,"dispute",did,{"dispute":True})
            await transaction(c,uid,"deal_refund" if refund else "deal_release","completed",payout_currency,payout_amount,desc,p["tag"],{"dispute":True})
        await c.execute("UPDATE disputes SET status='resolved',resolution=$1,resolved_by=$2,resolved_at=NOW() WHERE id=$3",resolution,u["id"],did); await c.execute("UPDATE deals SET status='completed',escrow_status='released',escrow_amount=0,completed_at=NOW() WHERE id=$1",p["deal_id"]); return p
    p=await tx(work)
    for uid in (p["buyer_id"],p["seller_id"]):
        if uid: asyncio.create_task(notify(int(uid),"dispute_resolved",f"Спор по сделке {p['tag']} разрешён: {resolution}"))
    return {"ok":True,"dispute":rowdict(p)}

@app.get("/api/events")
async def events(request:Request):
    u=await require(request); uid=u["id"]; queue=asyncio.Queue(); sse_clients.setdefault(uid,set()).add(queue)
    async def gen():
        try:
            yield f"event: ready\ndata: {json.dumps({'ok':True})}\n\n"
            while True:
                try: data=await asyncio.wait_for(queue.get(),25); yield f"event: notification\ndata: {data}\n\n"
                except asyncio.TimeoutError: yield f": ping {int(time.time())}\n\n"
        finally:
            sse_clients.get(uid,set()).discard(queue)
    return StreamingResponse(gen(),media_type="text/event-stream",headers={"Cache-Control":"no-cache","Connection":"keep-alive"})

@app.get("/api/orders")
async def orders(request:Request): u=await require(request); rows=await q("SELECT * FROM deals WHERE seller_id=$1 OR buyer_id=$1 OR creator_id=$1 ORDER BY created_at DESC LIMIT 200",u["id"]); return [deal_public(x) for x in rows]
@app.get("/api/wallets")
async def wallets(request:Request): u=await require(request); return await balances(u["id"])
@app.get("/api/transactions")
async def transactions_legacy(request:Request): u=await require(request); rows=await q("SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 200",u["id"]); return {"items":[rowdict(x) for x in rows]}

@app.websocket("/api/ws")
async def websocket(ws:WebSocket):
    await ws.accept(); init=ws.query_params.get("initData") or ws.headers.get("x-telegram-initdata") or ""; u=tg_user(verify_initdata(init))
    if not u: await ws.close(code=1008); return
    if await one("SELECT 1 FROM banned_users WHERE user_id=$1",u["id"]): await ws.close(code=1008); return
    await ensure_user(u); ws_clients.setdefault(u["id"],set()).add(ws)
    try:
        await ws.send_json({"event":"ready","payload":{"ok":True}})
        while True: await ws.receive_text()
    except WebSocketDisconnect: pass
    finally: ws_clients.get(u["id"],set()).discard(ws)

@app.post("/api/webhook/telegram")
async def telegram_webhook(request:Request):
    if TG_WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token")!=TG_WEBHOOK_SECRET: raise HTTPException(403,"Forbidden")
    update=await request.json(); uid=update.get("update_id")
    if uid is not None:
        inserted=await one("INSERT INTO telegram_updates(update_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING update_id",uid)
        if not inserted:
            return {"ok":True,"duplicate":True}
    try:
        if update.get("pre_checkout_query") and BOT_TOKEN:
            pq=update["pre_checkout_query"]
            ok=False; error_message="Invoice is not valid"
            try:
                payload=json.loads(pq.get("invoice_payload") or "{}")
                ok=payload.get("type")=="stars_topup" and int(payload.get("user_id"))==int(pq.get("from",{}).get("id")) and int(payload.get("amount"))==int(pq.get("total_amount"))
                if ok: error_message=None
            except Exception: ok=False
            body={"pre_checkout_query_id":pq["id"],"ok":ok}
            if error_message: body["error_message"]=error_message
            async with httpx.AsyncClient(timeout=10) as x: await x.post(f"https://api.telegram.org/bot{BOT_TOKEN}/answerPreCheckoutQuery",json=body)
        payment=(update.get("message") or {}).get("successful_payment")
        if payment:
            try: payload=json.loads(payment.get("invoice_payload") or "{}")
            except: payload={}
            if payload.get("type")=="stars_topup":
                amount=int(payment.get("total_amount") or 0); user_id=int(payload.get("user_id") or 0); sender_id=int((update.get("message") or {}).get("from",{}).get("id") or 0); charge=payment.get("telegram_payment_charge_id") or payment.get("provider_payment_charge_id")
                if payload.get("type")!="stars_topup" or not user_id or user_id!=sender_id or amount<=0 or int(payload.get("amount") or 0)!=amount or not charge:
                    raise ValueError("Invalid Stars payment payload")
                async def work(c):
                    if await c.fetchval("SELECT 1 FROM transactions WHERE type='stars_topup' AND metadata->>'telegram_charge_id'=$1",charge): return False
                    await ledger(c,user_id,"STARS",amount,"stars_topup","Telegram Stars top up","telegram_payment",charge,{"telegram_charge_id":charge,"invoice_payload":payload}); await transaction(c,user_id,"stars_topup","completed","STARS",amount,"Telegram Stars top up",metadata={"telegram_charge_id":charge,"invoice_payload":payload}); return True
                if await tx(work): asyncio.create_task(notify(user_id,"stars_topup",f"Зачислено {amount} STARS."))
        msg=update.get("message") or {}
        if str(msg.get("text","")).startswith("/start"):
            uid2=int(msg["from"]["id"]); await ensure_user(tg_user(msg["from"])); arg=str(msg["text"])[6:].strip(); await bot_send(uid2,f"Открыта сделка <b>{arg[5:]}</b>. Откройте WebApp для продолжения." if arg.startswith("deal_") else "Funpay Service готов к работе. Откройте приложение через кнопку бота.", "HTML" if arg.startswith("deal_") else None)
    except Exception as e: print("Telegram webhook:",e)
    return {"ok":True}

async def scan_ton():
    if not TON_API_KEY or not TON_DEPOSIT_ADDRESS:return
    try:
        async with httpx.AsyncClient(timeout=20) as x:
            r=await x.get(f"{TON_API_URL}/v2/accounts/{TON_DEPOSIT_ADDRESS}/events",params={"limit":100},headers={"Authorization":f"Bearer {TON_API_KEY}"});
            if not r.is_success:return
            data=r.json()
        for event in data.get("events",[]):
            for action in event.get("actions",[]):
                t=action.get("TonTransfer")
                if not t or t.get("recipient",{}).get("address")!=TON_DEPOSIT_ADDRESS:continue
                amount=dec(t.get("amount",0))/Decimal(10**9); h=str(event.get("event_id") or event.get("id") or "")
                if amount<=0 or not h:continue
                async def work(c):
                    if await c.fetchval("SELECT 1 FROM processed_chain_transactions WHERE currency='TON' AND network='ton' AND tx_hash=$1",h):return
                    d=await c.fetchrow("SELECT * FROM deposit_requests WHERE currency='TON' AND tx_hash=$1 AND status='pending' FOR UPDATE",h)
                    if not d:d=await c.fetchrow("SELECT * FROM deposit_requests WHERE currency='TON' AND status='pending' AND amount=$1 AND created_at>NOW()-INTERVAL '20 minutes' AND (from_address IS NULL OR from_address=$2) ORDER BY created_at LIMIT 1 FOR UPDATE",amount,t.get("sender",{}).get("address"))
                    if not d:return
                    raw=json.dumps(jsonable(event),ensure_ascii=False)
                    await c.execute("INSERT INTO processed_chain_transactions(currency,network,tx_hash,user_id,amount,raw_data) VALUES('TON','ton',$1,$2,$3,$4::jsonb)",h,d["user_id"],amount,raw)
                    await c.execute("UPDATE deposit_requests SET amount=$1,status='completed',tx_hash=COALESCE(tx_hash,$2),processed_at=NOW(),raw_data=$3::jsonb WHERE id=$4",amount,h,raw,d["id"])
                    await ledger(c,int(d["user_id"]),"TON",amount,"deposit","TON deposit","deposit",d["id"],{"tx_hash":h,"scanner":True})
                    await transaction(c,int(d["user_id"]),"deposit","completed","TON",amount,"TON deposit",metadata={"tx_hash":h,"scanner":True})
                    return d
                d=await tx(work)
                if d: asyncio.create_task(notify(int(d["user_id"]),"deposit_completed",f"Пополнение {amount} TON зачислено."))
    except Exception as e: print("TON scanner:",e)
async def scan_usdt():
    if not TRON_API_KEY or not USDT_DEPOSIT_ADDRESS:return
    try:
        async with httpx.AsyncClient(timeout=20) as x:r=await x.get(f"{TRON_API_URL}/v1/accounts/{USDT_DEPOSIT_ADDRESS}/transactions/trc20",params={"limit":200,"contract_address":USDT_CONTRACT},headers={"TRON-PRO-API-KEY":TRON_API_KEY});
        if not r.is_success:return
        for t in r.json().get("data",[]):
            if t.get("to")!=USDT_DEPOSIT_ADDRESS or t.get("token_info",{}).get("address")!=USDT_CONTRACT:continue
            h=t.get("transaction_id"); amount=dec(t.get("value",0))/(Decimal(10)**int(t.get("token_info",{}).get("decimals",6)))
            if not h or amount<=0:continue
            async def work(c):
                if await c.fetchval("SELECT 1 FROM processed_chain_transactions WHERE currency='USDT' AND network='trc20' AND tx_hash=$1",h):return
                d=await c.fetchrow("SELECT * FROM deposit_requests WHERE currency='USDT' AND tx_hash=$1 AND status='pending' FOR UPDATE",h)
                if not d:return
                raw=json.dumps(jsonable(t),ensure_ascii=False)
                await c.execute("INSERT INTO processed_chain_transactions(currency,network,tx_hash,user_id,amount,raw_data) VALUES('USDT','trc20',$1,$2,$3,$4::jsonb)",h,d["user_id"],amount,raw)
                await c.execute("UPDATE deposit_requests SET amount=$1,status='completed',processed_at=NOW(),raw_data=$2::jsonb WHERE id=$3",amount,raw,d["id"])
                await ledger(c,int(d["user_id"]),"USDT",amount,"deposit","USDT deposit","deposit",d["id"],{"tx_hash":h,"scanner":True})
                await transaction(c,int(d["user_id"]),"deposit","completed","USDT",amount,"USDT deposit",metadata={"tx_hash":h,"scanner":True})
                return d
            d=await tx(work)
            if d: asyncio.create_task(notify(int(d["user_id"]),"deposit_completed",f"Пополнение {amount} USDT зачислено."))
    except Exception as e: print("USDT scanner:",e)

async def setup_webhook():
    global BOT_USERNAME
    if not BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as x:
            me = await x.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe")
            if me.is_success:
                BOT_USERNAME = str(me.json().get("result",{}).get("username") or "").strip()
                if BOT_USERNAME:
                    await execute("INSERT INTO settings(key,value) VALUES('bot_username',$1::jsonb) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", json.dumps(BOT_USERNAME))
            if TELEGRAM_WEBHOOK_ENABLED and TG_WEBHOOK_URL:
                r=await x.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",json={"url":TG_WEBHOOK_URL,"secret_token":TG_WEBHOOK_SECRET or None,"allowed_updates":["message","pre_checkout_query"]})
                print("Telegram webhook:",r.text)
    except Exception as e:
        print("Telegram setup:",e)

async def background():
    await asyncio.sleep(2); await setup_webhook()
    while True:
        await scan_ton(); await scan_usdt(); await asyncio.sleep(CHAIN_SCAN_MS/1000)


# --- Integrated Telegram bot (API + bot in one process) ---
WELCOME_VIDEO = os.path.join(APP_DIR, "welcome_video.mp4")

WELCOME_TEXT = """<blockquote>👋 Добро пожаловать!</blockquote>
<blockquote>💼 Funpay Service — сервис для безопасных сделок.
✨ Быстро и удобно через Telegram Mini App.</blockquote>
<blockquote>📘 Комиссия за услугу: 1%
🕓 Поддержка: @FunRelayer</blockquote>
<blockquote>💌 Сделки проходят через эскроу Funpay Service. 🛡️</blockquote>"""

async def integrated_bot_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message: return
    url=os.getenv("TELEGRAM_WEB_APP_URL", "https://wertyxan.onrender.com/").strip()
    arg=str(context.args[0]).strip() if context.args else ""
    if arg.lower().startswith("deal_"):
        tag=arg[5:].strip(); item=None
        try:
            r=await one("SELECT * FROM deals WHERE upper(tag)=upper($1)", tag); item=deal_public(r) if r else None
        except Exception: pass
        if item:
            await update.effective_message.reply_text(f"🤝 <b>Приглашение в сделку #{tag}</b>\n\nСумма: <b>{item.get('amount')} {item.get('currency')}</b>\nСтатус: <b>{item.get('status')}</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Открыть сделку", web_app=WebAppInfo(url=f"{url.rstrip('/')}/?deal={tag}"))]]))
            return
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть приложение", web_app=WebAppInfo(url=url))]])
    # Отправляем видео именно как video-сообщение, а не как документ/ссылку.
    # Файл берём относительно app.py, поэтому это работает и локально, и на Render.
    if os.path.isfile(WELCOME_VIDEO) and os.path.getsize(WELCOME_VIDEO) > 0:
        try:
            with open(WELCOME_VIDEO, "rb") as video_file:
                await update.effective_message.reply_video(
                    video=video_file,
                    caption=WELCOME_TEXT,
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True,
                    reply_markup=markup,
                )
            return
        except Exception as e:
            print("WELCOME VIDEO ERROR:", repr(e))
    # Если видео отсутствует/Telegram не принял его — не ломаем /start.
    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=markup)

async def integrated_bot_error(update, context):
    print("BOT ERROR", repr(context.error))

async def run_integrated_bot():
    token=os.getenv("TELEGRAM_BOT_TOKEN") or BOT_TOKEN
    if not token: return
    try:
        req=HTTPXRequest(connect_timeout=20, read_timeout=90, write_timeout=90, pool_timeout=20)
        botapp=TelegramApplication.builder().token(token).request(req).build()
        botapp.add_handler(CommandHandler("start", integrated_bot_start)); botapp.add_error_handler(integrated_bot_error)
        await botapp.initialize(); await botapp.start(); await botapp.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        while True: await asyncio.sleep(3600)
    except Exception as e: print("Integrated bot stopped:", repr(e))

@app.on_event("startup")
async def startup():
    global pool
    pool=await asyncpg.create_pool(DATABASE_URL,min_size=1,max_size=int(os.getenv("DB_POOL_MAX","10")),command_timeout=30)
    schema=open(os.path.join(APP_DIR,"schema.sql"),encoding="utf-8").read()
    await pool.execute(schema)
    await pool.execute("""
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS escrow_status TEXT NOT NULL DEFAULT 'none';
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS escrow_amount NUMERIC(30,8) NOT NULL DEFAULT 0;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS escrow_currency TEXT;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS escrow_account TEXT;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS dispute_id BIGINT;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS payment_currency TEXT;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS payment_amount NUMERIC(30,8) NOT NULL DEFAULT 0;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS payment_fee NUMERIC(30,8) NOT NULL DEFAULT 0;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS fee NUMERIC(30,8) NOT NULL DEFAULT 0;
        ALTER TABLE deposit_requests ADD COLUMN IF NOT EXISTS network TEXT;
        ALTER TABLE deposit_requests ADD COLUMN IF NOT EXISTS from_address TEXT;
        ALTER TABLE deposit_requests ADD COLUMN IF NOT EXISTS to_address TEXT;
        ALTER TABLE deposit_requests ADD COLUMN IF NOT EXISTS raw_data JSONB NOT NULL DEFAULT '{}';
        ALTER TABLE withdrawal_requests ADD COLUMN IF NOT EXISTS network TEXT;
        ALTER TABLE withdrawal_requests ADD COLUMN IF NOT EXISTS tx_hash TEXT;
        ALTER TABLE withdrawal_requests ADD COLUMN IF NOT EXISTS raw_data JSONB NOT NULL DEFAULT '{}';
        ALTER TABLE withdrawal_requests ADD COLUMN IF NOT EXISTS client_request_id TEXT;
        ALTER TABLE withdrawal_requests ADD COLUMN IF NOT EXISTS comment TEXT;
    """)
    await pool.execute("INSERT INTO admins(user_id,username) VALUES($1,'bootstrap') ON CONFLICT DO NOTHING",BOOTSTRAP_ADMIN)
    await pool.execute("INSERT INTO settings(key,value) VALUES('escrow_username', $1::jsonb) ON CONFLICT DO NOTHING", json.dumps(ESCROW_USERNAME))
    await pool.execute("INSERT INTO settings(key,value) VALUES('rates',$1::jsonb) ON CONFLICT DO NOTHING",json.dumps(DEFAULT_RATES))
    await pool.execute("""CREATE TABLE IF NOT EXISTS ledger_entries(id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,currency TEXT NOT NULL,amount NUMERIC(30,8) NOT NULL,entry_type TEXT NOT NULL,description TEXT,reference_type TEXT,reference_id TEXT,metadata JSONB NOT NULL DEFAULT '{}',created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
    await pool.execute("CREATE INDEX IF NOT EXISTS ledger_user_idx ON ledger_entries(user_id,created_at DESC)")
    await pool.execute("CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_tx_hash_unique ON withdrawal_requests(tx_hash) WHERE tx_hash IS NOT NULL")
    await pool.execute("CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_client_request_unique ON withdrawal_requests(user_id,client_request_id) WHERE client_request_id IS NOT NULL")
    await pool.execute("CREATE INDEX IF NOT EXISTS transactions_stars_charge_idx ON transactions((metadata->>'telegram_charge_id')) WHERE type='stars_topup'")
    asyncio.create_task(background())
    if os.getenv("RUN_BOT", "true").lower() == "true": asyncio.create_task(run_integrated_bot())

@app.on_event("shutdown")
async def shutdown():
    if pool: await pool.close()
