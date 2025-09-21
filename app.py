import os, asyncio, base64, hmac, hashlib, json, time
from typing import Optional, Any, List, Dict, Tuple

import aiosqlite
import uvicorn
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError, field_validator
import discord
from discord.ext import commands, tasks
from discord import app_commands

# =========================
# ENV (Render-compatible)
# =========================
DISCORD_TOKEN      = os.environ.get("DISCORD_TOKEN", "")
INGEST_HMAC_SECRET = os.environ.get("INGEST_HMAC_SECRET", "")
DB_PATH            = os.environ.get("DB_PATH", "/tmp/activity.sqlite3")
GUILD_ID           = os.environ.get("GUILD_ID", "0")
ADMIN_ROLE_ID      = os.environ.get("ADMIN_ROLE_ID", "0")
ADMIN_USER_IDS     = os.environ.get("ADMIN_USER_IDS", "")
LOG_CHANNEL_ID     = os.environ.get("LOG_CHANNEL_ID", "0")
LOG_INTERVAL_SEC   = int(os.environ.get("LOG_INTERVAL_SEC", "5"))
LOG_BATCH_MAX      = int(os.environ.get("LOG_BATCH_MAX", "15"))
MAX_PAYLOAD_BYTES  = int(os.environ.get("MAX_PAYLOAD_BYTES", "262144"))
TIMESTAMP_SKEW_SEC = int(os.environ.get("TIMESTAMP_SKEW_SEC", "300"))
HTTP_HOST          = "0.0.0.0"
HTTP_PORT          = int(os.environ.get("PORT") or 8000)
# =========================
import os, asyncio, base64, hmac, hashlib, json, time
from typing import Optional, Any, List, Dict, Tuple

import aiosqlite
import uvicorn
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError, field_validator
import discord
from discord.ext import commands, tasks
from discord import app_commands

def _to_int(v: Optional[str]) -> int:
    try:
        if v is None: return 0
        s = v.strip()
        return int(s) if s and s.lstrip("-").isdigit() else 0
    except Exception:
        return 0

def _parse_ids_csv(s: str) -> List[int]:
    out: List[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if part and part.lstrip("-").isdigit():
            out.append(int(part))
    return out

GUILD_ID_I       = _to_int(GUILD_ID)
ADMIN_ROLE_ID_I  = _to_int(ADMIN_ROLE_ID)
ADMIN_USER_IDS_L = _parse_ids_csv(ADMIN_USER_IDS)
LOG_CHANNEL_ID_I = _to_int(LOG_CHANNEL_ID)

VALID_EVENT_TYPES = {"JOIN","LEAVE","CHAT","PURCHASE"}

class EventIn(BaseModel):
    ts: Optional[float] = None
    user_id: int
    username: Optional[str] = None
    display_name: Optional[str] = None
    type: str
    text: Optional[str] = None
    value: Optional[float] = None
    target_user_id: Optional[int] = None
    extra: Optional[Dict[str, Any]] = None
    @field_validator("type")
    @classmethod
    def _t(cls, v: str) -> str:
        t = (v or "").upper()
        if t not in VALID_EVENT_TYPES:
            raise ValueError("invalid type")
        return t

class EventBatchIn(BaseModel):
    events: List[EventIn]

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS players(
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  display_name TEXT,
  first_seen REAL,
  last_seen REAL
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  user_id INTEGER NOT NULL,
  username TEXT,
  display_name TEXT,
  type TEXT NOT NULL,
  text TEXT,
  value REAL,
  target_user_id INTEGER,
  extra_json TEXT,
  FOREIGN KEY(user_id) REFERENCES players(user_id)
);
CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts DESC);
CREATE INDEX IF NOT EXISTS ix_events_user_ts ON events(user_id, ts DESC);
CREATE INDEX IF NOT EXISTS ix_events_type_ts ON events(type, ts DESC);
"""

class DB:
    def __init__(self, path: str):
        self.path = path
    async def init(self):
        dirn = os.path.dirname(self.path)
        if dirn and not os.path.exists(dirn):
            os.makedirs(dirn, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            await db.commit()
    async def upsert_player(self, uid: int, un: Optional[str], dn: Optional[str], ts: float):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT 1 FROM players WHERE user_id=?", (uid,))
            row = await cur.fetchone()
            if row:
                await db.execute(
                    "UPDATE players SET username=COALESCE(?,username), display_name=COALESCE(?,display_name), last_seen=MAX(?,last_seen) WHERE user_id=?",
                    (un, dn, ts, uid)
                )
            else:
                await db.execute(
                    "INSERT INTO players(user_id,username,display_name,first_seen,last_seen) VALUES (?,?,?,?,?)",
                    (uid, un, dn, ts, ts)
                )
            await db.commit()
    async def insert_events(self, items: List[EventIn]) -> int:
        if not items: return 0
        now = time.time()
        rows = []
        async with aiosqlite.connect(self.path) as db:
            for e in items:
                ts = e.ts or now
                await self.upsert_player(e.user_id, e.username, e.display_name, ts)
                rows.append((
                    ts, e.user_id, e.username, e.display_name, e.type, e.text,
                    e.value, e.target_user_id,
                    json.dumps(e.extra, separators=(",",":"), ensure_ascii=False) if e.extra else None
                ))
            await db.executemany(
                "INSERT INTO events(ts,user_id,username,display_name,type,text,value,target_user_id,extra_json) VALUES (?,?,?,?,?,?,?,?,?)",
                rows
            )
            await db.commit()
        return len(rows)
    async def search_players(self, q: str, limit: int) -> List[Tuple[int,str,str,float,float]]:
        like = f"%{q}%"
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT user_id,username,display_name,first_seen,last_seen FROM players "
                "WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? COLLATE NOCASE OR display_name LIKE ? COLLATE NOCASE "
                "ORDER BY last_seen DESC LIMIT ?", (like, like, like, limit)
            )
            return await cur.fetchall()
    async def player_events(self, q: str, t: Optional[str], limit: int) -> List[Dict[str,Any]]:
        cand = await self.search_players(q, 1)
        if not cand: return []
        uid = cand[0][0]
        args: List[Any] = [uid]
        where = "user_id=?"
        if t:
            where += " AND type=?"
            args.append(t.upper())
        args.append(limit)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                f"SELECT ts,user_id,username,display_name,type,text,value,target_user_id,COALESCE(extra_json,'{{}}') "
                f"FROM events WHERE {where} ORDER BY ts DESC LIMIT ?", args
            )
            rows = await cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "ts": r[0], "user_id": r[1], "username": r[2], "display_name": r[3],
                "type": r[4], "text": r[5], "value": r[6], "target_user_id": r[7],
                "extra": json.loads(r[8]),
            })
        return out
    async def recent_events(self, limit: int) -> List[Dict[str,Any]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT ts,user_id,username,display_name,type,text,value,target_user_id,COALESCE(extra_json,'{}') "
                "FROM events ORDER BY ts DESC LIMIT ?", (limit,)
            )
            rows = await cur.fetchall()
        return [
            {
                "ts": r[0], "user_id": r[1], "username": r[2], "display_name": r[3],
                "type": r[4], "text": r[5], "value": r[6], "target_user_id": r[7],
                "extra": json.loads(r[8]),
            } for r in rows
        ]
    async def counts_by_type(self) -> List[Tuple[str,int]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT type, COUNT(*) FROM events GROUP BY type ORDER BY COUNT(*) DESC")
            return await cur.fetchall()
    async def last_join_before(self, user_id: int, ts: float) -> Optional[float]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT MAX(ts) FROM events WHERE user_id=? AND type='JOIN' AND ts<=?", (user_id, ts))
            row = await cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    async def first_join(self, user_id: int) -> Optional[float]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT MIN(ts) FROM events WHERE user_id=? AND type='JOIN'", (user_id,))
            row = await cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    async def current_players(self) -> List[Tuple[int,str,str,float,float]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("""
                SELECT p.user_id,
                       p.username,
                       p.display_name,
                       (SELECT MAX(ts) FROM events e1 WHERE e1.user_id=p.user_id AND e1.type='JOIN') AS last_join,
                       (SELECT MAX(ts) FROM events e2 WHERE e2.user_id=p.user_id AND e2.type='LEAVE') AS last_leave
                FROM players p
            """)
            rows = await cur.fetchall()
        out = []
        for uid, un, dn, lj, ll in rows:
            ljf = float(lj) if lj is not None else 0.0
            llf = float(ll) if ll is not None else 0.0
            if ljf > llf:
                out.append((uid, un, dn, ljf, llf))
        return out
    async def last_country(self, user_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT extra_json FROM events WHERE user_id=? AND extra_json IS NOT NULL ORDER BY ts DESC LIMIT 100", (user_id,))
            rows = await cur.fetchall()
        for (j,) in rows:
            try:
                d = json.loads(j)
                c = d.get("country")
                if c: return str(c)
            except Exception:
                continue
        return None
    async def total_seconds_latest(self, user_id: int) -> Optional[int]:
        best = None
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT extra_json FROM events WHERE user_id=? AND extra_json IS NOT NULL ORDER BY ts DESC LIMIT 200", (user_id,))
            rows = await cur.fetchall()
        for (j,) in rows:
            try:
                d = json.loads(j)
                v = d.get("total_seconds")
                if v is not None:
                    v = int(v)
                    best = v if best is None else max(best, v)
            except Exception:
                continue
        return best
    async def credits_latest(self, user_id: int) -> Optional[int]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT extra_json FROM events WHERE user_id=? AND extra_json IS NOT NULL ORDER BY ts DESC LIMIT 200", (user_id,))
            rows = await cur.fetchall()
        for (j,) in rows:
            try:
                d = json.loads(j)
                v = d.get("credits")
                if v is not None:
                    return int(v)
            except Exception:
                continue
        return None
    async def purchases_by_category(self, user_id: int, category: str) -> Dict[str, int]:
        out: Dict[str, int] = {}
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT text, COALESCE(extra_json,'{}') FROM events WHERE user_id=? AND type='PURCHASE' ORDER BY ts DESC LIMIT 1000", (user_id,))
            rows = await cur.fetchall()
        for txt, j in rows:
            try:
                d = json.loads(j)
            except Exception:
                d = {}
            if (d.get("category") or "").lower() != category.lower():
                continue
            name = d.get("name") or txt or "unknown"
            out[name] = out.get(name, 0) + 1
        return out

def verify_hmac(secret: str, body: bytes, x_sig: str, x_ts: str, skew: int):
    if not secret: raise HTTPException(status_code=500, detail="server not configured")
    if not x_sig or not x_ts: raise HTTPException(status_code=401, detail="missing signature")
    try: ts = int(x_ts)
    except Exception: raise HTTPException(status_code=401, detail="bad timestamp")
    now = int(time.time())
    if abs(now - ts) > skew: raise HTTPException(status_code=401, detail="stale timestamp")
    mac = hmac.new(secret.encode(), body + x_ts.encode(), hashlib.sha256).digest()
    b64 = base64.b64encode(mac).decode()
    if x_sig != b64 and x_sig != mac.hex(): raise HTTPException(status_code=401, detail="invalid signature")

api = FastAPI()

@api.get("/")
async def root():
    return {"ok": True}

@api.get("/health")
async def health():
    return {"ok": True}

_db = DB(DB_PATH)
_log_queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=2000)

def _fmt_line(e: Dict[str, Any]) -> str:
    who = str(e.get("display_name") or e.get("username") or e.get("user_id"))
    extra = e.get("extra") or {}
    ctry = extra.get("country")
    if ctry:
        who = f"{who} [{ctry}]"
    t   = e.get("type") or "?"
    txt = e.get("text") or ""
    if len(txt) > 100: txt = txt[:97] + "…"
    return f"[{t}] {who} {('- ' + txt) if txt else ''}".rstrip()

@api.post("/ingest")
async def ingest(request: Request, x_signature: str = Header(None), x_timestamp: str = Header(None)):
    body = await request.body()
    if len(body) > MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="payload too large")
    verify_hmac(INGEST_HMAC_SECRET, body, x_signature or "", x_timestamp or "", TIMESTAMP_SKEW_SEC)
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")
    try:
        if isinstance(data, dict) and "events" in data:
            items = EventBatchIn(**data).events
        elif isinstance(data, dict):
            items = [EventIn(**data)]
        elif isinstance(data, list):
            items = [EventIn(**d) for d in data]
        else:
            raise HTTPException(status_code=400, detail="invalid shape")
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
    n = await _db.insert_events(items)
    if LOG_CHANNEL_ID_I:
        for e in items:
            et = (e.type or "").upper()
            if et not in ("JOIN","LEAVE","CHAT","PURCHASE"):
                continue
            ed = e.model_dump()
            if et == "LEAVE":
                leave_ts = ed.get("ts") or time.time()
                lj = await _db.last_join_before(e.user_id, leave_ts)
                if lj is not None:
                    secs = max(0, int(round(leave_ts - lj)))
                    mins = secs // 60
                    add = f"session {mins}m {secs%60}s"
                    if ed.get("text"):
                        ed["text"] = f'{ed["text"]} | {add}'
                    else:
                        ed["text"] = add
            try:
                await _log_queue.put(_fmt_line(ed))
            except asyncio.QueueFull:
                break
    return JSONResponse({"ingested": n})

intents = discord.Intents.default()
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

def _has_admin(member: discord.Member) -> bool:
    if ADMIN_ROLE_ID_I == 0 and not ADMIN_USER_IDS_L:
        return True
    if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
        return True
    if ADMIN_ROLE_ID_I and any(r.id == ADMIN_ROLE_ID_I for r in getattr(member, "roles", [])):
        return True
    if ADMIN_USER_IDS_L and member.id in ADMIN_USER_IDS_L:
        return True
    return False

def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            raise app_commands.CheckFailure("ADMIN")
        if _has_admin(interaction.user):
            return True
        raise app_commands.CheckFailure("ADMIN")
    return app_commands.check(predicate)

def _fmt_ts(ts: float) -> str:
    return f"<t:{int(ts)}:f>"

@bot.tree.command(name="search", description="Search players by id/username/display")
@app_commands.describe(query="id/username/display", limit="<=25")
@admin_only()
async def search_cmd(interaction: discord.Interaction, query: str, limit: int = 10):
    rows = await _db.search_players(query, limit=max(1, min(25, limit)))
    if not rows:
        await interaction.response.send_message("No match.", ephemeral=True); return
    emb = discord.Embed(title=f"Players • {query}")
    for uid, un, dn, fs, ls in rows:
        emb.add_field(name=f"{dn or un or uid} • {uid}", value=f"user: {un or '—'}\nfirst: {_fmt_ts(fs)}\nlast: {_fmt_ts(ls)}", inline=False)
    await interaction.response.send_message(embed=emb, ephemeral=True)

@bot.tree.command(name="activity", description="Timeline for a player")
@app_commands.describe(query="id/username/display", limit="<=100")
@admin_only()
async def activity_cmd(interaction: discord.Interaction, query: str, limit: int = 25):
    evs = await _db.player_events(query, None, max(1, min(100, limit)))
    if not evs:
        await interaction.response.send_message("No events.", ephemeral=True); return
    who = f"{evs[0].get('display_name') or evs[0].get('username') or evs[0]['user_id']}"
    emb = discord.Embed(title=f"Activity • {who}")
    for e in evs:
        parts = [e["type"]]
        if e.get("text"):
            t = e["text"]
            parts.append((t[:160] + "…") if len(t) > 160 else t)
        if e.get("value") is not None:
            parts.append(f"value={e['value']}")
        if e.get("target_user_id"):
            parts.append(f"target={e['target_user_id']}")
        emb.add_field(name=_fmt_ts(e["ts"]), value=" • ".join(parts), inline=False)
    await interaction.response.send_message(embed=emb, ephemeral=True)

@bot.tree.command(name="recent", description="Recent events")
@app_commands.describe(limit="<=50")
@admin_only()
async def recent_cmd(interaction: discord.Interaction, limit: int = 25):
    evs = await _db.recent_events(max(1, min(50, limit)))
    if not evs:
        await interaction.response.send_message("No events.", ephemeral=True); return
    emb = discord.Embed(title="Recent Events")
    for e in evs:
        who = e.get("display_name") or e.get("username") or e["user_id"]
        txt = e.get("text") or ""
        if len(txt) > 120: txt = txt[:117] + "…"
        emb.add_field(name=f"{_fmt_ts(e['ts'])} • {e['type']}", value=f"{who} — {txt}" if txt else f"{who}", inline=False)
    await interaction.response.send_message(embed=emb, ephemeral=True)

@bot.tree.command(name="stats", description="Event counts by type")
@admin_only()
async def stats_cmd(interaction: discord.Interaction):
    pairs = await _db.counts_by_type()
    if not pairs:
        await interaction.response.send_message("No data.", ephemeral=True); return
    mx = max(c for _, c in pairs) if pairs else 1
    bar = lambda n: ("█" * int((n/mx)*20)) + ("░" * (20 - int((n/mx)*20)))
    lines = [f"{t:9s} {c:6d} {bar(c)}" for t, c in pairs]
    await interaction.response.send_message("```\n" + "\n".join(lines) + "\n```", ephemeral=True)

@bot.tree.command(name="gamepasses", description="Gamepasses bought by player")
@app_commands.describe(roblox_user_id="numeric Roblox user id")
@admin_only()
async def gamepasses_cmd(interaction: discord.Interaction, roblox_user_id: str):
    try: uid = int(roblox_user_id)
    except: await interaction.response.send_message("Bad id.", ephemeral=True); return
    data = await _db.purchases_by_category(uid, "gamepass")
    if not data:
        await interaction.response.send_message("None.", ephemeral=True); return
    total = sum(data.values())
    emb = discord.Embed(title=f"Gamepasses • {uid}", description=f"Total: {total}")
    for name, cnt in sorted(data.items(), key=lambda x: (-x[1], x[0])):
        emb.add_field(name=name, value=str(cnt), inline=True)
    await interaction.response.send_message(embed=emb, ephemeral=True)

@bot.tree.command(name="devproducts", description="Developer products bought by player")
@app_commands.describe(roblox_user_id="numeric Roblox user id")
@admin_only()
async def devproducts_cmd(interaction: discord.Interaction, roblox_user_id: str):
    try: uid = int(roblox_user_id)
    except: await interaction.response.send_message("Bad id.", ephemeral=True); return
    data = await _db.purchases_by_category(uid, "devproduct")
    if not data:
        await interaction.response.send_message("None.", ephemeral=True); return
    total = sum(data.values())
    emb = discord.Embed(title=f"Dev Products • {uid}", description=f"Total: {total}")
    for name, cnt in sorted(data.items(), key=lambda x: (-x[1], x[0])):
        emb.add_field(name=name, value=str(cnt), inline=True)
    await interaction.response.send_message(embed=emb, ephemeral=True)

@bot.tree.command(name="currentlyplaying", description="Players currently in-game")
@admin_only()
async def currentlyplaying_cmd(interaction: discord.Interaction):
    rows = await _db.current_players()
    if not rows:
        await interaction.response.send_message("None.", ephemeral=True); return
    rows.sort(key=lambda r: r[3])
    lines = []
    now = time.time()
    for uid, un, dn, lj, _ in rows[:200]:
        mins = int((now - lj)//60)
        lines.append(f"{dn or un or uid} • {uid} • {mins}m")
    chunks = ["\n".join(lines[i:i+25]) for i in range(0, len(lines), 25)]
    await interaction.response.send_message("```\n" + chunks[0] + "\n```", ephemeral=True)
    for ch in chunks[1:]:
        await interaction.followup.send("```\n" + ch + "\n```", ephemeral=True)

@bot.tree.command(name="firstjoin", description="First time player joined")
@app_commands.describe(roblox_user_id="numeric Roblox user id")
@admin_only()
async def firstjoin_cmd(interaction: discord.Interaction, roblox_user_id: str):
    try: uid = int(roblox_user_id)
    except: await interaction.response.send_message("Bad id.", ephemeral=True); return
    ts = await _db.first_join(uid)
    if not ts:
        await interaction.response.send_message("No data.", ephemeral=True); return
    await interaction.response.send_message(_fmt_ts(ts), ephemeral=True)

@bot.tree.command(name="country", description="Last known country for player")
@app_commands.describe(roblox_user_id="numeric Roblox user id")
@admin_only()
async def country_cmd(interaction: discord.Interaction, roblox_user_id: str):
    try: uid = int(roblox_user_id)
    except: await interaction.response.send_message("Bad id.", ephemeral=True); return
    c = await _db.last_country(uid)
    await interaction.response.send_message(c or "unknown", ephemeral=True)

@bot.tree.command(name="totaltime", description="Total minutes played across all sessions")
@app_commands.describe(roblox_user_id="numeric Roblox user id")
@admin_only()
async def totaltime_cmd(interaction: discord.Interaction, roblox_user_id: str):
    try: uid = int(roblox_user_id)
    except: await interaction.response.send_message("Bad id.", ephemeral=True); return
    secs = await _db.total_seconds_latest(uid)
    if secs is None:
        await interaction.response.send_message("No data.", ephemeral=True); return
    mins = secs // 60
    await interaction.response.send_message(f"{mins}", ephemeral=True)

@bot.tree.command(name="credits", description="Current credits for player")
@app_commands.describe(roblox_user_id="numeric Roblox user id")
@admin_only()
async def credits_cmd(interaction: discord.Interaction, roblox_user_id: str):
    try: uid = int(roblox_user_id)
    except: await interaction.response.send_message("Bad id.", ephemeral=True); return
    cr = await _db.credits_latest(uid)
    if cr is None:
        await interaction.response.send_message("No data.", ephemeral=True); return
    await interaction.response.send_message(f"{cr}", ephemeral=True)

@bot.tree.error
async def on_app_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        msg = "Forbidden." if isinstance(error, app_commands.CheckFailure) else "Error."
        if interaction.response.is_done(): await interaction.followup.send(msg, ephemeral=True)
        else: await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass

_last_posted: float = 0.0

@tasks.loop(seconds=1)
async def _poster():
    if not LOG_CHANNEL_ID_I:
        return
    channel = bot.get_channel(LOG_CHANNEL_ID_I)
    if not channel:
        return
    global _last_posted
    now = time.time()
    if (now - _last_posted) < max(1, LOG_INTERVAL_SEC):
        return
    lines: List[str] = []
    try:
        while len(lines) < LOG_BATCH_MAX:
            line = _log_queue.get_nowait()
            lines.append(line)
    except asyncio.QueueEmpty:
        pass
    if not lines:
        return
    _last_posted = now
    msg = "Latest events:\n" + "\n".join(lines)
    try:
        await channel.send(msg)
    except Exception:
        for l in lines:
            try: _log_queue.put_nowait(l)
            except asyncio.QueueFull: break

@bot.event
async def on_ready():
    try:
        if GUILD_ID_I:
            obj = discord.Object(id=GUILD_ID_I)
            bot.tree.copy_global_to(guild=obj)
            await bot.tree.sync(guild=obj)
        else:
            await bot.tree.sync()
    except Exception:
        pass
    if not _poster.is_running():
        _poster.start()

async def _run_http():
    await _db.init()
    cfg = uvicorn.Config(api, host=HTTP_HOST, port=HTTP_PORT, log_level="info")
    srv = uvicorn.Server(cfg)
    await srv.serve()

async def main():
    if not DISCORD_TOKEN: raise SystemExit("DISCORD_TOKEN not set")
    if not INGEST_HMAC_SECRET: raise SystemExit("INGEST_HMAC_SECRET not set")
    await asyncio.gather(_run_http(), bot.start(DISCORD_TOKEN))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
