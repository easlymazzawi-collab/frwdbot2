"""
tool_tauto_bot.py  v1 (hybrid bot + user copy)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Nâng cấp từ userbot v20 → Bot nhận bài + user session copy ra kênh:

  • Forward bài vào BOT (DM) — lệnh tương tác, không cần nhóm trung gian
  • Bot KHÔNG giữ emoji premium khi copy ra kênh/nhóm (giới hạn Bot API)
  • User session (test_session) copy_message / copy_media_group ra kênh
    → giữ emoji premium, entities, caption (cần tài khoản Premium)
  • User session: folder sync, ads đọc, topic, /botadd

Luồng:
  1. Forward bài vào bot (chat riêng)
  2. /done* / /xdone / /zdone  → xếp sequence
  3. Gõ tên kênh / tap lệnh   → user copy ra kênh đích
  4. Auto reset, sẵn sàng batch tiếp
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import copy
import inspect
import os
import json
import random
import re as _re_cmd
import time
import traceback
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.types import Message, ChatPrivileges
from pyrogram.errors import (
    FloodWait,
    ChannelInvalid,
    ChannelPrivate,
    ChatWriteForbidden,
    PeerIdInvalid,
    UserBannedInChannel,
    ChatAdminRequired,
    UserAlreadyParticipant,
)

load_dotenv()

API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

_ads_env = (os.getenv("ADS_CHAT") or "").strip()
if _ads_env.startswith("@"):
    ADS_CHAT: int | str = _ads_env
else:
    ADS_CHAT = int(_ads_env)

# ID thực sau khi resolve (có thể khác .env nếu cache/warm dialogs)
ads_chat_resolved: int | str | None = None
ads_bot_accessible: bool = False
# Session userbot cũ (giống tool__tauto_nostage: Client("test_session", ...))
USER_SESSION   = os.getenv("USER_SESSION", "test_session")
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

# ALLOWED_USER_IDS: nếu trống → tự lấy từ user session khi start
_allowed_raw = os.getenv("ALLOWED_USER_IDS") or os.getenv("ALLOWED_USER_ID") or ""
ALLOWED_USER_IDS = {int(x.strip()) for x in _allowed_raw.split(",") if x.strip()}

CHANNELS_FILE  = "channels.json"
FOLDERS_FILE   = "folders.json"
FAILED_FILE    = "failed_msgs.json"
RR_FILE        = "topic_rr.json"

FOLDER_SYNC_INTERVAL_SEC = 3600
DEAD_CHECK_INTERVAL_SEC  = 6 * 3600

FWD_MAX_RETRY               = 6
FWD_BETWEEN_CHANNELS_SEC    = 2.0
FWD_MAX_CONCURRENT_CHANNELS = 2
FWD_COPY_MIN_DELAY          = 1.0
FWD_GLOBAL_RATE             = 10.0
FWD_GLOBAL_BURST            = 15

DEAD_CHANNEL_ERRORS = (
    ChannelInvalid,
    ChannelPrivate,
    UserBannedInChannel,
)

# Peer chưa cache — warm/import rồi retry, KHÔNG coi là kênh chết
PEER_RETRY_ERRORS = (PeerIdInvalid,)

COPY_PEER_RETRIES = 3

SKIP_NOT_DEAD_ERRORS = (
    ChatWriteForbidden,
    ChatAdminRequired,
)


# ─────────────────────────────────────────────────────────
# TokenBucket
# ─────────────────────────────────────────────────────────

class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate     = rate
        self.capacity = capacity
        self.tokens   = capacity
        self.last     = time.monotonic()
        self.lock     = asyncio.Lock()

    async def acquire(self, n: float = 1.0):
        async with self.lock:
            now     = time.monotonic()
            elapsed = now - self.last
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last   = now
            if self.tokens >= n:
                self.tokens -= n
                return
            need = n - self.tokens
            wait = need / self.rate
            self.tokens = 0
            self.last   = now + wait
        await asyncio.sleep(wait)


_global_bucket: "TokenBucket | None" = None

app = Client(
    "tauto_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

user_app: "Client | None" = None
_bot_self_id: int | None = None

fwd_lock             = asyncio.Lock()
_channels_write_lock = asyncio.Lock()

_flood_gate  = asyncio.Lock()
_flood_until = 0.0


def log(tag, msg):
    print(f"[{tag}] {msg}")


async def flood_wait_globally(seconds: float, source: str = ""):
    global _flood_until
    async with _flood_gate:
        now       = time.monotonic()
        remaining = _flood_until - now
        if remaining > 0:
            log("FLOOD", f"[{source}] gate đang chờ {remaining:.0f}s — xếp hàng")
            await asyncio.sleep(remaining)
            return
        _flood_until = time.monotonic() + seconds
        log("FLOOD", f"[{source}] FloodWait toàn cục {seconds:.0f}s")
        await asyncio.sleep(seconds)
        _flood_until = 0.0


# ─────────────────────────────────────────────────────────
# State
# ─────────────────────────────────────────────────────────

def make_slot():
    return {
        "user_chat_id":      None,
        "content_msgs":      [],
        "seen_media_groups": set(),
        "ads_msgs":          [],
        "ads_chat_id":       None,
        "ads_index":         0,
        "final_sequence":    [],
        "menu_msg_id":       None,
        "waiting":           True,
        "awaiting_channel":  False,
        "channel_commands":  {},
        "topic_id":          None,
        "topic_title":       None,
        "topic_checked":     False,
        "all_mode":          False,
        "_album_pending":    0,
        "total_media_count": 0,
    }

state = {
    "slots":    [make_slot()],
    "checking": False,
}


def slot_for_user(chat_id: int):
    for s in state["slots"]:
        if s.get("user_chat_id") == chat_id and (s["waiting"] or s["awaiting_channel"]):
            return s
    s = make_slot()
    s["user_chat_id"] = chat_id
    state["slots"].append(s)
    return s


def active_slot(chat_id: int | None = None):
    if chat_id is not None:
        return slot_for_user(chat_id)
    for s in reversed(state["slots"]):
        if s["waiting"] or s["awaiting_channel"]:
            return s
    if not state["slots"]:
        state["slots"].append(make_slot())
    return state["slots"][-1]


def waiting_slot(chat_id: int | None = None):
    for s in state["slots"]:
        if s["awaiting_channel"]:
            if chat_id is None or s.get("user_chat_id") == chat_id:
                return s
    return None


def reset_slot(slot):
    uid = slot.get("user_chat_id")
    slot["content_msgs"].clear()
    slot["seen_media_groups"].clear()
    slot["ads_index"]        = 0
    slot["ads_msgs"]         = []
    slot["final_sequence"]   = []
    slot["menu_msg_id"]      = None
    slot["waiting"]          = True
    slot["awaiting_channel"] = False
    slot["channel_commands"] = {}
    slot["topic_id"]         = None
    slot["topic_title"]      = None
    slot["topic_checked"]    = False
    slot["all_mode"]         = False
    slot["_album_pending"]   = 0
    slot["total_media_count"] = 0
    slot["user_chat_id"]     = uid
    slot.pop("ads_bot_copy", None)
    slot.pop("_topic_event", None)


def reset_state(chat_id: int):
    slot = slot_for_user(chat_id)
    reset_slot(slot)
    state["slots"] = [s for s in state["slots"] if s["waiting"] or s["awaiting_channel"]]
    if not any(s.get("user_chat_id") == chat_id for s in state["slots"]):
        ns = make_slot()
        ns["user_chat_id"] = chat_id
        state["slots"].append(ns)
    log("RESET", f"State reset user={chat_id}")


def is_allowed(user_id: int) -> bool:
    if not ALLOWED_USER_IDS:
        return False
    return user_id in ALLOWED_USER_IDS


def _session_db_path(name: str) -> str:
    return f"{name}.session"


def _repair_session_db(session_name: str):
    """Sửa schema SQLite session cũ (lỗi 'no such column: number')."""
    path = _session_db_path(session_name)
    if not os.path.exists(path):
        return
    try:
        import sqlite3
        conn = sqlite3.connect(path)
        cur  = conn.cursor()
        cur.execute("PRAGMA table_info(sessions)")
        cols = {row[1] for row in cur.fetchall()}
        for col, typ in (
            ("number", "TEXT"),
            ("takeout_id", "INTEGER"),
            ("last_update", "INTEGER"),
        ):
            if col not in cols:
                cur.execute(f"ALTER TABLE sessions ADD COLUMN {col} {typ}")
        conn.commit()
        conn.close()
        log("USER", f"Đã migrate session DB: {path}")
    except Exception as e:
        log("WARN", f"session migrate {path}: {e}")


def _resolve_user_session_name() -> str | None:
    if SESSION_STRING:
        return None
    candidates = []
    if os.getenv("USER_SESSION"):
        candidates.append(USER_SESSION)
    candidates.extend(["test_session", "user_session", USER_SESSION])
    seen = set()
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        if os.path.exists(_session_db_path(name)):
            return name
    return USER_SESSION if os.path.exists(_session_db_path(USER_SESSION)) else None


async def get_bot_id() -> int:
    global _bot_self_id
    if _bot_self_id is None:
        me = await app.get_me()
        _bot_self_id = me.id
    return _bot_self_id


async def _client_for_chat(chat_id) -> Client:
    """Đọc metadata. Content DM → bot. Ads → bot hoặc user."""
    if _is_user_dm_chat(chat_id):
        return app
    if _is_ads_chat_id(chat_id) and not ads_bot_accessible:
        uc = await ensure_user_client()
        if uc:
            return uc
    return app


async def _reader_client() -> Client:
    """Đọc metadata (topic) — ưu tiên user session."""
    uc = await ensure_user_client()
    return uc or app


# ─────────────────────────────────────────────────────────
# Messaging (gửi về DM user, không qua nhóm trung gian)
# ─────────────────────────────────────────────────────────

SEND_MAX_RETRY = 6
SEND_MAX_WAIT  = 300
SEND_SPACING   = 0.4

_send_locks: dict[int, asyncio.Lock] = {}
_last_send_ts: dict[int, float] = {}


def _lock_for(chat_id: int) -> asyncio.Lock:
    if chat_id not in _send_locks:
        _send_locks[chat_id] = asyncio.Lock()
    return _send_locks[chat_id]


async def _spacing(chat_id: int):
    now = time.monotonic()
    last = _last_send_ts.get(chat_id, 0.0)
    gap = now - last
    if gap < SEND_SPACING:
        await asyncio.sleep(SEND_SPACING - gap)
    _last_send_ts[chat_id] = time.monotonic()


async def robust_send(text, chat_id: int,
                      max_retries=SEND_MAX_RETRY,
                      max_wait=SEND_MAX_WAIT):
    async with _lock_for(chat_id):
        for attempt in range(max_retries):
            try:
                await _spacing(chat_id)
                return await app.send_message(chat_id, text)
            except FloodWait as e:
                wait = min(e.value + 1, max_wait)
                log("FLOOD", f"robust_send FloodWait {wait}s — retry {attempt+1}/{max_retries}")
                await asyncio.sleep(wait)
            except Exception as e:
                log("WARN", f"robust_send fail: {type(e).__name__}: {e}")
                return None
        log("ERROR", f"robust_send bỏ cuộc — MẤT MSG: {text[:60]!r}")
        return None


async def robust_edit(chat_id, msg_id, text,
                      max_retries=SEND_MAX_RETRY,
                      max_wait=SEND_MAX_WAIT):
    async with _lock_for(chat_id):
        for attempt in range(max_retries):
            try:
                await _spacing(chat_id)
                await app.edit_message_text(chat_id, msg_id, text)
                return True
            except FloodWait as e:
                wait = min(e.value + 1, max_wait)
                log("FLOOD", f"robust_edit FloodWait {wait}s — retry {attempt+1}/{max_retries}")
                await asyncio.sleep(wait)
            except Exception as e:
                log("WARN", f"robust_edit fail: {type(e).__name__}: {e}")
                return False
        return False


async def safe_send(text, chat_id: int):
    await robust_send(text, chat_id)


# ─────────────────────────────────────────────────────────
# Channel list helpers
# ─────────────────────────────────────────────────────────

_channels_cache = None


def load_channels():
    global _channels_cache
    if _channels_cache is not None:
        return list(_channels_cache)
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
            _channels_cache = json.load(f)
            return list(_channels_cache)
    _channels_cache = []
    return []


def save_channels(channels):
    global _channels_cache
    _channels_cache = list(channels)
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)


async def remove_dead_channel(chat_id):
    async with _channels_write_lock:
        channels = load_channels()
        new_list = [ch for ch in channels if str(ch.get("id")) != str(chat_id)]
        if len(new_list) == len(channels):
            return None
        removed = next((ch for ch in channels if str(ch.get("id")) == str(chat_id)), None)
        save_channels(new_list)
        return (removed or {}).get("title", str(chat_id))


def get_ads_chat_id() -> int | str:
    return ads_chat_resolved if ads_chat_resolved is not None else ADS_CHAT


def _is_ads_chat_id(cid) -> bool:
    if cid is None:
        return False
    known = {get_ads_chat_id()}
    if isinstance(ADS_CHAT, int):
        known.add(ADS_CHAT)
    if isinstance(ads_chat_resolved, int):
        known.add(ads_chat_resolved)
    return cid in known


async def bot_has_ads_access() -> bool:
    return ads_bot_accessible


async def _resolve_bot_ads_chat():
    """
    Bot KHÔNG dùng được get_dialogs (BOT_METHOD_INVALID).
    Thử: get_chat(id/@username) → get_chat_member(bot) → user session ref.
    """
    global ads_chat_resolved, ads_bot_accessible

    if ads_bot_accessible:
        try:
            chat = await app.get_chat(get_ads_chat_id())
            ads_chat_resolved = chat.id
            return chat
        except Exception:
            ads_bot_accessible = False

    target   = ADS_CHAT
    user_ref = None
    uc       = await ensure_user_client()

    if uc:
        try:
            user_ref = await uc.get_chat(target)
            log("CONFIG", f"User session thấy ads: '{user_ref.title}' id={user_ref.id}")
        except Exception as e:
            log("WARN", f"user get_chat ads: {e}")

    candidates: list = []
    if isinstance(target, str):
        candidates.append(target)
    else:
        candidates.append(target)
    if user_ref:
        if user_ref.username:
            candidates.insert(0, user_ref.username)
        if user_ref.id not in candidates:
            candidates.append(user_ref.id)

    seen, unique = set(), []
    for c in candidates:
        key = str(c).lower()
        if key not in seen:
            seen.add(key)
            unique.append(c)

    for c in unique:
        try:
            chat = await app.get_chat(c)
            ads_chat_resolved  = chat.id
            ads_bot_accessible = True
            log("START", f"ADS_CHAT ✓ bot '{chat.title}' id={ads_chat_resolved}")
            return chat
        except Exception as e:
            log("WARN", f"bot get_chat({c}): {type(e).__name__}: {e}")

    me = await app.get_me()
    for c in unique:
        try:
            member = await app.get_chat_member(c, me.id)
            if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
                log("WARN", f"bot là member nhưng status={member.status} trong {c}")
                continue
            cid = getattr(getattr(member, "chat", None), "id", None) or (
                user_ref.id if user_ref else c
            )
            ads_chat_resolved  = cid
            ads_bot_accessible = True
            try:
                chat = await app.get_chat(cid)
                log("START", f"ADS_CHAT ✓ bot member '{chat.title}' id={cid}")
                return chat
            except Exception:
                log("START", f"ADS_CHAT ✓ bot member id={cid} (get_chat cache sau)")
                return user_ref
        except Exception as e:
            log("WARN", f"bot get_chat_member({c}): {type(e).__name__}: {e}")

    if user_ref:
        ads_chat_resolved  = user_ref.id
        ads_bot_accessible = False
        log("WARN", "User đọc ads OK — bot chưa vào nhóm hoặc chưa cache peer")
        if user_ref.username:
            log("WARN", f"  Thử .env: ADS_CHAT=@{user_ref.username}")
        log("WARN", "  Copy ra kênh vẫn qua user session (đọc ads OK)")
        return None

    return None


async def mark_ads_chat_live(chat_id: int, title: str = ""):
    """Bot nhận tin trong nhóm ads → cache peer (tùy chọn, không dùng để copy kênh)."""
    global ads_chat_resolved, ads_bot_accessible
    if ads_bot_accessible and ads_chat_resolved == chat_id:
        return
    ads_chat_resolved  = chat_id
    ads_bot_accessible = True
    log("ADS", f"✓ Bot live trong '{title or chat_id}' (peer cached)")


async def resolve_ads_chat_for_bot() -> bool:
    chat = await _resolve_bot_ads_chat()
    if chat and ads_bot_accessible:
        return True
    if ads_chat_resolved and not ads_bot_accessible:
        return False
    me = await app.get_me()
    log("WARN", "Bot chưa resolve được ADS_CHAT")
    log("WARN", f"  .env ADS_CHAT = {ADS_CHAT}")
    log("WARN", f"  Thêm @{me.username} vào nhóm ads → gửi 1 tin trong nhóm → restart")
    log("WARN", "  Hoặc dùng ADS_CHAT=@username thay vì id số")
    return False


async def cmd_checkads(reply_chat_id: int):
    chat = await _resolve_bot_ads_chat()

    lines = [
        "🔍 Chẩn đoán ADS_CHAT",
        "━━━━━━━━━━━━━━━",
        f".env ADS_CHAT = {ADS_CHAT}",
        f"Resolved     = {get_ads_chat_id()}",
        "Gửi ra kênh  = 👤 user session (giữ emoji premium)",
        f"Bot trong ads = {'✅' if ads_bot_accessible else '⚠️ không bắt buộc'}",
    ]

    if ads_bot_accessible:
        title = (getattr(chat, "title", None) if chat else None) or ads_chat_resolved
        lines.append(f"✅ Bot live: {title} (id={ads_chat_resolved})")
    else:
        lines.append("ℹ️ Bot chưa cache peer nhóm ads (không ảnh hưởng copy — user đảm nhiệm)")

    uc = await ensure_user_client()
    if uc:
        try:
            uch = await uc.get_chat(ADS_CHAT)
            lines += [
                "━━━━━━━━━━━━━━━",
                f"👤 User thấy: {uch.title} id={uch.id}",
            ]
            if uch.username:
                lines.append(f"💡 Thử .env: ADS_CHAT=@{uch.username}")
            if isinstance(ADS_CHAT, int) and uch.id != ADS_CHAT:
                lines.append(f"⚠️ Sửa .env: ADS_CHAT={uch.id}")
        except Exception as e:
            lines.append(f"❌ User không thấy ADS_CHAT: {e}")

    lines += [
        "━━━━━━━━━━━━━━━",
        "ℹ️ Đọc ads + copy ra kênh đều qua user session",
        "ℹ️ Bot chỉ nhận bài DM + lệnh (emoji premium kênh cần user Premium)",
    ]
    await safe_send("\n".join(lines), reply_chat_id)


def get_match_key(title: str) -> str:
    parts = title.strip().split(None, 1)
    if len(parts) >= 2:
        return parts[1].strip().lower()
    return (parts[0] if parts else "").lower()


def _lookup_channel(chat_id):
    for ch in load_channels():
        if str(ch.get("id")) == str(chat_id):
            return ch
    return None


def find_channels(query: str):
    q = query.lower().strip()
    if not q:
        return []
    result = []
    for ch in load_channels():
        alias     = ch.get("alias", "").lower().strip()
        title     = ch.get("title", "") or ""
        match_key = get_match_key(title)
        if alias:
            if q in alias:
                result.append(ch)
        else:
            if match_key and q in match_key:
                result.append(ch)
    return result


# ─────────────────────────────────────────────────────────
# Topic detection (bot fetch message gốc từ forward metadata)
# ─────────────────────────────────────────────────────────

_topic_title_cache = {}


async def resolve_forward_topic(client, msg: Message):
    try:
        if not msg.forward_from_chat or not msg.forward_from_message_id:
            return (None, None, None)
        src_id = msg.forward_from_chat.id
        smid   = msg.forward_from_message_id
        reader = await _reader_client()

        try:
            omsg = await reader.get_messages(src_id, smid)
            if omsg and omsg.topic:
                top_id    = omsg.topic.id
                top_title = omsg.topic.title or f"topic {top_id}"
                _topic_title_cache[top_id] = top_title
                return (src_id, top_id, top_title)
        except Exception as e:
            log("WARN", f"resolve_forward_topic get_messages: {e}")

        uc = await ensure_user_client()
        if not uc:
            return (src_id, None, None)

        from pyrogram.raw import functions as fn, types as tt
        try:
            ipc = await uc.resolve_peer(src_id)
            if not hasattr(ipc, "channel_id"):
                return (src_id, None, None)
            inch = tt.InputChannel(channel_id=ipc.channel_id, access_hash=ipc.access_hash)
            og   = await uc.invoke(fn.channels.GetMessages(
                channel=inch, id=[tt.InputMessageID(id=smid)]
            ))
            omsg = og.messages[0]
            rt   = getattr(omsg, "reply_to", None)
            if rt is not None and getattr(rt, "forum_topic", False):
                top_id = (getattr(rt, "reply_to_top_id", None)
                          or getattr(rt, "reply_to_msg_id", None))
            else:
                top_id = 1
            title = _topic_title_cache.get(top_id)
            if title is None:
                try:
                    ft = await uc.invoke(fn.channels.GetForumTopicsByID(
                        channel=inch, topics=[top_id]
                    ))
                    title = (ft.topics[0].title if getattr(ft, "topics", None)
                             else f"topic {top_id}")
                except Exception:
                    title = "General" if top_id == 1 else f"topic {top_id}"
                _topic_title_cache[top_id] = title
            return (src_id, top_id, title)
        except Exception as e:
            log("WARN", f"resolve_forward_topic raw: {type(e).__name__}: {e}")
            return (src_id, None, None)
    except Exception as e:
        log("WARN", f"resolve_forward_topic: {type(e).__name__}: {e}")
        return (None, None, None)


# ─────────────────────────────────────────────────────────
# Topic map
# ─────────────────────────────────────────────────────────

TOPIC_MAP_TXT = "topic_map.txt"

TOPIC_MAP_TEMPLATE = (
    "# ===== MAP TOPIC -> KÊNH (sửa tay file này) =====\n"
    "# Mỗi dòng 1 mapping:   tên_topic = tên_kênh\n"
    "# Dòng # là ghi chú. Sửa xong lưu là dùng được ngay.\n"
    "#\n"
    "# Ví dụ:\n"
    "# vitamin = pro\n"
    "# real    = real\n"
    "#\n"
    "# ----- Cấu hình xếp bài -----\n"
    "# @xepbai = on\n"
    "# @xepbaiwhite =\n"
)


def ensure_topic_map_txt():
    if not os.path.exists(TOPIC_MAP_TXT):
        try:
            with open(TOPIC_MAP_TXT, "w", encoding="utf-8") as f:
                f.write(TOPIC_MAP_TEMPLATE)
            log("MAP", f"Tạo file mẫu {TOPIC_MAP_TXT}")
        except Exception as e:
            log("WARN", f"ensure_topic_map_txt: {e}")


def _read_topic_lines():
    out = []
    if not os.path.exists(TOPIC_MAP_TXT):
        return out
    try:
        with open(TOPIC_MAP_TXT, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if "#" in s:
                    s = s.split("#", 1)[0].strip()
                if not s:
                    continue
                sep = "=" if "=" in s else (":" if ":" in s else None)
                if not sep:
                    continue
                left, _, right = s.partition(sep)
                out.append((left.strip(), right.strip()))
    except Exception as e:
        log("WARN", f"_read_topic_lines: {e}")
    return out


def load_topic_txt():
    return [(l, r.lstrip("/")) for (l, r) in _read_topic_lines()
            if l and r and not l.startswith("@")]


def get_xepbai_mode():
    val = "on"
    for l, r in _read_topic_lines():
        if l.lower() == "@xepbai":
            val = "off" if r.strip().lower() == "off" else "on"
    return val


def get_xepbai_whitelist():
    wl   = set()
    seen = False
    for l, r in _read_topic_lines():
        if l.lower() == "@xepbaiwhite":
            wl   = {x.strip().lower().lstrip("/") for x in r.replace(" ", ",").split(",") if x.strip()}
            seen = True
    return wl if seen else set()


def set_topic_directive(key, value):
    ensure_topic_map_txt()
    try:
        with open(TOPIC_MAP_TXT, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception:
        lines = []
    kept = []
    for ln in lines:
        s  = ln.strip()
        cs = s.split("#", 1)[0].strip() if "#" in s else s
        if "=" in cs and cs.partition("=")[0].strip().lower() == key.lower():
            continue
        kept.append(ln)
    kept.append(f"{key} = {value}")
    with open(TOPIC_MAP_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(kept) + "\n")


def find_cmds_for_topic_title(title):
    if not title:
        return []
    t   = title.strip().lower()
    out = []
    for topic, cmd in load_topic_txt():
        if topic.strip().lower() == t:
            if cmd.lower() not in [c.lower() for c in out]:
                out.append(cmd)
    return out


def get_cmd_key(title: str, alias: str = "") -> str:
    if alias and alias.strip():
        return _strip_junk(alias.strip()).lower()
    parts = (title or "").strip().split()
    if len(parts) <= 1:
        return _strip_junk(parts[0]).lower() if parts else ""
    rest = parts[1:]
    while rest and _is_junk_word(rest[-1]):
        rest.pop()
    if not rest:
        return ""
    return _strip_junk(rest[-1]).lower()


def resolve_channels_by_cmd(cmd_key):
    cmd_key = (cmd_key or "").strip().lower().lstrip("/")
    if not cmd_key:
        return []
    groups = {}
    for ch in load_channels():
        alias = (ch.get("alias") or "").strip().lower()
        title = (ch.get("title") or "").strip()
        k     = get_cmd_key(title, alias)
        if k:
            groups.setdefault(k, []).append(ch)
    return groups.get(cmd_key, [])


def all_channel_cmds():
    groups = {}
    for ch in load_channels():
        alias = (ch.get("alias") or "").strip().lower()
        title = (ch.get("title") or "").strip()
        k     = get_cmd_key(title, alias)
        if k:
            groups.setdefault(k, []).append(ch)
    return sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))


def gen_topic_map_txt():
    existing    = load_topic_txt()
    mapped_cmds = {c.lower() for _, c in existing}
    groups      = all_channel_cmds()
    lines       = [TOPIC_MAP_TEMPLATE.rstrip("\n"), ""]
    if existing:
        lines.append("# ===== ĐÃ MAP =====")
        for topic, cmd in existing:
            lines.append(f"{topic} = {cmd}")
        lines.append("")
    lines.append("# ===== ĐIỀN TÊN TOPIC VÀO TRƯỚC DẤU = =====")
    n_new = 0
    for cmd, chs in groups:
        if cmd.lower() in mapped_cmds:
            continue
        titles = ", ".join((c.get("title") or "") for c in chs)
        lines.append(f" = {cmd}    # {titles}")
        n_new += 1
    with open(TOPIC_MAP_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return len(existing), n_new


# ─────────────────────────────────────────────────────────
# Round-robin
# ─────────────────────────────────────────────────────────

def load_topic_rr() -> dict:
    if os.path.exists(RR_FILE):
        try:
            with open(RR_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_topic_rr(data: dict):
    with open(RR_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def pick_next_rr(topic_title: str, cmds: list) -> str:
    if not cmds:
        return ""
    if len(cmds) == 1:
        return cmds[0]
    rr   = load_topic_rr()
    key  = (topic_title or "").strip().lower()
    prev = rr.get(key, -1)
    idx  = (prev + 1) % len(cmds)
    rr[key] = idx
    save_topic_rr(rr)
    return cmds[idx]


# ─────────────────────────────────────────────────────────
# Quick-tap /cmd builder
# ─────────────────────────────────────────────────────────

RESERVED_CMDS = {
    "add", "addf", "addchan", "addfolder",
    "botadd", "botaddf", "addbot", "addbotf",
    "list", "listchan",
    "del", "delchan",
    "alias", "aliaschan",
    "check", "checkchan", "checkads",
    "clean", "cleanchan",
    "skip", "next", "help", "start",
    "xdone", "zdone",
    "map", "unmap", "mapgen",
    "xepbai", "xepbaiwhite",
    "all",
    "done", "done1", "done2", "done3", "done4", "done5",
    "done6", "done7", "done8", "done9", "done10",
}

_CMD_OK = _re_cmd.compile(r"^[a-z0-9_]+$")


def _is_junk_word(w: str) -> bool:
    return not w or w.isdigit() or not any(c.isalnum() for c in w)


def _strip_junk(w: str) -> str:
    i, j = 0, len(w)
    while i < j and not w[i].isalnum():
        i += 1
    while j > i and not w[j-1].isalnum():
        j -= 1
    return w[i:j]


def build_channel_commands(channels):
    if not channels:
        return "  (Chưa có kênh — dùng /add <link/id> để thêm)", {}
    groups = {}
    for ch in channels:
        alias = (ch.get("alias") or "").strip().lower()
        title = (ch.get("title") or "").strip()
        key   = get_cmd_key(title, alias)
        if not key:
            continue
        groups.setdefault(key, []).append(ch)
    sorted_keys = sorted(groups.keys(), key=lambda k: (-len(groups[k]), k))
    lines   = []
    cmd_map = {}
    for key in sorted_keys:
        chs       = groups[key]
        titles    = ", ".join((ch.get("title") or "").strip() for ch in chs)
        clickable = bool(_CMD_OK.match(key)) and key not in RESERVED_CMDS
        if clickable:
            cmd_map[key] = chs
            lines.append(f"/{key}   ({titles})")
        else:
            lines.append(f"• {key}   ({titles})")
    return "\n\n".join(lines), cmd_map


# ─────────────────────────────────────────────────────────
# Folder persistence
# ─────────────────────────────────────────────────────────

def load_folders():
    if os.path.exists(FOLDERS_FILE):
        try:
            with open(FOLDERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_folders(folders):
    with open(FOLDERS_FILE, "w", encoding="utf-8") as f:
        json.dump(folders, f, ensure_ascii=False, indent=2)


def remember_folder(slug, title=""):
    folders = load_folders()
    for fd in folders:
        if fd.get("slug") == slug:
            if title and not fd.get("title"):
                fd["title"] = title
                save_folders(folders)
            return False
    folders.append({"slug": slug, "title": title or slug, "added_at": int(time.time())})
    save_folders(folders)
    return True


# ─────────────────────────────────────────────────────────
# Failed messages
# ─────────────────────────────────────────────────────────

def load_failed():
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_failed(items):
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def record_failed(target_id, target_title, items):
    if not items:
        return
    data = load_failed()
    data.append({
        "target_id":    target_id,
        "target_title": target_title,
        "items":        items,
        "ts":           int(time.time()),
    })
    save_failed(data)


# ─────────────────────────────────────────────────────────
# Ads loader
# ─────────────────────────────────────────────────────────

async def load_ads_into(slot):
    """
    Đọc danh sách ads — LUÔN qua user session (đủ lịch sử).
    Copy ads ra kênh: user session (giữ emoji premium).
    """
    ads      = []
    chat_id  = slot.get("ads_chat_id") or get_ads_chat_id()
    last_err = None
    uc       = await ensure_user_client()
    order    = (("user", uc), ("bot", app)) if uc else (("bot", app),)

    for label, client in order:
        if client is None:
            continue
        try:
            ads = []
            async for msg in client.get_chat_history(chat_id, limit=200):
                if not msg.empty and not msg.service:
                    ads.append(msg.id)
            ads.reverse()
            slot["ads_msgs"]     = ads
            slot["ads_chat_id"]  = chat_id
            slot["ads_bot_copy"] = False
            log("ADS", f"Load {len(ads)} ads qua {label} | gửi ra kênh: user (emoji premium)")
            return
        except Exception as e:
            last_err = e
            log("WARN", f"load_ads qua {label}: {type(e).__name__}: {e}")

    log("ERROR", f"load_ads thất bại: {last_err}")


async def load_ads(chat_id: int):
    await load_ads_into(active_slot(chat_id))


# ─────────────────────────────────────────────────────────
# Menu
# ─────────────────────────────────────────────────────────

def get_menu(n_content, chat_id: int | None = None):
    slot      = active_slot(chat_id) if chat_id else active_slot()
    ads_count = max(1, len(slot["ads_msgs"]))
    best      = max(1, round(n_content / ads_count))
    media_cnt = slot.get("total_media_count", 0)
    media_str = f" / {media_cnt} media" if media_cnt > 0 else ""
    lines     = [f"📥 Đã nhận {n_content} bài{media_str}\n━━━━━━━━━━━━━━━"]
    for i in range(1, 11):
        mark = " ✅" if i == best else ""
        lines.append(f"/done{i} — {i} content/ads{mark}")
    lines += [
        "━━━━━━━━━━━━━━━",
        f"➡️ Gợi ý: /done{best}",
        "━━━━━━━━━━━━━━━",
        "/xdone — nhét hết ads (có thể 2 ads liền)",
        "/zdone — ads trước, content sau",
    ]
    return "\n".join(lines)


async def update_menu(n, chat_id: int):
    await asyncio.sleep(2)
    slot = active_slot(chat_id)
    if not slot["waiting"]:
        return

    for _ in range(30):
        if slot.get("_album_pending", 0) <= 0:
            break
        await asyncio.sleep(0.5)

    if len(slot["content_msgs"]) != n or not slot["waiting"]:
        return

    if slot.get("all_mode"):
        log("ALL", f"/all → up {n} bài lên tất cả kênh")
        await do_all_forward(slot)
        return

    ev = slot.get("_topic_event")
    if ev is not None and not ev.is_set():
        log("TOPIC", "update_menu chờ topic detect...")
        try:
            await asyncio.wait_for(ev.wait(), timeout=15)
        except asyncio.TimeoutError:
            log("TOPIC", "update_menu: timeout topic detect — tiếp tục")

    if len(slot["content_msgs"]) != n or not slot["waiting"]:
        return

    mode         = get_xepbai_mode()
    mapped_cmds  = find_cmds_for_topic_title(slot.get("topic_title"))
    whitelist    = get_xepbai_whitelist()
    force_manual = any(c.lower() in whitelist for c in mapped_cmds)
    topic_auto   = bool(mapped_cmds) and not force_manual
    log("XEPBAI", f"mode={mode} topic={slot.get('topic_title')!r} mapped={mapped_cmds} "
                  f"force_manual={force_manual} topic_auto={topic_auto}")

    if (mode == "off" and not force_manual) or topic_auto:
        if not slot["ads_msgs"]:
            await load_ads_into(slot)
        ads_count = max(1, len(slot["ads_msgs"]))
        best      = max(1, round(n / ads_count))
        reason    = f"topic map ({slot.get('topic_title')!r})" if topic_auto else "xepbai=off"
        log("XEPBAI", f"{reason} → tự xếp /done{best}")
        await build_sequence(best, chat_id=chat_id)
        return

    text = get_menu(n, chat_id)
    if slot.get("menu_msg_id"):
        ok = await robust_edit(chat_id, slot["menu_msg_id"], text)
        if ok:
            return
        slot["menu_msg_id"] = None

    if len(slot["content_msgs"]) != n or not slot["waiting"]:
        return

    m = await robust_send(text, chat_id)
    if m is not None:
        slot["menu_msg_id"] = m.id
    else:
        log("ERROR", f"update_menu: không gửi được menu n={n}")


# ─────────────────────────────────────────────────────────
# Copy core — user session copy ra kênh (giữ emoji premium)
# Bot API chỉ giữ raw/emoji đầy đủ trong chat riêng, không ra kênh.
# ─────────────────────────────────────────────────────────

def _is_user_dm_chat(chat_id) -> bool:
    return isinstance(chat_id, int) and chat_id > 0


async def _copy_from_chat(client: Client, stored_from: int) -> int:
    """
    stored_from trong sequence = user_chat_id (DM, nhìn từ phía bot).
    User session copy content cần from_chat = bot id (cùng msg_id trong 1-1).
    """
    if _is_user_dm_chat(stored_from):
        if client is app:
            return stored_from
        return await get_bot_id()
    return stored_from


async def _import_peer(src_client: Client, dst_client: Client, chat_id) -> bool:
    """Copy access_hash peer từ session nguồn sang session đích."""
    if src_client is dst_client:
        return True
    try:
        peer = await src_client.resolve_peer(chat_id)
        await dst_client.storage.update_peers([peer])
        return True
    except Exception:
        return False


async def _warm_peer(client: Client, chat_id, username: str | None = None) -> bool:
    """Cache peer: get_chat / @username / resolve / import từ user session."""
    if chat_id is None:
        return False

    identifiers: list = []
    if isinstance(chat_id, str) and str(chat_id).startswith("@"):
        identifiers.append(chat_id)
    else:
        identifiers.append(chat_id)
        if username:
            u = username if username.startswith("@") else f"@{username}"
            identifiers.append(u)

    for ident in identifiers:
        try:
            await client.get_chat(ident)
            return True
        except Exception:
            pass

    try:
        await client.resolve_peer(chat_id)
        return True
    except Exception:
        pass

    if client is app:
        uc = await ensure_user_client()
        if uc and await _import_peer(uc, app, chat_id):
            try:
                await app.resolve_peer(chat_id)
                return True
            except Exception:
                pass
        if isinstance(chat_id, int) and chat_id < 0:
            try:
                me = await app.get_me()
                await app.get_chat_member(chat_id, me.id)
                return True
            except Exception:
                pass

    return False


async def _warm_copy_peers(client: Client, target_id, from_chat) -> None:
    ch    = _lookup_channel(target_id)
    uname = (ch or {}).get("username") or ""
    await _warm_peer(client, target_id, uname or None)
    src = await _copy_from_chat(client, from_chat)
    await _warm_peer(client, src)


async def _warm_channels_for_copy(channel_ids: list) -> None:
    await _resolve_bot_ads_chat()
    uc = await ensure_user_client()
    for cid in channel_ids:
        ch    = _lookup_channel(cid)
        uname = (ch or {}).get("username") or ""
        if uc:
            await _warm_peer(uc, cid, uname or None)
        await _warm_peer(app, cid, uname or None)
    if uc:
        await _warm_peer(uc, await get_bot_id())
        ads_id = get_ads_chat_id()
        if ads_id:
            await _warm_peer(uc, ads_id)


async def _warm_all_saved_channels() -> None:
    channels = load_channels()
    if not channels:
        return
    log("START", f"Warm peer {len(channels)} kênh (user copy)...")
    await _warm_channels_for_copy([ch["id"] for ch in channels])


async def _clients_for_copy(from_chat) -> list:
    """Copy ra kênh: luôn ưu tiên user session (giữ emoji premium)."""
    uc = await ensure_user_client()
    if uc:
        return [uc]
    log("WARN", "Không có user session — fallback bot (emoji premium kênh có thể mất)")
    return [app]


async def _try_copy_one(target_id, from_chat, msg_id, is_album: bool,
                        bucket: TokenBucket, last_ts: list):
    clients_try = await _clients_for_copy(from_chat)
    ch          = _lookup_channel(target_id)
    uname       = (ch or {}).get("username") or ""
    await _warm_peer(app, target_id, uname or None)

    n_items = 1
    if is_album:
        reader = await _client_for_chat(from_chat)
        try:
            mg = await reader.get_media_group(from_chat, msg_id)
            if mg:
                n_items = len(mg)
        except Exception:
            pass

    async def _do_copy(c: Client):
        src = await _copy_from_chat(c, from_chat)
        if is_album:
            copied = await c.copy_media_group(target_id, src, msg_id)
            return len(copied) if copied else n_items
        await c.copy_message(target_id, src, msg_id)
        return 1

    last_err = None

    while True:
        elapsed = time.monotonic() - last_ts[0]
        gap     = FWD_COPY_MIN_DELAY + random.uniform(0, 0.3)
        if elapsed < gap:
            await asyncio.sleep(gap - elapsed)

        await bucket.acquire(1)
        last_ts[0] = time.monotonic()

        while True:
            remain = _flood_until - time.monotonic()
            if remain <= 0:
                break
            log("FLOOD", f"Chờ flood gate {remain:.0f}s — copy")
            await asyncio.sleep(remain)

        for peer_attempt in range(COPY_PEER_RETRIES):
            for c in clients_try:
                await _warm_copy_peers(c, target_id, from_chat)
                tag = "bot" if c is app else "user"
                try:
                    n = await _do_copy(c)
                    if c is not app:
                        kind = "content" if _is_user_dm_chat(from_chat) else "ads"
                        log("COPY", f"  {kind} via user src={await _copy_from_chat(c, from_chat)}")
                    return n, None
                except FloodWait as e:
                    wait = e.value + 3
                    log("FLOOD", f"FloodWait {wait}s — copy → global wait")
                    await flood_wait_globally(wait, source="copy")
                    last_err = e
                    break
                except DEAD_CHANNEL_ERRORS as e:
                    err = f"{type(e).__name__}: {str(e)[:80]}"
                    log("DEAD", f"Dead channel khi copy: {err}")
                    return 0, err
                except SKIP_NOT_DEAD_ERRORS as e:
                    log("SKIP", f"Skip channel ({type(e).__name__}) khi copy")
                    return 0, f"SKIP:{type(e).__name__}"
                except PEER_RETRY_ERRORS as e:
                    last_err = e
                    log("WARN", f"PeerIdInvalid {from_chat}/{msg_id} → warm retry {peer_attempt+1}/{COPY_PEER_RETRIES}")
                except Exception as e:
                    last_err = e
                    log("WARN", f"copy {from_chat}/{msg_id} ({tag}): {type(e).__name__}: {e}")

            if isinstance(last_err, FloodWait):
                break
            if peer_attempt < COPY_PEER_RETRIES - 1:
                await asyncio.sleep(1.0 + peer_attempt)

        if isinstance(last_err, FloodWait):
            continue

        log("ERROR", f"copy fail {from_chat}/{msg_id}: {last_err}")
        return 0, None


async def copy_sequence_to_channel(target_id, sequence):
    """
    Copy sequence tới target_id — user session copy_message (giữ emoji premium).
    Content: user copy từ chat bot. Ads: user copy từ nhóm ads.
    """
    await _resolve_bot_ads_chat()
    ch    = _lookup_channel(target_id)
    uname = (ch or {}).get("username") or ""
    await _warm_peer(app, target_id, uname or None)

    bucket  = _global_bucket or TokenBucket(FWD_GLOBAL_RATE, FWD_GLOBAL_BURST)
    last_ts = [0.0]

    expanded     = []
    seen_groups  = set()
    failed_items = []
    dead_reason  = None

    for seq_item in sequence:
        src_chat, msg_id = seq_item
        reader   = await _client_for_chat(src_chat)
        last_err   = None
        item_done  = False

        for attempt in range(FWD_MAX_RETRY):
            while True:
                remain = _flood_until - time.monotonic()
                if remain <= 0:
                    break
                await asyncio.sleep(remain)

            try:
                msg = await reader.get_messages(src_chat, msg_id)

                if msg.empty:
                    if attempt < FWD_MAX_RETRY - 1:
                        log("WARN", f"msg empty retry {attempt+1}: {src_chat}/{msg_id}")
                        await asyncio.sleep(3)
                        last_err = Exception("msg.empty")
                        continue
                    log("WARN", f"msg empty sau max retry → skip: {src_chat}/{msg_id}")
                    item_done = True
                    break

                if msg.media_group_id:
                    key = (src_chat, msg.media_group_id)
                    if key in seen_groups:
                        item_done = True
                        break
                    seen_groups.add(key)
                    album = await reader.get_media_group(src_chat, msg_id)
                    if album:
                        expanded.append((src_chat, album[0].id, True, seq_item, len(album)))
                    item_done = True
                    break
                else:
                    expanded.append((src_chat, msg_id, False, seq_item, 1))
                    item_done = True
                    break

            except FloodWait as e:
                wait = e.value + 3
                await flood_wait_globally(wait, source=f"expand ch={target_id}")
                last_err = e

            except DEAD_CHANNEL_ERRORS as dead_exc:
                dead_reason    = f"{type(dead_exc).__name__}: {str(dead_exc)[:80]}"
                already_expanded = {ex[3] for ex in expanded}
                remaining = [s for s in sequence if s not in already_expanded and s != seq_item]
                return 0, failed_items + [seq_item] + remaining, dead_reason

            except PEER_RETRY_ERRORS as e:
                last_err = e
                backoff  = 1.5 * (attempt + 1)
                log("WARN", f"expand peer retry {src_chat}/{msg_id}: {e} — {backoff:.1f}s")
                await asyncio.sleep(backoff)
                uc = await ensure_user_client()
                if uc:
                    await _import_peer(uc, reader, src_chat)
                await _warm_peer(reader, src_chat)

            except SKIP_NOT_DEAD_ERRORS:
                return 0, [], None

            except Exception as e:
                last_err = e
                backoff  = 2 * (attempt + 1) + random.uniform(0, 1.5)
                log("WARN", f"expand {src_chat}/{msg_id} attempt {attempt+1}: {e} — retry {backoff:.1f}s")
                await asyncio.sleep(backoff)

        if not item_done:
            failed_items.append(seq_item)
            log("ERROR", f"Không expand {src_chat}/{msg_id}: {last_err}")

    sent_count = 0
    for idx, (src_chat, mid, is_album, orig, _n) in enumerate(expanded):
        count, err = await _try_copy_one(target_id, src_chat, mid, is_album, bucket, last_ts)

        if err:
            is_dead = not err.startswith("SKIP:")
            dead_reason = err if is_dead else None
            leftover = [ex[3] for ex in expanded[idx:]]
            return sent_count, failed_items + leftover, dead_reason

        if count == 0:
            failed_items.append(orig)
        else:
            sent_count += count
            log("COPY", f"  ok×{count} src={src_chat} → ch={target_id}")

    if failed_items:
        log("COPY", f"⚠️ ch={target_id}: {len(failed_items)} items fail")

    return sent_count, failed_items, dead_reason


# ─────────────────────────────────────────────────────────
# Build sequence
# ─────────────────────────────────────────────────────────

async def build_sequence(content_per_ads=1, mode="normal", chat_id: int | None = None):
    slot         = active_slot(chat_id) if chat_id else active_slot()
    chat_id      = slot.get("user_chat_id") or chat_id
    contents     = slot["content_msgs"]
    n            = len(contents)
    ads_chat     = slot["ads_chat_id"] or get_ads_chat_id()
    content_chat = chat_id
    media_cnt    = slot.get("total_media_count", 0)
    media_str    = f"{n} bài / {media_cnt} media" if media_cnt else f"{n} bài"

    log("BUILD", f"mode={mode} n={n} media={media_cnt} ads={len(slot['ads_msgs'])} cpa={content_per_ads}")

    if not slot["ads_msgs"]:
        log("BUILD", "Không có ads — copy content không xen ads")
        sequence = [(content_chat, mid) for mid in contents]
        slot["final_sequence"]   = sequence
        slot["awaiting_channel"] = True
        slot["waiting"]          = False
        channels            = load_channels()
        chan_lines, cmd_map = build_channel_commands(channels)
        slot["channel_commands"] = cmd_map
        await safe_send(
            f"⚠️ Không có ads — copy {media_str} không xen ads.\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📡 Tap lệnh để gửi:\n{chan_lines}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"/skip — bỏ qua",
            chat_id,
        )
        return

    ads_queue = list(slot["ads_msgs"][slot["ads_index"]:])
    sequence  = []
    ads_used  = 0

    def take_ads(count):
        nonlocal ads_used
        taken = []
        for _ in range(count):
            if not ads_queue:
                break
            aid = ads_queue.pop(0)
            slot["ads_index"] += 1
            taken.append((ads_chat, aid))
            ads_used += 1
        return taken

    if mode == "normal":
        total_ads = len(ads_queue)
        if total_ads == 0 or n <= 1:
            for mid in contents:
                sequence.append((content_chat, mid))
        else:
            groups      = total_ads + 1
            base, extra = divmod(n, groups)
            sizes       = [base + (1 if g < extra else 0) for g in range(groups)]
            idx         = 0
            for g, size in enumerate(sizes):
                for _ in range(size):
                    sequence.append((content_chat, contents[idx]))
                    idx += 1
                if g < groups - 1:
                    sequence.extend(take_ads(1))

    elif mode == "xdone":
        slots_between = n - 1
        if slots_between <= 0:
            for mid in contents:
                sequence.append((content_chat, mid))
        else:
            total       = len(ads_queue)
            base, extra = divmod(total, slots_between)
            for i, mid in enumerate(contents):
                sequence.append((content_chat, mid))
                if i < n - 1:
                    sequence.extend(take_ads(base + (1 if i < extra else 0)))

    elif mode == "zdone":
        total       = len(ads_queue)
        base, extra = divmod(total, n)
        for i, mid in enumerate(contents):
            sequence.extend(take_ads(base + (1 if i < extra else 0)))
            sequence.append((content_chat, mid))

    log("BUILD", f"Xếp xong len(seq)={len(sequence)} ads_used={ads_used}")

    if not sequence:
        await safe_send("⚠️ Sequence rỗng.", chat_id)
        return

    slot["final_sequence"]   = sequence
    slot["awaiting_channel"] = True
    slot["waiting"]          = False

    mapped_cmds = find_cmds_for_topic_title(slot.get("topic_title"))

    if len(mapped_cmds) >= 2:
        picked_cmd = pick_next_rr(slot.get("topic_title", ""), mapped_cmds)
        rr_state   = load_topic_rr()
        rr_key     = (slot.get("topic_title") or "").strip().lower()
        rr_idx     = rr_state.get(rr_key, 0)
        rr_display = f"{rr_idx + 1}/{len(mapped_cmds)}"

        results = resolve_channels_by_cmd(picked_cmd)
        if results:
            names = ", ".join(ch["title"] for ch in results)
            await safe_send(
                f"🔄 Topic '{slot.get('topic_title')}' (lượt {rr_display}) → /{picked_cmd}\n"
                f"✅ Xếp {media_str} + {ads_used} ads → tự gửi tới {len(results)} kênh: {names}",
                chat_id,
            )
            await _start_forward(slot, results, f"/{picked_cmd}")
            return
        for fallback_cmd in mapped_cmds:
            if fallback_cmd == picked_cmd:
                continue
            results = resolve_channels_by_cmd(fallback_cmd)
            if results:
                names = ", ".join(ch["title"] for ch in results)
                await safe_send(
                    f"⚠️ /{picked_cmd} không có kênh → fallback /{fallback_cmd}\n"
                    f"✅ Xếp {media_str} + {ads_used} ads → tự gửi tới {len(results)} kênh: {names}",
                    chat_id,
                )
                await _start_forward(slot, results, f"/{fallback_cmd}")
                return
        cmd_map = {c: resolve_channels_by_cmd(c) for c in mapped_cmds if resolve_channels_by_cmd(c)}
        slot["channel_commands"] = cmd_map
        opts = "\n\n".join(f"/{c}" for c in mapped_cmds)
        await safe_send(
            f"⚠️ Topic '{slot.get('topic_title')}' — tất cả kênh đều không resolve được.\n"
            f"✅ Đã xếp {media_str} + {ads_used} ads. Vui lòng chọn tay:\n\n{opts}\n\n/skip — bỏ qua",
            chat_id,
        )
        return

    if len(mapped_cmds) == 1:
        mapped_cmd = mapped_cmds[0]
        results    = resolve_channels_by_cmd(mapped_cmd)
        if results:
            names = ", ".join(ch["title"] for ch in results)
            await safe_send(
                f"🎯 Topic '{slot.get('topic_title')}' → /{mapped_cmd}\n"
                f"✅ Xếp {media_str} + {ads_used} ads → tự gửi tới {len(results)} kênh: {names}",
                chat_id,
            )
            await _start_forward(slot, results, f"/{mapped_cmd}")
            return
        await safe_send(
            f"⚠️ Topic '{slot.get('topic_title')}' map tới /{mapped_cmd} "
            f"nhưng không có kênh nào khớp (xem /list). Chọn tay:",
            chat_id,
        )

    channels            = load_channels()
    chan_lines, cmd_map = build_channel_commands(channels)
    slot["channel_commands"] = cmd_map

    hint = ""
    if slot.get("topic_title") and not mapped_cmds:
        hint = (f"\n━━━━━━━━━━━━━━━\n"
                f"ℹ️ Topic: '{slot.get('topic_title')}' — chưa map.\n"
                f"Mở {TOPIC_MAP_TXT}, thêm: {slot.get('topic_title')} = <tenkenh>")

    await safe_send(
        f"✅ Xếp xong: {media_str} + {ads_used} ads\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📡 Tap lệnh để gửi:\n{chan_lines}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"/skip — bỏ qua"
        f"{hint}",
        chat_id,
    )


# ─────────────────────────────────────────────────────────
# /all mode
# ─────────────────────────────────────────────────────────

async def do_all_forward(slot):
    chat_id = slot["user_chat_id"]
    seq = [(chat_id, mid) for mid in slot["content_msgs"]]
    slot["all_mode"] = False
    if not seq:
        return
    channels = load_channels()
    if not channels:
        await safe_send("⚠️ /all: chưa có kênh nào.", chat_id)
        reset_slot(slot)
        return
    slot["final_sequence"]   = seq
    slot["awaiting_channel"] = True
    slot["waiting"]          = False
    media_cnt = slot.get("total_media_count", 0)
    media_str = f"{len(seq)} bài / {media_cnt} media" if media_cnt else f"{len(seq)} bài"
    await safe_send(f"📦 /all: up {media_str} → TẤT CẢ {len(channels)} kênh (không ads).", chat_id)
    await _start_forward(slot, channels, "/all")


# ─────────────────────────────────────────────────────────
# Forward job (copy đa kênh)
# ─────────────────────────────────────────────────────────

async def do_forward_job(slot, results):
    sequence    = slot["final_sequence"]
    chat_id     = slot["user_chat_id"]
    content_n   = len(slot["content_msgs"])
    content_med = slot.get("total_media_count", 0)

    ok_ids   = set()
    err_ids  = set()
    dead_ids = set()

    dead_killed  = []
    fail_recap   = []
    ch_media_ok: dict = {}
    sem = asyncio.Semaphore(FWD_MAX_CONCURRENT_CHANNELS)

    async def _copy_one(ch):
        ch_id    = ch["id"]
        ch_label = f"{ch['title']} (id={ch_id})"

        async with sem:
            try:
                seq_copy = copy.copy(sequence)
                sent, failed_items, dead_reason = await copy_sequence_to_channel(ch_id, seq_copy)

                if dead_reason:
                    dead_ids.add(ch_id)
                    removed = await remove_dead_channel(ch_id)
                    if removed:
                        dead_killed.append(removed)
                    if failed_items:
                        record_failed(ch_id, ch["title"], failed_items)
                        fail_recap.append((ch["title"], len(failed_items)))
                    log("DEAD", f"💀 {ch_label} — CHẾT | {dead_reason}")
                else:
                    ok_ids.add(ch_id)
                    ch_media_ok[ch_id] = sent
                    log("COPY", f"✓ {ch_label} — OK {sent} media, fail={len(failed_items)}")
                    if failed_items:
                        record_failed(ch_id, ch["title"], failed_items)
                        fail_recap.append((ch["title"], len(failed_items)))

                await asyncio.sleep(FWD_BETWEEN_CHANNELS_SEC + random.uniform(0, 1.0))

            except Exception as e:
                err_ids.add(ch_id)
                log("ERROR", f"copy to {ch_label}: {e}\n{traceback.format_exc()}")

    async with fwd_lock:
        await asyncio.gather(*[_copy_one(ch) for ch in results])

    total_ok_media = max(ch_media_ok.values()) if ch_media_ok else 0
    media_recv_str = f"{content_n} bài / {content_med} media" if content_med else f"{content_n} bài"

    lines = [
        f"✅ Xong! Copy → {len(results)} kênh",
        f"  📥 Nhận: {media_recv_str}",
        f"  📤 Gửi: {total_ok_media} media/kênh",
    ]
    if ok_ids:
        ok_names = [ch["title"] for ch in results if ch["id"] in ok_ids]
        lines.append(f"  ✓ OK ({len(ok_ids)}): {', '.join(ok_names)}")
    if dead_killed:
        lines.append(f"  💀 Kênh chết (đã xóa): {', '.join(dead_killed)}")
    if err_ids:
        err_names = [ch["title"] for ch in results if ch["id"] in err_ids]
        lines.append(f"  ❌ Lỗi: {', '.join(err_names)}")
    if fail_recap:
        detail = ", ".join(f"{t}({n} bài)" for t, n in fail_recap)
        lines.append(f"  ⚠️ Bài lưu retry: {detail}")

    try:
        await safe_send("\n".join(lines), chat_id)
    finally:
        reset_slot(slot)
        if slot in state["slots"]:
            state["slots"].remove(slot)
        if not state["slots"]:
            new_s = make_slot()
            new_s["user_chat_id"] = chat_id
            state["slots"].append(new_s)
            asyncio.ensure_future(load_ads_into(new_s))
        if not any(s["awaiting_channel"] for s in state["slots"]):
            await safe_send("✨ Sẵn sàng! Forward bài mới vào bot.", chat_id)


async def _start_forward(slot, results, query_display: str = ""):
    chat_id = slot["user_chat_id"]
    if not results:
        await safe_send(f"❌ Không tìm thấy kênh khớp '{query_display}'.\nDùng /list để xem hoặc gõ lại.", chat_id)
        return
    if not slot or not slot["final_sequence"]:
        await safe_send("⚠️ Không có batch nào đang chờ gửi.", chat_id)
        return
    names = ", ".join(ch["title"] for ch in results)
    if len(names) > 300:
        names = names[:300] + f"… (+{len(results)} kênh)"
    content_n   = len(slot["content_msgs"])
    content_med = slot.get("total_media_count", 0)
    media_str   = f"{content_n} bài / {content_med} media" if content_med else f"{content_n} bài"
    slot["awaiting_channel"] = False
    slot_info = f" | slot #{len(state['slots'])}" if len(state["slots"]) > 1 else ""
    await safe_send(
        f"📡 Copy → {len(results)} kênh: {names}\n"
        f"📦 {media_str} — {len(slot['final_sequence'])} seq items{slot_info}\n"
        f"▶️ Chạy nền — bạn có thể forward bài mới ngay!",
        chat_id,
    )
    new_s = make_slot()
    new_s["user_chat_id"] = chat_id
    state["slots"].append(new_s)
    await load_ads_into(new_s)
    await _warm_channels_for_copy([ch["id"] for ch in results])
    asyncio.ensure_future(do_forward_job(slot, results))


async def cmd_select_channel(query: str, chat_id: int):
    slot    = waiting_slot(chat_id)
    results = find_channels(query)
    await _start_forward(slot, results, query)


async def cmd_select_by_cmd(slot, cmd_key: str):
    cmd_map = slot.get("channel_commands") or {}
    results = cmd_map.get(cmd_key, [])
    await _start_forward(slot, results, f"/{cmd_key}")


# ─────────────────────────────────────────────────────────
# Folder sync
# ─────────────────────────────────────────────────────────

async def _fetch_folder_chats(slug):
    uc = await ensure_user_client()
    if not uc:
        raise RuntimeError(
            "Cần user session (test_session.session) để đọc folder — "
            "bot không gọi được CheckChatlistInvite"
        )
    from pyrogram.raw import functions as raw_fn
    result = await uc.invoke(raw_fn.chatlists.CheckChatlistInvite(slug=slug))
    return getattr(result, "title", slug) or slug, getattr(result, "chats", [])


def _raw_chat_to_channel(chat):
    title    = getattr(chat, "title", "") or ""
    username = getattr(chat, "username", "") or ""
    raw_id   = getattr(chat, "id", None)
    if not raw_id or not title:
        return None
    tg_id = int(f"-100{raw_id}") if raw_id > 0 else raw_id
    return {"id": tg_id, "title": title, "username": username, "alias": ""}


def _channels_from_folder_chats(chats):
    out = []
    for chat in chats:
        ch = _raw_chat_to_channel(chat)
        if ch:
            out.append(ch)
    return out


def _parse_channel_indices(arg: str, total: int) -> list[int]:
    if not arg.strip():
        return list(range(total))
    indices: set[int] = set()
    for part in arg.replace(",", " ").split():
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            try:
                start = int(left)
                end   = int(right)
            except ValueError:
                continue
            if start > end:
                start, end = end, start
            for i in range(start, end + 1):
                indices.add(i - 1)
        else:
            try:
                indices.add(int(part) - 1)
            except ValueError:
                continue
    return sorted(i for i in indices if 0 <= i < total)


# ─────────────────────────────────────────────────────────
# User session — mời bot vào kênh + cấp admin (bot không tự add được)
# ─────────────────────────────────────────────────────────

def _make_bot_post_privileges() -> ChatPrivileges:
    """Tạo ChatPrivileges tương thích mọi phiên bản Pyrogram."""
    desired = {
        "can_manage_chat": True,
        "can_post_messages": True,
        "can_edit_messages": True,
        "can_delete_messages": True,
        "can_invite_users": False,
        "can_promote_members": False,
        "can_change_info": False,
        "can_pin_messages": False,
        "can_manage_video_chats": False,
        "can_restrict_members": False,
        "can_manage_topics": False,
        "is_anonymous": False,
    }
    supported = set(inspect.signature(ChatPrivileges.__init__).parameters) - {"self"}
    return ChatPrivileges(**{k: v for k, v in desired.items() if k in supported})


BOT_POST_PRIVILEGES = _make_bot_post_privileges()


async def ensure_user_client() -> "Client | None":
    global user_app
    if user_app is not None and user_app.is_connected:
        return user_app
    try:
        if SESSION_STRING:
            user_app = Client(
                USER_SESSION or "test_session",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=SESSION_STRING,
            )
        else:
            session_name = _resolve_user_session_name()
            if not session_name:
                return None
            _repair_session_db(session_name)
            user_app = Client(session_name, api_id=API_ID, api_hash=API_HASH)
        await user_app.start()
        me = await user_app.get_me()
        sess_tag = "string" if SESSION_STRING else (_resolve_user_session_name() or USER_SESSION)
        log("USER", f"User session ✓ {sess_tag} id={me.id}")
        return user_app
    except Exception as e:
        log("ERROR", f"ensure_user_client: {type(e).__name__}: {e}")
        if "no such column" in str(e).lower():
            log("ERROR", "Session cũ lỗi schema — thử xóa file .session và login lại, "
                         "hoặc pip install pyrogram==2.0.106 giống tool cũ")
        user_app = None
        return None


async def sync_allowed_from_user():
    global ALLOWED_USER_IDS
    if ALLOWED_USER_IDS:
        return
    uc = await ensure_user_client()
    if not uc:
        return
    me = await uc.get_me()
    ALLOWED_USER_IDS = {me.id}
    register_notify_user(me.id)
    log("CONFIG", f"ALLOWED_USER_IDS tự động = {me.id} (từ {USER_SESSION})")


def _bot_member_ok(member) -> bool:
    if not member:
        return False
    status = member.status
    if status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
        return False
    if status == ChatMemberStatus.ADMINISTRATOR:
        priv = member.privileges
        if priv and getattr(priv, "can_post_messages", None) is False:
            return False
        return True
    if status == ChatMemberStatus.OWNER:
        return True
    return False


async def _is_broadcast_channel(uc: Client, ch_id) -> bool:
    try:
        chat = await uc.get_chat(ch_id)
        return chat.type == ChatType.CHANNEL
    except Exception:
        return True


async def _raw_edit_bot_admin(uc: Client, ch_id, bot_id) -> str | None:
    """Fallback raw EditAdmin — promote bot làm admin kênh broadcast."""
    try:
        from pyrogram.raw import functions, types
        channel = await uc.resolve_peer(ch_id)
        user    = await uc.resolve_peer(bot_id)
        rights  = types.ChatAdminRights(
            change_info=False,
            post_messages=True,
            edit_messages=True,
            delete_messages=True,
            ban_users=False,
            invite_users=False,
            pin_messages=False,
            add_admins=False,
            anonymous=False,
            manage_call=False,
            other=False,
        )
        await uc.invoke(functions.channels.EditAdmin(
            channel=channel,
            user_id=user,
            admin_rights=rights,
            rank="",
        ))
        return None
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        return await _raw_edit_bot_admin(uc, ch_id, bot_id)
    except Exception as e:
        return f"raw admin: {type(e).__name__}: {str(e)[:100]}"


async def _install_bot_as_admin(uc: Client, ch_id, bot_id, bot_target) -> str | None:
    """
    Thêm bot làm admin kênh.
    Kênh broadcast: bot CHỈ được là admin — không dùng add_chat_members.
  Supergroup: có thể add member rồi promote.
    """
    is_channel = await _is_broadcast_channel(uc, ch_id)

    if is_channel:
        for target in (bot_target, bot_id):
            err = await _promote_bot_in_channel(uc, ch_id, target)
            if not err:
                return None
        return await _raw_edit_bot_admin(uc, ch_id, bot_id)

    err = await _promote_bot_in_channel(uc, ch_id, bot_id)
    if not err:
        return None

    invite_err = await _invite_bot_to_channel(uc, ch_id, bot_target)
    if invite_err and "USER_BOT" not in str(invite_err).upper():
        return invite_err

    return await _promote_bot_in_channel(uc, ch_id, bot_id)


async def _invite_bot_to_channel(uc: Client, ch_id, bot_target) -> str | None:
    try:
        await uc.add_chat_members(ch_id, bot_target)
        return None
    except UserAlreadyParticipant:
        return None
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        try:
            await uc.add_chat_members(ch_id, bot_target)
            return None
        except UserAlreadyParticipant:
            return None
        except Exception as e2:
            return f"mời bot: {type(e2).__name__}: {str(e2)[:80]}"
    except Exception as e:
        return f"mời bot: {type(e).__name__}: {str(e)[:80]}"


async def _promote_bot_in_channel(uc: Client, ch_id, bot_id) -> str | None:
    try:
        await uc.promote_chat_member(ch_id, bot_id, privileges=BOT_POST_PRIVILEGES)
        return None
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        try:
            await uc.promote_chat_member(ch_id, bot_id, privileges=BOT_POST_PRIVILEGES)
            return None
        except Exception as e2:
            return f"cấp admin: {type(e2).__name__}: {str(e2)[:80]}"
    except Exception as e:
        return f"cấp admin: {type(e).__name__}: {str(e)[:80]}"


async def cmd_botadd_channels(
    channel_list: list,
    reply_chat_id: int,
    *,
    merge_into_json: bool = False,
    title_prefix: str = "",
):
    if not channel_list:
        await safe_send("⚠️ Không có kênh nào để xử lý.", reply_chat_id)
        return

    uc = await ensure_user_client()
    if not uc:
        await safe_send(
            "❌ Cần **user session** để thêm bot vào kênh.\n"
            "━━━━━━━━━━━━━━━\n"
            "Bot không thể tự join kênh — tài khoản user (admin kênh) mời bot.\n\n"
            "Cấu hình 1 trong 2:\n"
            f"  • File `{USER_SESSION}.session` (login Pyrogram 1 lần)\n"
            "  • `SESSION_STRING` trong .env\n\n"
            "User phải là admin kênh với quyền:\n"
            "  thêm member + cấp admin (đăng bài).",
            reply_chat_id,
        )
        return

    me_bot     = await app.get_me()
    bot_id     = me_bot.id
    bot_target = me_bot.username or bot_id

    ok, already, failed = [], [], []
    status_msg = await robust_send(
        f"🤖 {title_prefix}Đang add @{me_bot.username or bot_id} vào {len(channel_list)} kênh... (0/{len(channel_list)})",
        reply_chat_id,
    )

    if merge_into_json:
        stored = load_channels()
    else:
        stored = None

    for i, ch in enumerate(channel_list):
        ch_id = ch["id"]
        title = ch.get("title") or str(ch_id)

        try:
            member = await app.get_chat_member(ch_id, bot_id)
            if _bot_member_ok(member):
                already.append(title)
                if merge_into_json and stored is not None:
                    if not any(str(c["id"]) == str(ch_id) for c in stored):
                        stored.append(ch)
                continue
        except Exception:
            pass

        err = await _install_bot_as_admin(uc, ch_id, bot_id, bot_target)
        if err:
            failed.append((title, err))
            await asyncio.sleep(0.4)
            continue

        try:
            member = await app.get_chat_member(ch_id, bot_id)
            if _bot_member_ok(member):
                ok.append(title)
                if merge_into_json and stored is not None:
                    if not any(str(c["id"]) == str(ch_id) for c in stored):
                        stored.append(ch)
            else:
                failed.append((title, "bot chưa có quyền đăng bài sau promote"))
        except Exception as e:
            ok.append(title)
            if merge_into_json and stored is not None:
                if not any(str(c["id"]) == str(ch_id) for c in stored):
                    stored.append(ch)
            log("WARN", f"verify {title}: {e}")

        await asyncio.sleep(0.5)

        if status_msg and ((i + 1) % 3 == 0 or i == len(channel_list) - 1):
            await robust_edit(
                reply_chat_id,
                status_msg.id,
                f"🤖 {title_prefix}Add bot... ({i+1}/{len(channel_list)})\n"
                f"✅ mới {len(ok)} | ✓ sẵn {len(already)} | ❌ {len(failed)}",
            )

    if merge_into_json and stored is not None:
        save_channels(stored)

    lines = [
        f"🤖 {title_prefix}Kết quả add bot ({len(channel_list)} kênh):",
        "━━━━━━━━━━━━━━━",
        f"✅ Mới add + admin: {len(ok)}",
        f"✓ Đã có sẵn: {len(already)}",
        f"❌ Lỗi: {len(failed)}",
    ]
    if ok:
        lines.append("━━━━━━━━━━━━━━━")
        lines.extend(f"  ✅ {t}" for t in ok[:20])
        if len(ok) > 20:
            lines.append(f"  ... +{len(ok) - 20} kênh")
    if failed:
        lines.append("━━━━━━━━━━━━━━━")
        for t, err in failed[:15]:
            lines.append(f"  ❌ {t}")
            lines.append(f"     └ {err}")
        if len(failed) > 15:
            lines.append(f"  ... +{len(failed) - 15} lỗi")
    if merge_into_json:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("📋 Kênh OK đã merge vào channels.json")

    final = "\n".join(lines)
    if status_msg:
        if not await robust_edit(reply_chat_id, status_msg.id, final):
            await robust_send(final, reply_chat_id)
    else:
        await robust_send(final, reply_chat_id)


async def cmd_botadd(reply_chat_id: int, indices_arg: str = ""):
    channels = load_channels()
    if not channels:
        await safe_send("📭 Chưa có kênh. Dùng /addf hoặc /botaddf trước.", reply_chat_id)
        return
    idxs = _parse_channel_indices(indices_arg, len(channels))
    if not idxs:
        await safe_send("❌ Chỉ số kênh không hợp lệ. VD: /botadd 1 3  hoặc /botadd 1-10", reply_chat_id)
        return
    targets = [channels[i] for i in idxs]
    await cmd_botadd_channels(targets, reply_chat_id)


async def cmd_botaddfolder(link: str, reply_chat_id: int):
    import re as _re
    match = _re.search(r"addlist/([A-Za-z0-9_+=-]+)", link.strip())
    if not match:
        await safe_send("❌ Link folder không hợp lệ.\nĐịnh dạng: https://t.me/addlist/xxxxx", reply_chat_id)
        return
    slug = match.group(1)
    await safe_send("⏳ Đang đọc folder + add bot...", reply_chat_id)
    try:
        folder_title, chats = await _fetch_folder_chats(slug)
    except Exception as e:
        await safe_send(f"❌ Không đọc được folder: {e}", reply_chat_id)
        return
    channel_list = _channels_from_folder_chats(chats)
    if not channel_list:
        await safe_send("⚠️ Folder trống hoặc không có kênh.", reply_chat_id)
        remember_folder(slug, folder_title)
        return
    remember_folder(slug, folder_title)
    await cmd_botadd_channels(
        channel_list,
        reply_chat_id,
        merge_into_json=True,
        title_prefix=f"[{folder_title}] ",
    )


async def cmd_addfolder(link: str, reply_chat_id: int, silent: bool = False, remember: bool = True):
    import re as _re
    match = _re.search(r"addlist/([A-Za-z0-9_+=-]+)", link.strip())
    if not match:
        if not silent:
            await safe_send("❌ Link folder không hợp lệ.\nĐịnh dạng: https://t.me/addlist/xxxxx", reply_chat_id)
        return 0
    slug = match.group(1)
    if not silent:
        await safe_send("⏳ Đang đọc folder...", reply_chat_id)
    try:
        folder_title, chats = await _fetch_folder_chats(slug)
    except Exception as e:
        if not silent:
            await safe_send(f"❌ Không đọc được folder: {e}", reply_chat_id)
        log("ERROR", f"addfolder({slug}): {e}")
        return 0
    if not chats:
        if not silent:
            await safe_send("⚠️ Folder trống.", reply_chat_id)
        if remember:
            remember_folder(slug, folder_title)
        return 0
    channels       = load_channels()
    added, skipped = [], []
    for ch in _channels_from_folder_chats(chats):
        title = ch["title"]
        if any(str(c["id"]) == str(ch["id"]) for c in channels):
            skipped.append(title)
            continue
        channels.append(ch)
        added.append(
            f"✅ #{len(channels)}. {title}"
            + (f" (@{ch['username']})" if ch.get("username") else "")
        )
    save_channels(channels)
    if remember:
        remember_folder(slug, folder_title)
    if not silent:
        out = [f"📁 Folder: {folder_title}"]
        if added:
            out.append(f"✅ Thêm {len(added)} kênh:")
            out.extend(added)
        if skipped:
            out.append(f"⚠️ Bỏ qua {len(skipped)} (đã có): {', '.join(skipped)}")
        if added:
            out.append("💡 /alias <số> <tên> để đặt tên tắt")
        out.append("🔄 Folder đã lưu — kênh mới sẽ tự sync mỗi 1h")
        await safe_send("\n".join(out), reply_chat_id)
    else:
        if added:
            log("FOLDER-SYNC", f"+{len(added)} kênh mới từ folder '{folder_title}'")
    return len(added)


async def cmd_addchan(raw: str, reply_chat_id: int):
    lines_raw    = [l.strip() for l in raw.splitlines() if l.strip()]
    if not lines_raw:
        await safe_send("❌ Không có link/id nào.", reply_chat_id)
        return
    folder_links = [l for l in lines_raw if "addlist" in l]
    identifiers  = [l for l in lines_raw if "addlist" not in l]
    for fl in folder_links:
        await cmd_addfolder(fl, reply_chat_id)
    if not identifiers:
        return
    channels               = load_channels()
    added, skipped, failed = [], [], []
    await safe_send(f"⏳ Đang xử lý {len(identifiers)} kênh...", reply_chat_id)
    uc = await ensure_user_client()
    for ident in identifiers:
        try:
            chat = None
            for client in (app, uc):
                if client is None:
                    continue
                try:
                    chat = await client.get_chat(ident)
                    break
                except Exception:
                    continue
            if chat is None:
                raise ValueError("bot/user đều không resolve được")
            if any(str(ch["id"]) == str(chat.id) for ch in channels):
                skipped.append(f"⚠️ {chat.title} (đã có)")
                continue
            channels.append({"id": chat.id, "title": chat.title or "", "username": chat.username or "", "alias": ""})
            added.append(f"✅ #{len(channels)}. {chat.title}  (@{chat.username or 'private'})")
        except Exception as e:
            failed.append(f"❌ {ident}  → {e}")
        await asyncio.sleep(0.3)
    save_channels(channels)
    out = []
    if added:
        out.append(f"✅ Đã thêm {len(added)} kênh:")
        out.extend(added)
    if skipped:
        out.append(f"⚠️ Bỏ qua: {len(skipped)}")
        out.extend(skipped)
    if failed:
        out.append(f"❌ Thất bại {len(failed)}:")
        out.extend(failed)
    if added:
        out.append("💡 /alias <số> <tên> để đặt tên tắt")
    await safe_send("\n".join(out), reply_chat_id)


# ─────────────────────────────────────────────────────────
# /check & /clean
# ─────────────────────────────────────────────────────────

async def _probe_channel(ch):
    last_err = None
    uname = ch.get("username") or ""
    for attempt in range(3):
        try:
            await _warm_peer(app, ch["id"], uname or None)
            chat = await app.get_chat(ch["id"])
            return ("alive", chat)
        except FloodWait as e:
            wait = e.value + 2
            log("CHECK", f"FloodWait {wait}s — retry {attempt+1}/3")
            await asyncio.sleep(wait)
            last_err = e
        except PEER_RETRY_ERRORS as e:
            last_err = e
            uc = await ensure_user_client()
            if uc:
                await _import_peer(uc, app, ch["id"])
            await asyncio.sleep(1.0 + attempt)
        except DEAD_CHANNEL_ERRORS as e:
            return ("dead", f"{type(e).__name__}: {str(e)[:80]}")
        except SKIP_NOT_DEAD_ERRORS as e:
            return ("unknown", f"{type(e).__name__}: {str(e)[:80]}")
        except Exception as e:
            return ("unknown", f"{type(e).__name__}: {str(e)[:80]}")
    return ("unknown", f"probe fail: {last_err}")


async def cmd_checkchan(reply_chat_id: int, auto_clean: bool = False, silent: bool = False):
    channels = load_channels()
    if not channels:
        if not silent:
            await safe_send("📭 Chưa có kênh nào để check.", reply_chat_id)
        return 0
    total      = len(channels)
    status_msg = None
    if not silent:
        status_msg = await robust_send(f"🔍 Đang check {total} kênh... (0/{total})", reply_chat_id)
    alive, dead, unknown = [], [], []
    for i, ch in enumerate(channels):
        status, payload = await _probe_channel(ch)
        if status == "alive":
            chat = payload
            if chat.title:
                ch["title"] = chat.title
            ch["username"] = chat.username or ""
            alive.append(ch)
        elif status == "dead":
            dead.append({"ch": ch, "err": payload})
        else:
            unknown.append({"ch": ch, "err": payload})
        if not silent and status_msg and ((i + 1) % 5 == 0 or i == total - 1):
            await robust_edit(
                reply_chat_id, status_msg.id,
                f"🔍 Đang check... ({i+1}/{total})\n"
                f"✅ {len(alive)}   ❌ {len(dead)}   ❓ {len(unknown)}"
            )
        await asyncio.sleep(0.5)
    keep = alive + [item["ch"] for item in unknown]
    if auto_clean:
        save_channels(keep)
    else:
        save_channels(keep + [item["ch"] for item in dead])
    if silent:
        return len(dead)
    lines = [
        f"📊 Kết quả check {total} kênh:",
        "━━━━━━━━━━━━━━━",
        f"✅ Hoạt động:   {len(alive)}",
        f"❌ Chết:        {len(dead)}",
        f"❓ Không rõ:    {len(unknown)}",
    ]
    if dead:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("🪦 Kênh chết (sẽ xóa nếu /clean):")
        for item in dead:
            ch   = item["ch"]
            name = ch.get("title") or str(ch.get("id"))
            lines.append(f"  • {name}")
            lines.append(f"     └ {item['err']}")
    if unknown:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("❓ Không xác định (giữ lại):")
        for item in unknown[:10]:
            ch   = item["ch"]
            name = ch.get("title") or str(ch.get("id"))
            lines.append(f"  • {name}  ({item['err'].split(':')[0]})")
    if auto_clean and dead:
        lines += ["━━━━━━━━━━━━━━━", f"🗑️ Đã xóa {len(dead)} kênh chết."]
    elif dead:
        lines += ["━━━━━━━━━━━━━━━", "💡 /clean — xóa các kênh chết"]
    elif not unknown:
        lines += ["━━━━━━━━━━━━━━━", "🎉 Tất cả kênh đều hoạt động!"]
    final = "\n".join(lines)
    if status_msg:
        ok = await robust_edit(reply_chat_id, status_msg.id, final)
        if not ok:
            await robust_send(final, reply_chat_id)
    else:
        await robust_send(final, reply_chat_id)
    return len(dead)


# ─────────────────────────────────────────────────────────
# Background tasks
# ─────────────────────────────────────────────────────────

_notify_users: set[int] = set()


def register_notify_user(chat_id: int):
    _notify_users.add(chat_id)


async def task_auto_sync_folders():
    await asyncio.sleep(30)
    while True:
        folders = load_folders()
        if folders:
            log("AUTO-SYNC", f"Bắt đầu sync {len(folders)} folder...")
            total_added = 0
            notify_id = next(iter(_notify_users), None)
            for fd in folders:
                slug = fd.get("slug")
                if not slug:
                    continue
                try:
                    added = await cmd_addfolder(
                        f"https://t.me/addlist/{slug}",
                        notify_id or 0,
                        silent=True,
                        remember=False,
                    )
                    total_added += added or 0
                except Exception as e:
                    log("AUTO-SYNC", f"folder {slug}: {e}")
                await asyncio.sleep(2)
            if total_added > 0 and notify_id:
                await safe_send(f"🔄 Auto-sync: thêm {total_added} kênh mới từ folder đã lưu.", notify_id)
        await asyncio.sleep(FOLDER_SYNC_INTERVAL_SEC)


async def task_auto_clean_dead():
    await asyncio.sleep(300)
    while True:
        await asyncio.sleep(DEAD_CHECK_INTERVAL_SEC)
        if state.get("checking"):
            continue
        def _user_busy():
            return any(
                s.get("content_msgs") or s.get("awaiting_channel") or s.get("all_mode")
                for s in state["slots"]
            )
        waited = 0
        while _user_busy() and waited < 600:
            await asyncio.sleep(60)
            waited += 60
        if _user_busy():
            continue
        notify_id = next(iter(_notify_users), None)
        if not notify_id:
            continue
        state["checking"] = True
        try:
            dead_count = await cmd_checkchan(notify_id, auto_clean=True, silent=True)
            if dead_count and dead_count > 0:
                await safe_send(f"🧹 Auto-clean: đã xóa {dead_count} kênh chết.", notify_id)
        except Exception as e:
            log("AUTO-CLEAN", f"Lỗi: {e}")
        finally:
            state["checking"] = False


# ─────────────────────────────────────────────────────────
# Command aliases
# ─────────────────────────────────────────────────────────

COMMAND_ALIASES = {
    "/addchan":   "/add",
    "/addfolder": "/addf",
    "/addbot":    "/botadd",
    "/addbotf":   "/botaddf",
    "/listchan":  "/list",
    "/delchan":   "/del",
    "/aliaschan": "/alias",
    "/checkchan": "/check",
    "/cleanchan": "/clean",
}


def normalize_command(text: str):
    if not text.startswith("/"):
        return text
    for old, new in COMMAND_ALIASES.items():
        if text == old:
            return new
        if text.startswith(old + " ") or text.startswith(old + "\n"):
            return new + text[len(old):]
    return text


# ─────────────────────────────────────────────────────────
# Message handler
# ─────────────────────────────────────────────────────────

def _private_allowed_filter():
    async def func(_, __, msg: Message):
        return (
            msg.chat.type.name == "PRIVATE"
            and msg.from_user
            and is_allowed(msg.from_user.id)
        )
    return filters.create(func)


def _ads_chat_filter():
    async def func(_, __, msg: Message):
        return bool(msg.chat and _is_ads_chat_id(msg.chat.id))
    return filters.create(func)


@app.on_message(_ads_chat_filter())
async def ads_chat_wakeup(client, msg: Message):
    """Tin mới trong nhóm ads → bot cache peer (fix Peer id invalid lúc start)."""
    await mark_ads_chat_live(msg.chat.id, msg.chat.title or "")


@app.on_message(_private_allowed_filter())
async def handler(client, msg: Message):
    chat_id = msg.chat.id
    register_notify_user(chat_id)

    # ══ 1. Bài forward vào bot (DM) ══════════════════════════════
    if msg.forward_date:
        slot = slot_for_user(chat_id)
        slot["user_chat_id"] = chat_id

        if not slot["waiting"]:
            return

        if msg.forward_from_chat and _is_ads_chat_id(msg.forward_from_chat.id):
            slot["ads_chat_id"] = msg.forward_from_chat.id
            await load_ads_into(slot)
            return

        is_first = not slot["topic_checked"] and not slot.get("all_mode")
        if is_first:
            slot["topic_checked"] = True
            slot["_topic_event"]  = asyncio.Event()

            async def _detect(forward_msg, ev, target_slot):
                src_id, top_id, top_title = await resolve_forward_topic(client, forward_msg)
                if top_id is not None:
                    target_slot["topic_id"]    = top_id
                    target_slot["topic_title"] = top_title
                    log("TOPIC", f"Batch topic='{top_title}' id={top_id}")
                else:
                    log("TOPIC", "Không detect được topic")
                ev.set()

            asyncio.ensure_future(_detect(msg, slot["_topic_event"], slot))
        elif "_topic_event" not in slot:
            slot["_topic_event"] = asyncio.Event()
            slot["_topic_event"].set()

        if msg.media_group_id:
            gid = msg.media_group_id
            if gid in slot["seen_media_groups"]:
                return
            slot["seen_media_groups"].add(gid)
            slot["_album_pending"] = slot.get("_album_pending", 0) + 1
            album = None
            try:
                for attempt, delay in enumerate([0.5, 1.0, 2.0]):
                    await asyncio.sleep(delay)
                    try:
                        album = await client.get_media_group(chat_id, msg.id)
                        if album and len(album) >= 1:
                            break
                        album = None
                    except Exception as e:
                        log("WARN", f"get_media_group attempt {attempt+1}: {e}")
                if not album:
                    log("ERROR", f"Bỏ album {gid} — không fetch được")
                    return
                slot["content_msgs"].append(album[0].id)
                slot["total_media_count"] = slot.get("total_media_count", 0) + len(album)
                log("MSG", f"Album {gid} ({len(album)} items) — bài #{len(slot['content_msgs'])}")
            except Exception as e:
                log("ERROR", f"Album {gid} exception: {e}")
                return
            finally:
                slot["_album_pending"] = max(0, slot.get("_album_pending", 0) - 1)
                if not album:
                    slot["seen_media_groups"].discard(gid)
        else:
            slot["content_msgs"].append(msg.id)
            if msg.media:
                slot["total_media_count"] = slot.get("total_media_count", 0) + 1
            log("MSG", f"Bài #{len(slot['content_msgs'])} id={msg.id}")

        asyncio.ensure_future(update_menu(len(slot["content_msgs"]), chat_id))
        return

    # ══ 2. Lệnh text ══════════════════════════════════════════════
    text_raw = (msg.text or "")
    text     = text_raw.strip()
    if not text:
        return

    text     = normalize_command(text)
    text_raw = normalize_command(text_raw)

    if text in ("/start", "/help"):
        n_ch = len(load_channels())
        await safe_send(
            "🤖 Bot phân phối bài v1\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🚀 Flow:\n"
            "  1. Forward bài vào bot (chat này)\n"
            "  2. /done* / /xdone / /zdone → xếp sequence\n"
            "  3. Gõ tên kênh hoặc tap /lệnh → copy ra kênh\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 {n_ch} kênh đã lưu\n"
            "✨ Emoji premium: user session copy ra kênh (cần Premium)!\n"
            "Gõ /help để xem đầy đủ lệnh.",
            chat_id,
        )
        if text == "/help":
            await safe_send(
                "📖 Hướng dẫn đầy đủ\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📡 Quản lý kênh:\n"
                "  /add /addf /list /del /alias /check /clean\n"
                "  /botadd /botaddf — auto mời bot + cấp admin\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🤖 /botadd cần user session (SESSION_STRING hoặc .session)\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🗺️ Auto topic: /mapgen /map\n"
                "⚙️ Xếp bài: /xepbai /xepbaiwhite\n"
                "📋 Mode: /done1~/done10 /xdone /zdone\n"
                "📢 Khác: /all /next /skip\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚙️ Yêu cầu:\n"
                "  • User session admin các kênh đích (đăng bài + emoji premium)\n"
                "  • Bot admin kênh (dự phòng) + trong nhóm ads (đọc tin mới)\n",
                chat_id,
            )
        return

    if text == "/all":
        reset_slot(slot_for_user(chat_id))
        slot = slot_for_user(chat_id)
        slot["user_chat_id"] = chat_id
        slot["all_mode"] = True
        n_ch = len(load_channels())
        await safe_send(
            f"📦 Chế độ /all ĐÃ BẬT.\n"
            f"➡️ Forward bài vào bot → tự up TẤT CẢ {n_ch} kênh.\n"
            f"Gõ /next để huỷ.",
            chat_id,
        )
        return

    if text == "/xepbaiwhite" or text.startswith("/xepbaiwhite "):
        arg = text[len("/xepbaiwhite"):].strip()
        if not arg:
            wl = get_xepbai_whitelist()
            await safe_send(
                "⭐ Whitelist:\n" + (", ".join(sorted(wl)) if wl else "  (trống)")
                + "\nDùng: /xepbaiwhite pro,real  |  /xepbaiwhite clear",
                chat_id,
            )
            return
        if arg.lower() == "clear":
            set_topic_directive("@xepbaiwhite", "")
            await safe_send("⭐ Đã xoá whitelist.", chat_id)
            return
        items = [x.strip().lstrip("/").lower() for x in arg.replace(" ", ",").split(",") if x.strip()]
        set_topic_directive("@xepbaiwhite", ", ".join(items))
        await safe_send("⭐ Whitelist: " + ", ".join(items), chat_id)
        return

    if text == "/xepbai" or text.startswith("/xepbai "):
        arg = text[len("/xepbai"):].strip().lower()
        if arg in ("on", "off"):
            set_topic_directive("@xepbai", arg)
            await safe_send(f"{'🟢' if arg == 'on' else '🔴'} /xepbai {arg.upper()}", chat_id)
            return
        mode = get_xepbai_mode()
        wl   = get_xepbai_whitelist()
        await safe_send(f"⚙️ /xepbai: {mode.upper()} | Whitelist: {', '.join(sorted(wl)) or '(trống)'}", chat_id)
        return

    if text == "/mapgen" or text == "/map gen":
        n_kept, n_new = gen_topic_map_txt()
        await safe_send(f"🧩 Đã ghi {TOPIC_MAP_TXT}: giữ {n_kept}, thêm {n_new} kênh chưa map.", chat_id)
        return

    if text == "/map" or text.startswith("/map "):
        entries = load_topic_txt()
        if not entries:
            await safe_send(f"📭 Chưa map topic. Gõ /mapgen để tạo {TOPIC_MAP_TXT}.", chat_id)
        else:
            lines = [f"🗺️ Mapping ({TOPIC_MAP_TXT}):"]
            for topic, cmd in entries:
                lines.append(f"  • {topic} → /{cmd}")
            await safe_send("\n".join(lines), chat_id)
        return

    if text.startswith("/add ") or text.startswith("/add\n") or text == "/add":
        raw = text_raw[4:].strip()
        if not raw:
            await safe_send("❌ Dùng:\n/add @kenh1\n@kenh2\n-100123456", chat_id)
            return
        await cmd_addchan(raw, chat_id)
        return

    if text.startswith("/addf"):
        lnk = text[5:].strip()
        if not lnk:
            await safe_send("❌ Dùng: /addf https://t.me/addlist/xxxxx", chat_id)
            return
        await cmd_addfolder(lnk, chat_id)
        return

    if text == "/botadd" or text.startswith("/botadd "):
        arg = text[7:].strip() if text.startswith("/botadd ") else ""
        await cmd_botadd(chat_id, arg)
        return

    if text.startswith("/botaddf"):
        lnk = text[8:].strip()
        if not lnk:
            await safe_send(
                "❌ Dùng: /botaddf https://t.me/addlist/xxxxx\n"
                "→ Đọc folder, lưu kênh + mời bot vào + cấp admin đăng bài.",
                chat_id,
            )
            return
        await cmd_botaddfolder(lnk, chat_id)
        return

    if text == "/list":
        channels = load_channels()
        if not channels:
            await safe_send("📭 Chưa có kênh. /add <link> để thêm.", chat_id)
            return
        lines = ["📋 Danh sách kênh:"]
        for i, ch in enumerate(channels):
            alias = f"  [{ch['alias']}]" if ch.get("alias") else ""
            lines.append(
                f"{i+1}. {ch['title']}{alias}\n"
                f"   @{ch.get('username') or 'private'}  |  ID: {ch['id']}"
            )
        await safe_send("\n".join(lines), chat_id)
        return

    if text.startswith("/del "):
        try:
            idx      = int(text[5:].strip()) - 1
            channels = load_channels()
            if 0 <= idx < len(channels):
                removed = channels.pop(idx)
                save_channels(channels)
                await safe_send(f"🗑️ Đã xóa: {removed['title']}", chat_id)
            else:
                await safe_send("❌ Số thứ tự không hợp lệ.", chat_id)
        except ValueError:
            await safe_send("❌ Dùng: /del <số>", chat_id)
        return

    if text.startswith("/alias "):
        parts = text[7:].split(None, 1)
        if len(parts) == 2:
            try:
                idx      = int(parts[0]) - 1
                alias    = parts[1].strip()
                channels = load_channels()
                if 0 <= idx < len(channels):
                    channels[idx]["alias"] = alias
                    save_channels(channels)
                    await safe_send(f"✏️ Alias '{alias}' → {channels[idx]['title']}", chat_id)
                else:
                    await safe_send("❌ Số thứ tự không hợp lệ.", chat_id)
            except ValueError:
                await safe_send("❌ Dùng: /alias <số> <tên>", chat_id)
        return

    if text == "/check" or text == "/clean":
        if state.get("checking"):
            await safe_send("⏳ Đang check rồi...", chat_id)
            return
        auto_clean = (text == "/clean")

        async def _bg_check():
            state["checking"] = True
            try:
                await cmd_checkchan(chat_id, auto_clean=auto_clean)
            except Exception as e:
                await safe_send(f"❌ Check lỗi: {type(e).__name__}", chat_id)
            finally:
                state["checking"] = False

        asyncio.ensure_future(_bg_check())
        await safe_send("🔍 Bắt đầu check ở nền.", chat_id)
        return

    if text == "/checkads":
        await cmd_checkads(chat_id)
        return

    if waiting_slot(chat_id):
        if text == "/skip":
            ws = waiting_slot(chat_id)
            if ws and ws in state["slots"]:
                state["slots"].remove(ws)
            if not any(s.get("user_chat_id") == chat_id for s in state["slots"]):
                ns = make_slot()
                ns["user_chat_id"] = chat_id
                state["slots"].append(ns)
                await load_ads_into(ns)
            await safe_send("⏭️ Đã bỏ qua. Sẵn sàng nhận bài mới!", chat_id)
            return
        if text.startswith("/") and len(text) > 1 and " " not in text and "\n" not in text:
            ws        = waiting_slot(chat_id)
            candidate = text[1:].lower()
            if ws and candidate in (ws.get("channel_commands") or {}):
                await cmd_select_by_cmd(ws, candidate)
                return
        if text and not text.startswith("/"):
            await cmd_select_channel(text, chat_id)
            return

    if text == "/skip":
        await safe_send("⏭️ /skip chỉ dùng khi đang đợi chọn kênh. /next để huỷ batch đang gom.", chat_id)
        return

    if text == "/next":
        reset_state(chat_id)
        await load_ads(chat_id)
        await safe_send("🔄 Reset xong! Forward bài mới vào bot.", chat_id)
        return

    if text == "/xdone":
        slot = active_slot(chat_id)
        if not slot["content_msgs"]:
            await safe_send("⚠️ Chưa có bài nào.", chat_id)
            return
        if not slot["ads_msgs"]:
            await load_ads_into(slot)
        await build_sequence(mode="xdone", chat_id=chat_id)
        return

    if text == "/zdone":
        slot = active_slot(chat_id)
        if not slot["content_msgs"]:
            await safe_send("⚠️ Chưa có bài nào.", chat_id)
            return
        if not slot["ads_msgs"]:
            await load_ads_into(slot)
        await build_sequence(mode="zdone", chat_id=chat_id)
        return

    if text.startswith("/done"):
        num = text[5:]
        try:
            cpa = int(num) if num else 1
        except ValueError:
            cpa = 1
        slot = active_slot(chat_id)
        if not slot["content_msgs"]:
            await safe_send("⚠️ Chưa có bài nào.", chat_id)
            return
        if not slot["ads_msgs"]:
            await load_ads_into(slot)
        await build_sequence(cpa, chat_id=chat_id)
        return


# ─────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────

async def main():
    global _global_bucket, ALLOWED_USER_IDS, ads_chat_resolved, ads_bot_accessible
    _global_bucket = TokenBucket(FWD_GLOBAL_RATE, FWD_GLOBAL_BURST)

    if not BOT_TOKEN:
        log("ERROR", "Thiếu BOT_TOKEN trong .env")
        return

    log("CONFIG", f"ADS_CHAT={ADS_CHAT} | USER_SESSION={USER_SESSION}")
    ensure_topic_map_txt()

    # User session (test_session) — folder, ads đọc, topic raw, botadd
    if SESSION_STRING or _resolve_user_session_name():
        uc = await ensure_user_client()
        if uc:
            await sync_allowed_from_user()
            log("START", "User session OK — folder sync / ads đọc / topic / botadd")
        else:
            log("WARN", "Có file session nhưng không start được — xem lỗi phía trên")
    else:
        log("WARN", f"Không thấy {_session_db_path('test_session')} — copy file session từ tool cũ")

    if not ALLOWED_USER_IDS:
        log("WARN", "ALLOWED_USER_IDS trống — set trong .env hoặc sửa session user")
    else:
        log("CONFIG", f"allowed={ALLOWED_USER_IDS}")

    await app.start()
    me = await app.get_me()
    log("START", f"Bot chạy | @{me.username}")

    await resolve_ads_chat_for_bot()
    await _warm_all_saved_channels()

    asyncio.ensure_future(task_auto_sync_folders())
    asyncio.ensure_future(task_auto_clean_dead())

    log("START", "📡 Đang lắng nghe...")
    await asyncio.Event().wait()


if __name__ == "__main__":
    app.run(main())
