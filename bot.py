# -*- coding: utf-8 -*-
"""
Анонимный чат знакомства — Telegram-бот (aiogram 3 + PostgreSQL/Neon).

Скорость: все данные живут в памяти (кэш). Нажатия кнопок НЕ ходят в базу —
запись в Neon идёт в фоне пакетами. Поэтому ответ на кнопки мгновенный.
"""
import asyncio
import html
import logging
import os
import random
import re
import secrets
import time
from collections import deque
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import asyncpg
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ContentType, ParseMode
from aiogram.exceptions import (TelegramBadRequest, TelegramForbiddenError,
                                TelegramRetryAfter)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, InputMediaPhoto, KeyboardButton,
                           LabeledPrice, Message, PreCheckoutQuery,
                           ReplyKeyboardMarkup)

# ───────────────────────── НАСТРОЙКИ ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
MAIN_ADMIN = int(os.getenv("ADMIN_ID", "7659107145"))
SUPPORT = os.getenv("SUPPORT_USERNAME", "ffxdavlatov").lstrip("@")
PRICE = int(os.getenv("PREMIUM_PRICE_STARS", "100"))
PREM_DAYS = 30
FREE_LIKES = int(os.getenv("FREE_LIKES_PER_DAY", "30"))
AUTOBAN = int(os.getenv("AUTOBAN_REPORTS", "5"))  # 0 = выключить автобан
PORT = int(os.getenv("PORT", "10000"))
BOT_NAME = "Анонимный чат знакомства"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("dating")

# ───────────────────────── ТЕКСТЫ ─────────────────────────
SAFETY = ("⚠️ Не отправляйте адрес, телефон, пароли или финансовые данные. "
          "Используйте кнопки «Жалоба» и «Блок».")

B_SEARCH = "🔍 Найти собеседника"
B_PROFILES = "👤 Анкеты"
B_LIKES = "❤️ Кто оставил мне лайк"
B_PREMIUM = "💎 Премиум"
B_PROFILE = "⚙️ Мой профиль"
B_HELP = "🆘 Помощь"
B_ADMIN = "🛠 Админ-панель"
B_NEXT = "⏭ Следующий"
B_STOP = "⛔ Стоп"
B_REPORT = "🚩 Жалоба"
B_BLOCK = "🚫 Блок"
B_CANCEL = "❌ Отменить поиск"

FEATURES = [
    ("❤️", "Видите, кто поставил вам лайк"),
    ("🎯", "Поиск собеседника и анкет по полу"),
    ("🎂", "Фильтр по возрасту"),
    ("⚡", "Приоритет в очереди — вас находят первыми"),
    ("📷", "Фото, видео, голосовые, кружки и GIF в анонимном чате"),
    ("🔗", "Ссылки в сообщениях"),
    ("♾", f"Безлимитные лайки (бесплатно — {FREE_LIKES} в день)"),
    ("💎", "Значок Premium в вашей анкете"),
]
REASONS = {
    "spam": "Спам / реклама",
    "abuse": "Оскорбления",
    "adult": "Откровенный контент",
    "scam": "Мошенничество",
    "minor": "Несовершеннолетний",
    "profile": "Анкета",
}
TIPS = [
    "💡 Вы анонимны — собеседник не видит ваш аккаунт",
    "💎 Premium — поиск без очереди и по полу",
    "👤 Заполните анкету — с ней вам пишут чаще",
    "🛡 «Жалоба» и «Блок» всегда под рукой",
]
MOON = "🌑🌒🌓🌔🌕🌖🌗🌘"
URL_RE = re.compile(
    r"(https?://|www\.|t\.me/|telegram\.me/|\b[\w-]+\.(?:com|ru|org|net|me|io|tj|uz|kz|info|xyz)\b)",
    re.I)

# ───────────────────────── ДАННЫЕ (КЭШ) ─────────────────────────


@dataclass
class U:
    id: int
    username: str = ""
    first_name: str = ""
    gender: str = ""          # m / f
    age: int = 0
    city: str = ""
    about: str = ""
    photo: str = ""
    premium_until: int = 0
    banned: bool = False
    ban_reason: str = ""
    registered: int = 0
    last_seen: int = 0
    adult_ok: bool = False
    likes_day: str = ""
    likes_used: int = 0
    pref: str = "any"         # any / m / f   (Premium)
    amin: int = 18            # возрастной фильтр (Premium)
    amax: int = 99
    visible: bool = True
    chats: int = 0
    ready: bool = False
    alive: bool = True


COLS = [f.name for f in fields(U)]
UPSERT = ("INSERT INTO users(%s) VALUES(%s) ON CONFLICT(id) DO UPDATE SET %s" % (
    ",".join(COLS),
    ",".join("$%d" % (i + 1) for i in range(len(COLS))),
    ",".join("%s=EXCLUDED.%s" % (c, c) for c in COLS if c != "id")))

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
 id BIGINT PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT '',
 gender TEXT DEFAULT '', age INT DEFAULT 0, city TEXT DEFAULT '', about TEXT DEFAULT '',
 photo TEXT DEFAULT '', premium_until BIGINT DEFAULT 0, banned BOOLEAN DEFAULT FALSE,
 ban_reason TEXT DEFAULT '', registered BIGINT DEFAULT 0, last_seen BIGINT DEFAULT 0,
 adult_ok BOOLEAN DEFAULT FALSE, likes_day TEXT DEFAULT '', likes_used INT DEFAULT 0,
 pref TEXT DEFAULT 'any', amin INT DEFAULT 18, amax INT DEFAULT 99,
 visible BOOLEAN DEFAULT TRUE, chats INT DEFAULT 0, ready BOOLEAN DEFAULT FALSE,
 alive BOOLEAN DEFAULT TRUE);
CREATE TABLE IF NOT EXISTS likes(from_id BIGINT, to_id BIGINT, created BIGINT,
 PRIMARY KEY(from_id,to_id));
CREATE INDEX IF NOT EXISTS likes_to_idx ON likes(to_id);
CREATE TABLE IF NOT EXISTS blocks(user_id BIGINT, blocked_id BIGINT,
 PRIMARY KEY(user_id,blocked_id));
CREATE TABLE IF NOT EXISTS reports(id BIGSERIAL PRIMARY KEY, from_id BIGINT, to_id BIGINT,
 reason TEXT, created BIGINT);
CREATE TABLE IF NOT EXISTS channels(chat_id BIGINT PRIMARY KEY, title TEXT, link TEXT);
CREATE TABLE IF NOT EXISTS admins(user_id BIGINT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS payments(charge_id TEXT PRIMARY KEY, user_id BIGINT,
 amount INT, created BIGINT);
"""
INS_LIKE = "INSERT INTO likes(from_id,to_id,created) VALUES($1,$2,$3) ON CONFLICT DO NOTHING"
INS_BLOCK = "INSERT INTO blocks(user_id,blocked_id) VALUES($1,$2) ON CONFLICT DO NOTHING"
INS_REPORT = "INSERT INTO reports(from_id,to_id,reason,created) VALUES($1,$2,$3,$4)"
INS_PAY = ("INSERT INTO payments(charge_id,user_id,amount,created) VALUES($1,$2,$3,$4) "
           "ON CONFLICT DO NOTHING")

users: dict = {}
dirty: set = set()
likes_from: dict = {}      # кто -> кого лайкнул
likes_to: dict = {}        # кого -> кто лайкнул
blocks: dict = {}          # user -> {заблокированные}
reporters: dict = {}       # цель -> {кто жаловался}
CHANNELS: list = []        # [{chat_id,title,link}]
admins: set = set()        # доп. админы (кроме MAIN_ADMIN)
partner: dict = {}         # uid -> uid в активном чате
last_partner: dict = {}
recent: dict = {}          # чтобы не соединять сразу повторно
sub_ok: dict = {}
view: dict = {}            # состояние просмотра анкет
seen: dict = {}
seen_likes: dict = {}
like_notified: dict = {}
tokens: dict = {}
paid_ids: set = set()
flood: dict = {}
hint_ts: dict = {}
ban_msg_ts: dict = {}
bot: Bot = None  # type: ignore


@dataclass
class Q:
    uid: int
    since: float
    prem: bool
    chat_id: int
    msg_id: int = 0


queue: dict = {}
anim: dict = {}
_pool = {"ids": [], "ts": 0.0}
_tasks: set = set()
broadcast_running = False


def now() -> int:
    return int(time.time())


def esc(s) -> str:
    return html.escape(str(s or ""))


def is_admin(uid: int) -> bool:
    return uid == MAIN_ADMIN or uid in admins


def all_admins() -> set:
    return {MAIN_ADMIN} | set(admins)


def is_prem(uid: int) -> bool:
    if is_admin(uid):
        return True
    u = users.get(uid)
    return bool(u and u.premium_until > now())


def today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%d.%m.%Y %H:%M") + " UTC"


def gl(g: str) -> str:
    return "👩 Девушка" if g == "f" else "👨 Парень"


PREF_LABEL = {"any": "Все", "m": "Парни", "f": "Девушки"}


def bg(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)

    def _done(task):
        _tasks.discard(task)
        if not task.cancelled() and task.exception():
            log.error("bg task error: %r", task.exception())
    t.add_done_callback(_done)
    return t


# ───────────────────────── БАЗА ДАННЫХ ─────────────────────────
def clean_dsn(dsn: str) -> str:
    """Neon даёт ссылку с channel_binding — asyncpg его не знает, убираем."""
    p = urlsplit(dsn)
    q = [(k, v) for k, v in parse_qsl(p.query) if k != "channel_binding"]
    if not any(k == "sslmode" for k, _ in q):
        q.append(("sslmode", "require"))
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q), p.fragment))


class DB:
    def __init__(self):
        self.pool = None
        self.q: asyncio.Queue = asyncio.Queue()

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            clean_dsn(DATABASE_URL), min_size=1, max_size=4,
            statement_cache_size=0, max_inactive_connection_lifetime=120,
            command_timeout=30)

    async def _run(self, kind, sql, args):
        last = None
        for attempt in range(4):
            try:
                async with self.pool.acquire() as c:
                    if kind == "exec":
                        return await c.execute(sql, *args)
                    if kind == "many":
                        return await c.executemany(sql, args)
                    return await c.fetch(sql, *args)
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError,
                    OSError, asyncio.TimeoutError) as e:
                last = e
                await asyncio.sleep(0.6 * (attempt + 1))
            except Exception as e:  # ошибка запроса — повтор не поможет
                log.error("DB error: %s | %s", e, sql[:80])
                return None
        log.error("DB unavailable: %s", last)
        return None

    async def execute(self, sql, *args):
        return await self._run("exec", sql, args)

    async def executemany(self, sql, rows) -> bool:
        if not rows:
            return True
        return await self._run("many", sql, rows) is not False and True

    async def fetch(self, sql, *args):
        return await self._run("fetch", sql, args)

    def enqueue(self, sql, *args):
        self.q.put_nowait((sql, args))

    async def writer(self):
        while True:
            sql, args = await self.q.get()
            try:
                await self.execute(sql, *args)
            except Exception as e:
                log.error("writer: %s", e)


db = DB()


def user_row(u: U):
    return [getattr(u, c) for c in COLS]


async def flush_dirty():
    if not dirty:
        return
    ids = list(dirty)
    dirty.clear()
    rows = [user_row(users[i]) for i in ids if i in users]
    try:
        await db.pool.executemany(UPSERT, rows)
    except Exception as e:
        log.error("flush failed: %s", e)
        dirty.update(ids)


async def flusher():
    while True:
        await asyncio.sleep(2)
        try:
            await flush_dirty()
        except Exception as e:
            log.error("flusher: %s", e)


async def load_all():
    await db.execute(SCHEMA)
    for r in await db.fetch("SELECT * FROM users") or []:
        u = U(**{c: r[c] for c in COLS})
        users[u.id] = u
    for r in await db.fetch("SELECT from_id,to_id FROM likes") or []:
        likes_from.setdefault(r["from_id"], set()).add(r["to_id"])
        likes_to.setdefault(r["to_id"], set()).add(r["from_id"])
    for r in await db.fetch("SELECT user_id,blocked_id FROM blocks") or []:
        blocks.setdefault(r["user_id"], set()).add(r["blocked_id"])
    for r in await db.fetch("SELECT DISTINCT from_id,to_id FROM reports") or []:
        reporters.setdefault(r["to_id"], set()).add(r["from_id"])
    for r in await db.fetch("SELECT chat_id,title,link FROM channels") or []:
        CHANNELS.append({"chat_id": r["chat_id"], "title": r["title"], "link": r["link"]})
    for r in await db.fetch("SELECT user_id FROM admins") or []:
        admins.add(r["user_id"])
    log.info("Загружено: %d пользователей, %d каналов", len(users), len(CHANNELS))


# ───────────────────────── КЛАВИАТУРЫ ─────────────────────────
def btn(text, cb=None, url=None):
    return InlineKeyboardButton(text=text, callback_data=cb, url=url)


def ikb(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_kb(uid):
    rows = [[KeyboardButton(text=B_SEARCH)],
            [KeyboardButton(text=B_PROFILES), KeyboardButton(text=B_LIKES)],
            [KeyboardButton(text=B_PREMIUM), KeyboardButton(text=B_PROFILE)],
            [KeyboardButton(text=B_HELP)]]
    if is_admin(uid):
        rows.append([KeyboardButton(text=B_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True,
                               input_field_placeholder="Выберите действие 👇")


def chat_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=B_NEXT), KeyboardButton(text=B_STOP)],
                  [KeyboardButton(text=B_REPORT), KeyboardButton(text=B_BLOCK)]],
        resize_keyboard=True, is_persistent=True, input_field_placeholder="Напишите сообщение…")


def search_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=B_CANCEL)]],
                               resize_keyboard=True, is_persistent=True)


def after_kb():
    return ikb([[btn("🔍 Новый собеседник", "sr")],
                [btn("🚩 Пожаловаться", "rpt"), btn("🚫 Заблокировать", "blk")]])


def support_kb():
    return ikb([[btn("✉️ Написать @" + SUPPORT, url="https://t.me/" + SUPPORT)]])


# ───────────────────────── ОТПРАВКА ─────────────────────────
async def send(chat_id, text, **kw):
    for _ in range(2):
        try:
            return await bot.send_message(chat_id, text, **kw)
        except TelegramRetryAfter as e:
            await asyncio.sleep(min(e.retry_after, 10))
        except TelegramForbiddenError:
            u = users.get(chat_id)
            if u and u.alive:
                u.alive = False
                dirty.add(chat_id)
            return None
        except Exception as e:
            log.warning("send to %s failed: %s", chat_id, e)
            return None
    return None


async def edit_safe(chat_id, mid, text, kb=None):
    try:
        await bot.edit_message_text(text, chat_id=chat_id, message_id=mid, reply_markup=kb)
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(min(e.retry_after, 5))
    except Exception:
        pass
    return False


async def safe_delete(chat_id, mid):
    try:
        await bot.delete_message(chat_id, mid)
    except Exception:
        pass


async def cb_edit(cb: CallbackQuery, text, kb=None):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" in str(e):
            return
        await cb.message.answer(text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)


def throttled(store: dict, uid: int, gap: float) -> bool:
    t = time.time()
    if t - store.get(uid, 0) < gap:
        return True
    store[uid] = t
    return False


# ───────────────────────── ПОЛЬЗОВАТЕЛИ / ПОДПИСКА ─────────────────────────
def touch(tg) -> U:
    u = users.get(tg.id)
    t = now()
    if not u:
        u = U(id=tg.id, registered=t, last_seen=t)
        users[tg.id] = u
        dirty.add(tg.id)
    changed = False
    uname = tg.username or ""
    fn = (tg.first_name or "")[:64]
    if u.username != uname or u.first_name != fn:
        u.username, u.first_name, changed = uname, fn, True
    if not u.alive:
        u.alive, changed = True, True
    if t - u.last_seen > 600:
        u.last_seen, changed = t, True
    if changed:
        dirty.add(u.id)
    return u


async def is_member(ch, uid) -> bool:
    try:
        m = await bot.get_chat_member(ch["chat_id"], uid)
        if m.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR,
                        ChatMemberStatus.CREATOR):
            return True
        if m.status == ChatMemberStatus.RESTRICTED and getattr(m, "is_member", False):
            return True
        return False
    except Exception as e:  # бот не админ / канал недоступен — не блокируем людей
        log.warning("check sub %s failed: %s", ch.get("chat_id"), e)
        return True


async def missing_channels(uid) -> list:
    chans = list(CHANNELS)
    if not chans:
        return []
    res = await asyncio.gather(*[is_member(c, uid) for c in chans])
    miss = [c for c, ok in zip(chans, res) if not ok]
    if miss:
        sub_ok.pop(uid, None)
    else:
        sub_ok[uid] = time.time()
    return miss


def gate_view(miss):
    rows = [[btn("📢 " + c["title"], url=c["link"])] for c in miss]
    rows.append([btn("✅ Я подписался", "chk_sub")])
    text = ("📢 <b>Подпишитесь на каналы, чтобы пользоваться ботом</b>\n\n"
            "После подписки нажмите «✅ Я подписался».")
    return text, ikb(rows)


class Guard(BaseMiddleware):
    """Бан и обязательная подписка для всех сообщений и кнопок."""

    async def __call__(self, handler, event, data):
        tg = event.from_user
        if tg is None or tg.is_bot:
            return
        if isinstance(event, Message) and event.chat.type != "private":
            return
        u = touch(tg)
        if isinstance(event, Message) and event.successful_payment:
            return await handler(event, data)
        if is_admin(u.id):
            return await handler(event, data)
        if u.banned:
            text = ("🚫 <b>Ваш аккаунт заблокирован</b>\n"
                    f"Причина: {esc(u.ban_reason or 'нарушение правил')}\n\n"
                    f"Для разбана напишите: @{SUPPORT}")
            if isinstance(event, CallbackQuery):
                await event.answer("🚫 Аккаунт заблокирован", show_alert=True)
            if not throttled(ban_msg_ts, u.id, 30):
                await send(u.id, text, reply_markup=support_kb())
            return
        is_chk = isinstance(event, CallbackQuery) and event.data == "chk_sub"
        if CHANNELS and not is_chk and time.time() - sub_ok.get(u.id, 0) > 180:
            miss = await missing_channels(u.id)
            if miss:
                if isinstance(event, CallbackQuery):
                    await event.answer("Сначала подпишитесь на каналы", show_alert=True)
                text, kb = gate_view(miss)
                await send(u.id, text, reply_markup=kb)
                return
        return await handler(event, data)


# ───────────────────────── ПОДБОР СОБЕСЕДНИКОВ ─────────────────────────
def pref_ok(seeker: U, other: U) -> bool:
    if is_prem(seeker.id):
        if seeker.pref in ("m", "f") and other.gender != seeker.pref:
            return False
        if not (seeker.amin <= other.age <= seeker.amax):
            return False
    return True


def compat(a: U, b: U) -> bool:
    if b.id in blocks.get(a.id, ()) or a.id in blocks.get(b.id, ()):
        return False
    return pref_ok(a, b) and pref_ok(b, a)


def try_match(uid: int):
    me = users[uid]
    cands = sorted((e for e in queue.values() if e.uid != uid),
                   key=lambda e: (not e.prem, e.since))
    for e in cands:
        o = users.get(e.uid)
        if o and not o.banned and compat(me, o) \
                and recent.get(uid) != e.uid and recent.get(e.uid) != uid:
            return e.uid
    return None


def stop_anim(uid):
    t = anim.pop(uid, None)
    if t and t is not asyncio.current_task():
        t.cancel()


def search_text(sec, n, tick, prem):
    dots = "." * (tick % 4)
    moon = MOON[tick % len(MOON)]
    tip = TIPS[(sec // 5) % len(TIPS)]
    if prem and "Premium" in tip:
        tip = TIPS[0]
    return (f"{moon} <b>Ищем собеседника{dots}</b>\n\n"
            f"⏱ Прошло: {sec} сек\n👥 Сейчас в поиске: {n}\n\n{tip}")


async def animate(uid):
    tick = 0
    try:
        while uid in queue:
            await asyncio.sleep(1.2)
            e = queue.get(uid)
            if not e:
                return
            if not e.msg_id:
                continue
            tick += 1
            sec = int(time.time() - e.since)
            if sec > 300:
                queue.pop(uid, None)
                anim.pop(uid, None)
                await edit_safe(e.chat_id, e.msg_id, "😔 <b>Пока никого не нашли</b>")
                await send(e.chat_id, "Попробуйте ещё раз чуть позже — нажмите «🔍 Найти собеседника».",
                           reply_markup=main_kb(uid))
                return
            await edit_safe(e.chat_id, e.msg_id, search_text(sec, len(queue), tick, e.prem))
    except asyncio.CancelledError:
        pass


async def start_search(uid: int, chat_id: int):
    if uid in partner:
        await send(chat_id, "💬 Вы уже в диалоге. «⏭ Следующий» — найти другого.", reply_markup=chat_kb())
        return
    if uid in queue:
        await send(chat_id, "🔎 Поиск уже идёт…", reply_markup=search_kb())
        return
    other = try_match(uid)
    if other is not None:
        await connect(uid, other, chat_id)
        return
    queue[uid] = Q(uid, time.time(), is_prem(uid), chat_id)  # сразу в очередь (без гонок)
    msg = await send(chat_id, search_text(0, len(queue), 0, is_prem(uid)), reply_markup=search_kb())
    e = queue.get(uid)
    if e is None:                 # пока слали сообщение, нас уже соединили/отменили
        if msg:
            bg(safe_delete(chat_id, msg.message_id))
        return
    if msg:
        e.msg_id = msg.message_id
    anim[uid] = asyncio.create_task(animate(uid))


async def connect(a: int, b: int, chat_a: int = 0):
    ea, eb = queue.pop(a, None), queue.pop(b, None)
    stop_anim(a)
    stop_anim(b)
    partner[a], partner[b] = b, a                  # без await между проверкой и записью
    ua, ub = users[a], users[b]
    ua.chats += 1
    ub.chats += 1
    dirty.update((a, b))
    await asyncio.gather(_pair_msg(a, ub, ea), _pair_msg(b, ua, eb))


async def _pair_msg(uid, other: U, entry):
    if entry and entry.msg_id:
        await edit_safe(entry.chat_id, entry.msg_id, "✅ <b>Собеседник найден!</b>")
    prem = " 💎" if is_prem(other.id) else ""
    text = (f"✅ <b>Собеседник найден!</b>\n"
            f"{gl(other.gender)}, {other.age} лет{prem}\n\n"
            "Пишите — собеседник не видит ваш аккаунт.\n\n" + SAFETY)
    await send(uid, text, reply_markup=chat_kb())


def finish_chat(uid):
    pid = partner.pop(uid, None)
    if pid is None:
        return None
    partner.pop(pid, None)
    recent[uid], recent[pid] = pid, uid
    last_partner[uid], last_partner[pid] = pid, uid
    return pid


async def post_chat(uid, text):
    await send(uid, text, reply_markup=main_kb(uid))
    await send(uid, "Что дальше?", reply_markup=after_kb())


async def end_chat_both(uid, text_self="⛔ Вы завершили диалог.",
                        text_other="😔 Собеседник завершил диалог."):
    pid = finish_chat(uid)
    if pid is None:
        return None
    await asyncio.gather(post_chat(uid, text_self) if text_self else asyncio.sleep(0),
                         post_chat(pid, text_other))
    return pid


def cancel_search(uid) -> bool:
    e = queue.pop(uid, None)
    stop_anim(uid)
    if e and e.msg_id:
        bg(edit_safe(e.chat_id, e.msg_id, "❌ Поиск отменён"))
    return e is not None


# ───────────────────────── ЖАЛОБЫ / БЛОКИ / БАН ─────────────────────────
def do_block(uid, target):
    blocks.setdefault(uid, set()).add(target)
    db.enqueue(INS_BLOCK, uid, target)


async def file_report(uid, target, code):
    do_block(uid, target)
    reporters.setdefault(target, set()).add(uid)
    db.enqueue(INS_REPORT, uid, target, code, now())
    n = len(reporters[target])
    t = users.get(target)
    text = (f"🚩 <b>Жалоба</b>: {esc(REASONS.get(code, code))}\n"
            f"На: <code>{target}</code> {('@' + esc(t.username)) if t and t.username else ''}\n"
            f"От: <code>{uid}</code>\nВсего жалоб на пользователя: {n}")
    kb = ikb([[btn("🚫 Забанить", f"adm:qban:{target}"), btn("👁 Профиль", f"adm:info:{target}")]])
    for a in all_admins():
        bg(send(a, text, reply_markup=kb))
    if AUTOBAN and n >= AUTOBAN and t and not t.banned and not is_admin(target):
        await ban_user(target, "Множественные жалобы (автоматически)")


async def ban_user(uid, reason="Нарушение правил") -> bool:
    u = users.get(uid)
    if not u:
        return False
    u.banned, u.ban_reason = True, reason
    dirty.add(uid)
    cancel_search(uid)
    await end_chat_both(uid, "", "😔 Собеседник завершил диалог.")
    await send(uid, "🚫 <b>Ваш аккаунт заблокирован</b>\n"
                    f"Причина: {esc(reason)}\n\nДля разбана напишите: @{SUPPORT}",
               reply_markup=support_kb())
    return True


async def unban_user(uid) -> bool:
    u = users.get(uid)
    if not u:
        return False
    u.banned, u.ban_reason = False, ""
    reporters.pop(uid, None)
    dirty.add(uid)
    await send(uid, "✅ Ваш аккаунт разблокирован. Нажмите /start", reply_markup=main_kb(uid))
    return True


async def give_premium(uid, days):
    u = users[uid]
    u.premium_until = max(now(), u.premium_until) + days * 86400
    dirty.add(uid)
    await flush_dirty()


# ───────────────────────── АНКЕТЫ ─────────────────────────
def card_text(o: U) -> str:
    lines = [f"<b>{gl(o.gender)}, {o.age}</b>" + (" 💎" if is_prem(o.id) else "")]
    if o.city:
        lines.append("📍 " + esc(o.city))
    if o.about:
        lines.append("\n" + esc(o.about))
    return "\n".join(lines)


def get_pool():
    if time.time() - _pool["ts"] > 30:
        _pool["ids"] = [u.id for u in users.values()
                        if u.ready and u.visible and not u.banned and u.alive]
        _pool["ts"] = time.time()
    return _pool["ids"]


def _base_ok(uid, tid) -> bool:
    o = users.get(tid)
    if not o or tid == uid or not (o.ready and o.visible and not o.banned and o.alive):
        return False
    return tid not in blocks.get(uid, ()) and uid not in blocks.get(tid, ())


def pick_browse(uid):
    me = users[uid]
    sn = seen.setdefault(uid, set())
    liked = likes_from.get(uid, ())
    if is_prem(uid):
        want = me.pref if me.pref in ("m", "f") else None
    else:
        want = "f" if me.gender == "m" else "m"
    prem = is_prem(uid)

    def ok(tid):
        if tid in sn or tid in liked or not _base_ok(uid, tid):
            return False
        o = users[tid]
        if want and o.gender != want:
            return False
        if prem and not (me.amin <= o.age <= me.amax):
            return False
        return True
    pool = get_pool()
    if len(pool) <= 300:
        c = [t for t in pool if ok(t)]
        return random.choice(c) if c else None
    for _ in range(120):
        t = random.choice(pool)
        if ok(t):
            return t
    return None


def likers_of(uid) -> list:
    mine = likes_from.get(uid, ())
    sl = seen_likes.get(uid, ())
    return [t for t in likes_to.get(uid, ()) if t not in mine and t not in sl and _base_ok(uid, t)]


def card_kb(seq):
    return ikb([[btn("❤️ Нравится", f"lk:{seq}"), btn("👎 Дальше", f"nx:{seq}")],
                [btn("🚩 Жалоба", f"rp:{seq}"), btn("🏠 Закрыть", "cl")]])


async def send_card(chat_id, o: U, text, kb=None):
    if o.photo:
        try:
            return await bot.send_photo(chat_id, o.photo, caption=text, reply_markup=kb)
        except Exception as e:
            log.warning("photo send failed: %s", e)
    return await send(chat_id, text, reply_markup=kb)


async def show_card(uid, chat_id, mode, cb: CallbackQuery = None):
    tid = random.choice(likers_of(uid) or [None]) if mode == "likes" else pick_browse(uid)
    old = view.get(uid)
    if tid is None:
        if mode == "likes":
            text, kb = "💔 Пока нет новых лайков. Загляните позже!", ikb([[btn("🏠 Закрыть", "cl")]])
        else:
            text = ("😔 <b>Подходящие анкеты закончились</b>\n"
                    "Загляните позже или посмотрите заново.")
            kb = ikb([[btn("🔄 Смотреть заново", "rs")], [btn("🏠 Закрыть", "cl")]])
        if cb and old and old.get("mid") == cb.message.message_id:
            try:
                await bot.delete_message(chat_id, old["mid"])
            except Exception:
                pass
        elif old and old.get("mid"):
            bg(safe_delete(chat_id, old["mid"]))
        view.pop(uid, None)
        await send(chat_id, text, reply_markup=kb)
        return
    o = users[tid]
    seq = (old["seq"] + 1) if old else 1
    text, kb = card_text(o), card_kb(seq)
    if mode == "browse":
        seen.setdefault(uid, set()).add(tid)
    new_mid = None
    done = False
    if cb and old and old.get("mid") == cb.message.message_id:
        old_photo = bool(cb.message.photo)
        try:
            if old_photo and o.photo:
                await bot.edit_message_media(
                    InputMediaPhoto(media=o.photo, caption=text, parse_mode=ParseMode.HTML),
                    chat_id=chat_id, message_id=old["mid"], reply_markup=kb)
                done = True
            elif not old_photo and not o.photo:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=old["mid"], reply_markup=kb)
                done = True
        except Exception as e:
            log.info("card edit fallback: %s", e)
        if done:
            new_mid = old["mid"]
    if not done:
        msg = await send_card(chat_id, o, text, kb)
        new_mid = msg.message_id if msg else None
        if old and old.get("mid") and old["mid"] != new_mid:
            bg(safe_delete(chat_id, old["mid"]))
    view[uid] = {"t": tid, "seq": seq, "mode": mode, "mid": new_mid}


def add_like(a, b) -> bool:
    likes_from.setdefault(a, set()).add(b)
    likes_to.setdefault(b, set()).add(a)
    db.enqueue(INS_LIKE, a, b, now())
    return a in likes_from.get(b, ())


def make_token(a, b) -> str:
    t = secrets.token_hex(5)
    tokens[t] = (a, b, time.time() + 86400)
    return t


async def notify_like(a, b, mutual):
    if mutual:
        tok = make_token(a, b)
        kb = ikb([[btn("💬 Начать анонимный чат", f"iv:{tok}")]])
        for x, y in ((a, b), (b, a)):
            await send_card(x, users[y], "💘 <b>Взаимная симпатия!</b>\n\n" + card_text(users[y]) +
                            "\n\nХотите пообщаться анонимно?", kb)
        return
    ub = users.get(b)
    if not ub or ub.banned or not ub.alive:
        return
    prem = is_prem(b)
    if throttled(like_notified, b, 600 if prem else 1800):
        return
    if prem:
        await send(b, "❤️ <b>Кому-то понравилась ваша анкета!</b>",
                   reply_markup=ikb([[btn("❤️ Посмотреть", "lkv")]]))
    else:
        await send(b, "❤️ <b>Вам поставили лайк!</b>\nХотите узнать, кто это? Откройте 💎 Premium.",
                   reply_markup=ikb([[btn("💎 Узнать, кто", "prem")]]))


# ───────────────────────── ПРЕМИУМ ─────────────────────────
def prem_view(uid):
    u = users[uid]
    if is_admin(uid):
        text = ("👑 <b>Администратор</b>\n\nВам доступны все функции Premium бесплатно и без ограничений.\n\n" +
                "\n".join(f"✅ {e} {t}" for e, t in FEATURES))
        return text, ikb([[btn("🏠 Закрыть", "cl")]])
    if u.premium_until > now():
        left = u.premium_until - now()
        text = (f"💎 <b>Premium активен</b>\n\n📅 Действует до: <b>{fmt_ts(u.premium_until)}</b>\n"
                f"⏳ Осталось: <b>{left // 86400} дн. {(left % 86400) // 3600} ч.</b>\n\n"
                "Ваши возможности:\n" + "\n".join(f"✅ {e} {t}" for e, t in FEATURES))
        return text, ikb([[btn(f"➕ Продлить на {PREM_DAYS} дней — {PRICE} ⭐", "buy")],
                          [btn("🏠 Закрыть", "cl")]])
    text = ("💎 <b>Premium — больше возможностей</b>\n\nЧто вы получите:\n" +
            "\n".join(f"{e} {t}" for e, t in FEATURES) +
            f"\n\n💰 Цена: <b>{PRICE} ⭐ на {PREM_DAYS} дней</b>\nОплата звёздами Telegram — быстро и безопасно.")
    return text, ikb([[btn(f"⭐ Купить Premium — {PRICE} ⭐", "buy")], [btn("🏠 Закрыть", "cl")]])


# ───────────────────────── СОСТОЯНИЯ ─────────────────────────
class Reg(StatesGroup):
    age = State()


class Edit(StatesGroup):
    city = State()
    about = State()
    photo = State()
    age = State()
    agef = State()


class AdminSt(StatesGroup):
    inp = State()
    bc_msg = State()
    bc_btn = State()


router = Router()


# ───────────────────────── РЕГИСТРАЦИЯ ─────────────────────────
async def onboarding(m: Message, state: FSMContext, u: U):
    if not u.adult_ok:
        await m.answer(f"👋 <b>{BOT_NAME}</b>\n\nОбщайтесь анонимно: случайный собеседник или анкеты.\n\n"
                       "🔞 <b>Сервис только для лиц 18+</b>\nВам уже есть 18 лет?",
                       reply_markup=ikb([[btn("✅ Да, мне 18+", "adult:1"), btn("❌ Нет", "adult:0")]]))
    elif not u.gender:
        await m.answer("Кто вы?", reply_markup=ikb([[btn("👨 Парень", "g:m"), btn("👩 Девушка", "g:f")]]))
    elif not u.age:
        await state.set_state(Reg.age)
        await m.answer("🎂 Сколько вам лет? Напишите число (от 18).")
    else:
        await finish_reg(m, state, u)


async def finish_reg(m: Message, state: FSMContext, u: U):
    await state.clear()
    u.ready = True
    dirty.add(u.id)
    _pool["ts"] = 0
    await m.answer(f"🎉 <b>Добро пожаловать в «{BOT_NAME}»!</b>\n\n"
                   "🔍 <b>Найти собеседника</b> — случайный анонимный чат\n"
                   "👤 <b>Анкеты</b> — смотрите и ставьте лайки\n"
                   "⚙️ <b>Мой профиль</b> — добавьте город, «о себе» и фото\n\n" + SAFETY,
                   reply_markup=main_kb(u.id))


async def ready_gate(m: Message, state: FSMContext) -> bool:
    u = users[m.from_user.id]
    if u.ready:
        return True
    await onboarding(m, state, u)
    return False


@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    if uid in partner:
        await m.answer("💬 Вы в диалоге. «⛔ Стоп» — завершить.", reply_markup=chat_kb())
        return
    if uid in queue:
        await m.answer("🔎 Идёт поиск собеседника…", reply_markup=search_kb())
        return
    u = users[uid]
    if u.ready:
        await m.answer(f"🏠 <b>{BOT_NAME}</b>\nВыберите действие 👇", reply_markup=main_kb(uid))
    else:
        await onboarding(m, state, u)


@router.callback_query(F.data.startswith("adult:"))
async def adult_cb(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    u = users[cb.from_user.id]
    if cb.data == "adult:0":
        await cb_edit(cb, "😔 Сервис доступен только совершеннолетним. Возвращайтесь, когда вам исполнится 18.")
        return
    u.adult_ok = True
    dirty.add(u.id)
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    await onboarding(cb.message, state, u)


@router.callback_query(F.data.in_({"g:m", "g:f"}))
async def gender_cb(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    u = users[cb.from_user.id]
    if not u.gender:
        u.gender = cb.data[2]
        dirty.add(u.id)
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    await onboarding(cb.message, state, u)


@router.message(Reg.age, F.text)
async def reg_age(m: Message, state: FSMContext):
    u = users[m.from_user.id]
    t = m.text.strip()
    if not t.isdigit():
        await m.answer("Напишите возраст числом, например: 24")
        return
    age = int(t)
    if age < 18:
        await ban_user(u.id, "Возраст младше 18 лет")
        await state.clear()
        return
    if age > 99:
        await m.answer("Укажите реальный возраст (18–99).")
        return
    u.age = age
    dirty.add(u.id)
    await finish_reg(m, state, u)


@router.callback_query(F.data == "chk_sub")
async def chk_sub(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    miss = await missing_channels(uid)
    if miss:
        await cb.answer("❌ Вы подписались не на все каналы", show_alert=True)
        text, kb = gate_view(miss)
        await cb_edit(cb, text, kb)
        return
    await cb.answer("✅ Спасибо!")
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    u = users[uid]
    if u.ready:
        await cb.message.answer("🏠 Главное меню 👇", reply_markup=main_kb(uid))
    else:
        await onboarding(cb.message, state, u)


# ───────────────────────── МЕНЮ ─────────────────────────
@router.message(Command("menu"))
async def cmd_menu(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🏠 Главное меню 👇", reply_markup=main_kb(m.from_user.id))


@router.message(F.text == B_SEARCH)
@router.message(Command("search"))
async def h_search(m: Message, state: FSMContext):
    await state.clear()
    if m.from_user.id in partner:
        await m.answer("💬 Вы уже в диалоге. «⏭ Следующий» — найти другого.", reply_markup=chat_kb())
        return
    if await ready_gate(m, state):
        await start_search(m.from_user.id, m.chat.id)


@router.message(F.text == B_CANCEL)
async def h_cancel(m: Message):
    uid = m.from_user.id
    if cancel_search(uid):
        await m.answer("❌ Поиск отменён.", reply_markup=main_kb(uid))
    else:
        await m.answer("🏠 Главное меню 👇", reply_markup=main_kb(uid))


@router.message(F.text == B_NEXT)
@router.message(Command("next"))
async def h_next(m: Message, state: FSMContext):
    uid = m.from_user.id
    if uid in queue:
        return
    if uid in partner:
        pid = finish_chat(uid)
        bg(post_chat(pid, "😔 Собеседник завершил диалог."))
        await start_search(uid, m.chat.id)
    elif await ready_gate(m, state):
        await start_search(uid, m.chat.id)


@router.message(F.text == B_STOP)
@router.message(Command("stop"))
async def h_stop(m: Message):
    uid = m.from_user.id
    if uid in partner:
        await end_chat_both(uid)
    elif cancel_search(uid):
        await m.answer("❌ Поиск отменён.", reply_markup=main_kb(uid))
    else:
        await m.answer("🏠 Главное меню 👇", reply_markup=main_kb(uid))


def reasons_kb():
    rows = [[btn(v, f"rr:{k}")] for k, v in REASONS.items() if k != "profile"]
    rows.append([btn("↩️ Отмена", "cl")])
    return ikb(rows)


@router.message(F.text == B_REPORT)
async def h_report(m: Message):
    uid = m.from_user.id
    if partner.get(uid) or last_partner.get(uid):
        await m.answer("🚩 <b>Что случилось?</b>\nВыберите причину — собеседник будет заблокирован для вас.",
                       reply_markup=reasons_kb())
    else:
        await m.answer("Пока не на кого жаловаться.")


@router.callback_query(F.data == "rpt")
async def rpt_cb(cb: CallbackQuery):
    await cb.answer()
    await cb_edit(cb, "🚩 <b>Что случилось?</b>\nВыберите причину — собеседник будет заблокирован для вас.",
                  reasons_kb())


@router.callback_query(F.data.startswith("rr:"))
async def rr_cb(cb: CallbackQuery):
    await cb.answer("🚩 Жалоба отправлена")
    uid = cb.from_user.id
    target = partner.get(uid) or last_partner.get(uid)
    if not target:
        await cb_edit(cb, "Собеседник не найден.")
        return
    code = cb.data[3:]
    in_chat = uid in partner
    bg(file_report(uid, target, code))
    await cb_edit(cb, "✅ Жалоба отправлена, собеседник заблокирован. Спасибо!")
    if in_chat:
        await end_chat_both(uid, "⛔ Диалог завершён.", "😔 Собеседник завершил диалог.")


@router.message(F.text == B_BLOCK)
async def h_block(m: Message):
    uid = m.from_user.id
    target = partner.get(uid) or last_partner.get(uid)
    if not target:
        await m.answer("Пока некого блокировать.")
        return
    do_block(uid, target)
    if uid in partner:
        await end_chat_both(uid, "🚫 Собеседник заблокирован.", "😔 Собеседник завершил диалог.")
    else:
        await m.answer("🚫 Собеседник заблокирован.", reply_markup=main_kb(uid))


@router.callback_query(F.data == "blk")
async def blk_cb(cb: CallbackQuery):
    uid = cb.from_user.id
    target = last_partner.get(uid)
    if not target:
        await cb.answer("Некого блокировать")
        return
    do_block(uid, target)
    await cb.answer("🚫 Заблокирован")
    await cb_edit(cb, "🚫 Собеседник заблокирован. Вы больше не встретитесь.")


@router.callback_query(F.data == "sr")
async def sr_cb(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    uid = cb.from_user.id
    if uid in partner or uid in queue:
        return
    u = users[uid]
    if not u.ready:
        await onboarding(cb.message, state, u)
        return
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    await start_search(uid, cb.message.chat.id)


@router.callback_query(F.data == "cl")
async def close_cb(cb: CallbackQuery):
    await cb.answer()
    view.pop(cb.from_user.id, None)
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))


# ── анкеты ──
@router.message(F.text == B_PROFILES)
async def h_profiles(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    if uid in partner:
        await m.answer("💬 Сначала завершите диалог («⛔ Стоп»).", reply_markup=chat_kb())
        return
    if uid in queue:
        await m.answer("🔎 Идёт поиск. Нажмите «❌ Отменить поиск», чтобы смотреть анкеты.")
        return
    if await ready_gate(m, state):
        await show_card(uid, m.chat.id, "browse")


@router.callback_query(F.data.regexp(r"^(lk|nx|rp):\d+$"))
async def card_action(cb: CallbackQuery):
    act, seq = cb.data.split(":")
    uid = cb.from_user.id
    v = view.get(uid)
    if not v or v["seq"] != int(seq):
        await cb.answer("Анкета устарела — откройте заново")
        return
    t, mode = v["t"], v["mode"]
    if act == "lk":
        u = users[uid]
        if not is_prem(uid):
            if u.likes_day != today():
                u.likes_day, u.likes_used = today(), 0
            if u.likes_used >= FREE_LIKES:
                await cb.answer("Лимит лайков на сегодня исчерпан. 💎 Premium — безлимит!", show_alert=True)
                return
            u.likes_used += 1
            dirty.add(uid)
        await cb.answer("❤️")
        mutual = add_like(uid, t)
        bg(notify_like(uid, t, mutual))
    elif act == "nx":
        await cb.answer()
    else:
        await cb.answer("🚩 Жалоба отправлена, анкета скрыта")
        bg(file_report(uid, t, "profile"))
    if mode == "likes":
        seen_likes.setdefault(uid, set()).add(t)
    await show_card(uid, cb.message.chat.id, mode, cb)


@router.callback_query(F.data == "rs")
async def rs_cb(cb: CallbackQuery):
    await cb.answer()
    seen[cb.from_user.id] = set()
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    await show_card(cb.from_user.id, cb.message.chat.id, "browse")


@router.message(F.text == B_LIKES)
async def h_likes(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    if uid in partner:
        await m.answer("💬 Сначала завершите диалог («⛔ Стоп»).", reply_markup=chat_kb())
        return
    if not await ready_gate(m, state):
        return
    if is_prem(uid):
        await show_card(uid, m.chat.id, "likes")
        return
    n = len(likers_of(uid))
    text = (f"❤️ <b>Вам поставили лайк: {n}</b>\n\nХотите узнать, кто это? Откройте 💎 Premium — "
            "вы увидите все анкеты, которым вы понравились." if n else
            "❤️ Пока никто не поставил вам лайк.\n\nС 💎 Premium вы будете видеть, кто лайкнул вашу анкету.")
    await m.answer(text, reply_markup=ikb([[btn(f"💎 Открыть за {PRICE} ⭐", "prem")], [btn("🏠 Закрыть", "cl")]]))


@router.callback_query(F.data == "lkv")
async def lkv_cb(cb: CallbackQuery):
    await cb.answer()
    uid = cb.from_user.id
    if is_prem(uid) and uid not in partner:
        await show_card(uid, cb.message.chat.id, "likes")


# ── приглашение в чат после взаимной симпатии ──
@router.callback_query(F.data.startswith("iv:"))
async def invite_cb(cb: CallbackQuery):
    rec = tokens.get(cb.data[3:])
    me = cb.from_user.id
    if not rec or me not in rec[:2]:
        await cb.answer("Приглашение устарело", show_alert=True)
        return
    other = rec[1] if me == rec[0] else rec[0]
    if me in partner or me in queue:
        await cb.answer("Вы уже в диалоге или в поиске", show_alert=True)
        return
    ou = users.get(other)
    if not ou or ou.banned or other in partner or other in queue:
        await cb.answer("Пользователь сейчас занят. Попробуйте позже.", show_alert=True)
        return
    await cb.answer("📨 Приглашение отправлено")
    tok = cb.data[3:]
    await send(other, "💬 <b>Вам предлагают анонимный чат</b>\nУ вас взаимная симпатия 💘",
               reply_markup=ikb([[btn("✅ Принять", f"ia:{tok}"), btn("❌ Отказать", f"id:{tok}")]]))


@router.callback_query(F.data.startswith("ia:"))
async def invite_accept(cb: CallbackQuery):
    rec = tokens.get(cb.data[3:])
    me = cb.from_user.id
    if not rec or me not in rec[:2]:
        await cb.answer("Приглашение устарело", show_alert=True)
        return
    other = rec[1] if me == rec[0] else rec[0]
    if me in partner or other in partner:
        await cb.answer("Кто-то уже в другом диалоге", show_alert=True)
        return
    await cb.answer("✅")
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    cancel_search(me)
    cancel_search(other)
    await connect(me, other)


@router.callback_query(F.data.startswith("id:"))
async def invite_decline(cb: CallbackQuery):
    await cb.answer("Отклонено")
    bg(safe_delete(cb.message.chat.id, cb.message.message_id))


# ── премиум ──
@router.message(F.text == B_PREMIUM)
async def h_prem(m: Message, state: FSMContext):
    await state.clear()
    text, kb = prem_view(m.from_user.id)
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data == "prem")
async def prem_cb(cb: CallbackQuery):
    await cb.answer()
    text, kb = prem_view(cb.from_user.id)
    await cb_edit(cb, text, kb)


@router.callback_query(F.data == "buy")
async def buy_cb(cb: CallbackQuery):
    uid = cb.from_user.id
    if is_admin(uid):
        await cb.answer("У вас уже безлимитный доступ 👑", show_alert=True)
        return
    await cb.answer()
    try:
        await bot.send_invoice(
            chat_id=uid, title=f"💎 Premium на {PREM_DAYS} дней",
            description="Видите, кто вас лайкнул, поиск по полу и возрасту, приоритет в очереди, "
                        "фото и видео в чате, безлимитные лайки.",
            payload="prem30", provider_token="", currency="XTR",
            prices=[LabeledPrice(label=f"Premium {PREM_DAYS} дней", amount=PRICE)])
    except Exception as e:
        log.error("invoice failed: %s", e)
        await send(uid, f"⚠️ Не удалось открыть оплату. Напишите @{SUPPORT}", reply_markup=support_kb())


@router.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    u = users.get(q.from_user.id)
    ok = q.invoice_payload == "prem30" and not (u and u.banned)
    await q.answer(ok=ok, error_message=None if ok else "Платёж недоступен")


@router.message(F.successful_payment)
async def paid(m: Message):
    sp = m.successful_payment
    uid = m.from_user.id
    if sp.telegram_payment_charge_id in paid_ids:
        return
    paid_ids.add(sp.telegram_payment_charge_id)
    u = users.get(uid) or touch(m.from_user)
    await give_premium(uid, PREM_DAYS)
    db.enqueue(INS_PAY, sp.telegram_payment_charge_id, uid, sp.total_amount, now())
    await m.answer(f"🎉 <b>Premium активирован!</b>\n📅 Действует до: <b>{fmt_ts(u.premium_until)}</b>\n\n"
                   "Спасибо за поддержку! Все функции Premium уже доступны 💎", reply_markup=main_kb(uid))
    for a in all_admins():
        bg(send(a, f"💰 Покупка Premium: <code>{uid}</code> {('@' + esc(u.username)) if u.username else ''} — {sp.total_amount} ⭐"))


# ── помощь ──
@router.message(F.text == B_HELP)
@router.message(Command("help"))
async def h_help(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(
        f"🆘 <b>Помощь и поддержка</b>\n\n"
        "🔍 <b>Найти собеседника</b> — случайный анонимный чат.\n"
        "👤 <b>Анкеты</b> — листайте анкеты и ставьте ❤️. При взаимной симпатии можно начать чат.\n"
        "💎 <b>Премиум</b> — больше возможностей за " f"{PRICE} ⭐ в месяц.\n\n" + SAFETY + "\n\n"
        f"📩 По вопросам <b>разбана, рекламы</b> и любым другим пишите: @{SUPPORT}",
        reply_markup=support_kb())


# ───────────────────────── ПРОФИЛЬ ─────────────────────────
def profile_view(u: U):
    status = (f"💎 Premium до {fmt_ts(u.premium_until)}" if u.premium_until > now() and not is_admin(u.id)
              else "👑 Администратор" if is_admin(u.id) else "🆓 Бесплатный аккаунт")
    text = (f"⚙️ <b>Мой профиль</b>\n\n{gl(u.gender)}, {u.age}\n"
            f"📍 Город: {esc(u.city) or '—'}\n✍️ О себе: {esc(u.about) or '—'}\n"
            f"📷 Фото: {'есть' if u.photo else 'нет'}\n"
            f"👁 Анкета: {'показывается' if u.visible else 'скрыта'}\n\n{status}\n"
            f"🎯 Кого искать: {PREF_LABEL[u.pref]}   🎂 Возраст: {u.amin}–{u.amax}")
    kb = ikb([[btn("🏙 Город", "pf:city"), btn("✍️ О себе", "pf:about")],
              [btn("📷 Фото", "pf:photo"), btn("🎂 Мой возраст", "pf:age")],
              [btn("🎯 Кого искать 💎", "pf:pref"), btn("🔢 Фильтр возраста 💎", "pf:agef")],
              [btn("👁 Скрыть анкету" if u.visible else "👁 Показать анкету", "pf:vis")],
              [btn("👀 Как видят мою анкету", "pf:prev")],
              [btn("🏠 Закрыть", "cl")]])
    return text, kb


@router.message(F.text == B_PROFILE)
async def h_profile(m: Message, state: FSMContext):
    await state.clear()
    if await ready_gate(m, state):
        text, kb = profile_view(users[m.from_user.id])
        await m.answer(text, reply_markup=kb)


PROMPTS = {
    "city": ("🏙 Напишите ваш город (до 40 символов)", Edit.city),
    "about": ("✍️ Расскажите о себе (до 300 символов)", Edit.about),
    "photo": ("📷 Пришлите ваше фото одним сообщением", Edit.photo),
    "age": ("🎂 Напишите ваш возраст (18–99)", Edit.age),
    "agef": ("🔢 Напишите диапазон возраста, например: <b>20-30</b>", Edit.agef),
}


@router.callback_query(F.data.startswith("pf:"))
async def pf_cb(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    u = users[uid]
    act = cb.data.split(":")
    key = act[1]
    if key in ("pref", "agef") and not is_prem(uid):
        await cb.answer("💎 Это функция Premium", show_alert=True)
        return
    await cb.answer()
    if key in PROMPTS:
        text, st = PROMPTS[key]
        await state.set_state(st)
        rows = [[btn("❌ Отмена", "pf:cancel")]]
        if key in ("city", "about", "photo"):
            rows[0].insert(0, btn("🗑 Очистить", f"pf:clr:{key}"))
        await cb.message.answer(text, reply_markup=ikb(rows))
        return
    if key == "cancel":
        await state.clear()
        bg(safe_delete(cb.message.chat.id, cb.message.message_id))
        return
    if key == "clr":
        setattr(u, act[2], "")
        await state.clear()
        bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    elif key == "pref":
        u.pref = {"any": "f", "f": "m", "m": "any"}[u.pref]
        _pool["ts"] = 0
    elif key == "vis":
        u.visible = not u.visible
        _pool["ts"] = 0
    elif key == "prev":
        await send_card(cb.message.chat.id, u, "👀 <b>Так вас видят другие:</b>\n\n" + card_text(u))
        return
    dirty.add(uid)
    text, kb = profile_view(u)
    await cb_edit(cb, text, kb) if key in ("pref", "vis") else await cb.message.answer(text, reply_markup=kb)


@router.message(Edit.city, F.text)
async def e_city(m: Message, state: FSMContext):
    u = users[m.from_user.id]
    u.city = m.text.strip()[:40]
    await _edit_done(m, state, u)


@router.message(Edit.about, F.text)
async def e_about(m: Message, state: FSMContext):
    u = users[m.from_user.id]
    u.about = m.text.strip()[:300]
    await _edit_done(m, state, u)


@router.message(Edit.photo, F.photo)
async def e_photo(m: Message, state: FSMContext):
    u = users[m.from_user.id]
    u.photo = m.photo[-1].file_id
    await _edit_done(m, state, u)


@router.message(Edit.age, F.text)
async def e_age(m: Message, state: FSMContext):
    u = users[m.from_user.id]
    t = m.text.strip()
    if not t.isdigit() or not 18 <= int(t) <= 99:
        await m.answer("Введите возраст числом от 18 до 99.")
        return
    u.age = int(t)
    await _edit_done(m, state, u)


@router.message(Edit.agef, F.text)
async def e_agef(m: Message, state: FSMContext):
    u = users[m.from_user.id]
    r = re.fullmatch(r"\s*(\d{2})\s*[-–—]\s*(\d{2})\s*", m.text)
    if not r or not 18 <= int(r.group(1)) <= int(r.group(2)) <= 99:
        await m.answer("Формат: <b>20-30</b> (от 18 до 99).")
        return
    u.amin, u.amax = int(r.group(1)), int(r.group(2))
    await _edit_done(m, state, u)


async def _edit_done(m: Message, state: FSMContext, u: U):
    await state.clear()
    dirty.add(u.id)
    _pool["ts"] = 0
    text, kb = profile_view(u)
    await m.answer("✅ Сохранено!")
    await m.answer(text, reply_markup=kb)


@router.message(Edit.photo)
@router.message(Edit.city)
@router.message(Edit.about)
@router.message(Edit.age)
@router.message(Edit.agef)
async def e_wrong(m: Message):
    await m.answer("Пожалуйста, отправьте данные в нужном формате или нажмите «❌ Отмена».")


# ───────────────────────── АДМИН-ПАНЕЛЬ ─────────────────────────
def adm_kb():
    return ikb([[btn("📊 Статистика", "adm:stats"), btn("🔎 Пользователь", "adm:find")],
                [btn("🚫 Забанить", "adm:ban"), btn("✅ Разбанить", "adm:unban")],
                [btn("💎 Выдать Premium", "adm:give"), btn("➖ Забрать Premium", "adm:take")],
                [btn("📢 Каналы (подписка)", "adm:ch"), btn("📣 Рассылка / реклама", "adm:bc")],
                [btn("👑 Админы", "adm:adm"), btn("🚩 Жалобы", "adm:rep")],
                [btn("❌ Закрыть", "adm:close")]])


def back_kb():
    return ikb([[btn("⬅️ В админ-панель", "adm:home")]])


ADM_TEXT = "🛠 <b>Админ-панель</b>\nВам доступно всё бесплатно и без ограничений."


@router.message(F.text == B_ADMIN)
@router.message(Command("admin"))
async def h_admin(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    await state.clear()
    await m.answer(ADM_TEXT, reply_markup=adm_kb())


def stats_text():
    t = now()
    us = list(users.values())
    return ("📊 <b>Статистика</b>\n\n"
            f"👥 Всего: {len(us)}\n🆕 За 24 ч: {sum(1 for u in us if t - u.registered < 86400)}\n"
            f"🟢 Активны за 24 ч: {sum(1 for u in us if t - u.last_seen < 86400)}\n"
            f"✅ С анкетой: {sum(1 for u in us if u.ready)}\n"
            f"💎 Premium: {sum(1 for u in us if u.premium_until > t)}\n"
            f"🚫 Забанены: {sum(1 for u in us if u.banned)}\n"
            f"💬 В чатах: {len(partner) // 2} пар\n🔎 В поиске: {len(queue)}\n"
            f"❤️ Лайков: {sum(len(s) for s in likes_from.values())}\n"
            f"📢 Каналов ОП: {len(CHANNELS)}")


def resolve(token: str):
    token = (token or "").strip()
    if token.lstrip("-").isdigit():
        return users.get(int(token))
    name = token.lstrip("@").lower()
    for u in users.values():
        if u.username and u.username.lower() == name:
            return u
    return None


def user_info(u: U):
    t = now()
    prem = (f"до {fmt_ts(u.premium_until)}" if u.premium_until > t else "нет")
    text = (f"👤 <b>Пользователь</b> <code>{u.id}</code>\n"
            f"Username: {('@' + esc(u.username)) if u.username else '—'}\n"
            f"Пол/возраст: {gl(u.gender) if u.gender else '—'}, {u.age or '—'}\n"
            f"Город: {esc(u.city) or '—'}\nPremium: {prem}\n"
            f"Бан: {('да — ' + esc(u.ban_reason)) if u.banned else 'нет'}\n"
            f"Чатов: {u.chats} · Жалоб: {len(reporters.get(u.id, ()))}\n"
            f"Регистрация: {fmt_ts(u.registered) if u.registered else '—'}")
    kb = ikb([[btn("✅ Разбанить" if u.banned else "🚫 Забанить",
                   f"adm:{'qunban' if u.banned else 'qban'}:{u.id}")],
              [btn("💎 +30 дней", f"adm:qgive:{u.id}:30"), btn("💎 +365", f"adm:qgive:{u.id}:365")],
              [btn("➖ Забрать Premium", f"adm:qtake:{u.id}")],
              [btn("⬅️ В админ-панель", "adm:home")]])
    return text, kb


ADM_PROMPTS = {
    "find": "🔎 Отправьте ID или @username пользователя",
    "ban": "🚫 Отправьте ID или @username (можно с причиной): <code>123456 спам</code>",
    "unban": "✅ Отправьте ID или @username для разбана",
    "give": "💎 Отправьте ID и число дней: <code>123456 30</code>",
    "take": "➖ Отправьте ID или @username — Premium будет отключён",
    "addch": ("📢 Отправьте канал: <code>@channel</code> или <code>-100123456789</code>.\n"
              "Для закрытого канала добавьте ссылку: <code>-100123456789 https://t.me/+abc</code>\n\n"
              "⚠️ Бот должен быть <b>администратором</b> канала."),
    "addadm": "👑 Отправьте ID нового администратора",
}


async def adm_edit(cb, text, kb):
    await cb_edit(cb, text, kb)


@router.callback_query(F.data.startswith("adm:"))
async def adm_cb(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.answer()
    p = cb.data.split(":")
    act = p[1]
    if act == "home":
        await state.clear()
        await adm_edit(cb, ADM_TEXT, adm_kb())
    elif act == "close":
        await state.clear()
        bg(safe_delete(cb.message.chat.id, cb.message.message_id))
    elif act == "stats":
        await adm_edit(cb, stats_text(), back_kb())
    elif act in ADM_PROMPTS:
        await state.set_state(AdminSt.inp)
        await state.update_data(act=act)
        await adm_edit(cb, ADM_PROMPTS[act], back_kb())
    elif act == "info":
        u = users.get(int(p[2]))
        if u:
            text, kb = user_info(u)
            await cb.message.answer(text, reply_markup=kb)
    elif act == "ch":
        rows = [[btn(f"❌ {c['title']}", f"adm:chdel:{c['chat_id']}")] for c in CHANNELS]
        rows.append([btn("➕ Добавить канал", "adm:addch")])
        rows.append([btn("⬅️ В админ-панель", "adm:home")])
        await adm_edit(cb, "📢 <b>Каналы обязательной подписки</b>\n"
                           "Пользователи не смогут пользоваться ботом, пока не подпишутся.\n"
                           "Нажмите на канал, чтобы удалить.", ikb(rows))
    elif act == "chdel":
        cid = int(p[2])
        CHANNELS[:] = [c for c in CHANNELS if c["chat_id"] != cid]
        sub_ok.clear()
        db.enqueue("DELETE FROM channels WHERE chat_id=$1", cid)
        cb.data = "adm:ch"
        await adm_cb(cb, state)
    elif act == "adm":
        if uid != MAIN_ADMIN:
            await cb.message.answer("Управлять админами может только главный администратор.")
            return
        rows = [[btn(f"❌ {a}", f"adm:rmadm:{a}")] for a in sorted(admins)]
        rows.append([btn("➕ Добавить админа", "adm:addadm")])
        rows.append([btn("⬅️ В админ-панель", "adm:home")])
        await adm_edit(cb, f"👑 <b>Администраторы</b>\nГлавный: <code>{MAIN_ADMIN}</code>\n"
                           "Нажмите на ID, чтобы убрать админа.", ikb(rows))
    elif act == "rmadm" and uid == MAIN_ADMIN:
        a = int(p[2])
        admins.discard(a)
        db.enqueue("DELETE FROM admins WHERE user_id=$1", a)
        cb.data = "adm:adm"
        await adm_cb(cb, state)
    elif act == "rep":
        rows = await db.fetch("SELECT from_id,to_id,reason,created FROM reports ORDER BY id DESC LIMIT 10") or []
        text = "🚩 <b>Последние жалобы</b>\n\n" + ("\n".join(
            f"• <code>{r['from_id']}</code> → <code>{r['to_id']}</code> · "
            f"{esc(REASONS.get(r['reason'], r['reason']))} · {fmt_ts(r['created'])}" for r in rows) or "Жалоб нет.")
        await adm_edit(cb, text, back_kb())
    elif act in ("qban", "qunban", "qgive", "qtake"):
        t = users.get(int(p[2]))
        if not t:
            await cb.message.answer("Пользователь не найден.")
            return
        if act == "qban":
            if is_admin(t.id):
                await cb.message.answer("Админа забанить нельзя.")
                return
            await ban_user(t.id, "Решение администрации")
            res = "🚫 Забанен"
        elif act == "qunban":
            await unban_user(t.id)
            res = "✅ Разбанен"
        elif act == "qgive":
            await give_premium(t.id, int(p[3]))
            bg(send(t.id, f"🎁 Вам выдан 💎 Premium на {p[3]} дн.!"))
            res = f"💎 Выдан Premium на {p[3]} дн."
        else:
            t.premium_until = 0
            dirty.add(t.id)
            res = "➖ Premium отключён"
        text, kb = user_info(t)
        await cb.message.answer(f"{res}\n\n{text}", reply_markup=kb)
    elif act == "bc":
        await adm_edit(cb, "📣 <b>Рассылка / реклама</b>\nКому отправить?",
                       ikb([[btn("👥 Всем", "adm:bca:all")],
                            [btn("🆓 Бесплатным", "adm:bca:free"), btn("💎 Premium", "adm:bca:prem")],
                            [btn("⬅️ В админ-панель", "adm:home")]]))
    elif act == "bca":
        await state.set_state(AdminSt.bc_msg)
        await state.update_data(aud=p[2])
        await adm_edit(cb, "✉️ Отправьте сообщение для рассылки (текст, фото, видео — как есть).", back_kb())
    elif act == "bcskip":
        await bc_preview(cb.message.chat.id, state)
    elif act == "bcgo":
        await bc_go(cb.message.chat.id, state)
    elif act == "bcx":
        await state.clear()
        await adm_edit(cb, "❌ Рассылка отменена.", back_kb())


@router.message(AdminSt.inp)
async def adm_input(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    act = (await state.get_data()).get("act")
    txt = (m.text or "").strip()
    await state.clear()
    if act == "addch":
        await add_channel(m, txt)
        return
    if act == "addadm":
        if m.from_user.id != MAIN_ADMIN:
            await m.answer("Только главный администратор может добавлять админов.", reply_markup=back_kb())
            return
        if not txt.isdigit():
            await m.answer("Нужен числовой ID.", reply_markup=back_kb())
            return
        admins.add(int(txt))
        db.enqueue("INSERT INTO admins(user_id) VALUES($1) ON CONFLICT DO NOTHING", int(txt))
        bg(send(int(txt), "👑 Вас назначили администратором бота. Нажмите /start",
                reply_markup=main_kb(int(txt))))
        await m.answer(f"✅ Администратор <code>{txt}</code> добавлен.", reply_markup=back_kb())
        return
    parts = txt.split(maxsplit=1)
    t = resolve(parts[0]) if parts else None
    if not t:
        await m.answer("❌ Пользователь не найден (он должен был запускать бота).", reply_markup=back_kb())
        return
    if act == "ban":
        if is_admin(t.id):
            await m.answer("Админа забанить нельзя.", reply_markup=back_kb())
            return
        await ban_user(t.id, parts[1] if len(parts) > 1 else "Решение администрации")
    elif act == "unban":
        await unban_user(t.id)
    elif act == "give":
        days = int(parts[1]) if len(parts) > 1 and parts[1].strip().isdigit() else PREM_DAYS
        await give_premium(t.id, days)
        bg(send(t.id, f"🎁 Вам выдан 💎 Premium на {days} дн.!"))
    elif act == "take":
        t.premium_until = 0
        dirty.add(t.id)
        bg(send(t.id, "ℹ️ Ваш Premium был отключён администратором."))
    text, kb = user_info(t)
    await m.answer("✅ Готово.\n\n" + text, reply_markup=kb)


async def add_channel(m: Message, txt: str):
    parts = txt.split()
    if not parts:
        await m.answer("Пусто.", reply_markup=back_kb())
        return
    ref = parts[0]
    link = parts[1] if len(parts) > 1 else ""
    if ref.lstrip("-").isdigit():
        ref = int(ref)
    elif not ref.startswith("@"):
        ref = "@" + ref
    try:
        chat = await bot.get_chat(ref)
        me = await bot.get_chat_member(chat.id, bot.id)
    except Exception as e:
        await m.answer(f"❌ Не удалось найти канал: {esc(e)}\nДобавьте бота в канал администратором.",
                       reply_markup=back_kb())
        return
    if me.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
        await m.answer("❌ Сделайте бота <b>администратором</b> канала и повторите.", reply_markup=back_kb())
        return
    if not link:
        link = f"https://t.me/{chat.username}" if chat.username else (getattr(chat, "invite_link", "") or "")
    if not link:
        await m.answer("❌ Для закрытого канала добавьте ссылку-приглашение после ID.", reply_markup=back_kb())
        return
    title = (chat.title or "Канал")[:40]
    CHANNELS[:] = [c for c in CHANNELS if c["chat_id"] != chat.id] + [
        {"chat_id": chat.id, "title": title, "link": link}]
    sub_ok.clear()
    db.enqueue("INSERT INTO channels(chat_id,title,link) VALUES($1,$2,$3) "
               "ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title, link=EXCLUDED.link",
               chat.id, title, link)
    await m.answer(f"✅ Канал «{esc(title)}» добавлен. Теперь подписка обязательна.", reply_markup=back_kb())


# ── рассылка ──
@router.message(AdminSt.bc_msg)
async def bc_msg(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    await state.update_data(src_chat=m.chat.id, src_mid=m.message_id)
    await state.set_state(AdminSt.bc_btn)
    await m.answer("🔘 Добавить кнопку-ссылку? Отправьте: <code>Текст | https://ссылка</code>\nили нажмите «Пропустить».",
                   reply_markup=ikb([[btn("⏭ Пропустить", "adm:bcskip")]]))


@router.message(AdminSt.bc_btn)
async def bc_btn(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    r = re.fullmatch(r"(.{1,60}?)\s*\|\s*((?:https?://|tg://)\S+)", (m.text or "").strip())
    if not r:
        await m.answer("Формат: <code>Текст | https://ссылка</code>")
        return
    await state.update_data(btn=[r.group(1), r.group(2)])
    await bc_preview(m.chat.id, state)


def bc_markup(data):
    b = data.get("btn")
    return ikb([[btn(b[0], url=b[1])]]) if b else None


def bc_recipients(aud):
    t = now()
    out = []
    for u in users.values():
        if u.banned or not u.alive or not u.ready:
            continue
        pr = u.premium_until > t or is_admin(u.id)
        if aud == "free" and pr or aud == "prem" and not pr:
            continue
        out.append(u.id)
    return out


async def bc_preview(chat_id, state: FSMContext):
    d = await state.get_data()
    if not d.get("src_mid"):
        await send(chat_id, "Сначала отправьте сообщение для рассылки.")
        return
    await state.set_state(None)
    n = len(bc_recipients(d.get("aud", "all")))
    try:
        await bot.copy_message(chat_id, d["src_chat"], d["src_mid"], reply_markup=bc_markup(d))
    except Exception as e:
        await send(chat_id, f"Ошибка предпросмотра: {esc(e)}")
        return
    await send(chat_id, f"👆 Так будет выглядеть реклама.\nПолучателей: <b>{n}</b>",
               reply_markup=ikb([[btn("🚀 Отправить", "adm:bcgo"), btn("❌ Отмена", "adm:bcx")]]))


async def bc_go(chat_id, state: FSMContext):
    global broadcast_running
    d = await state.get_data()
    await state.clear()
    if broadcast_running:
        await send(chat_id, "⏳ Другая рассылка ещё идёт.")
        return
    if not d.get("src_mid"):
        await send(chat_id, "Нет сообщения для рассылки.")
        return
    bg(run_broadcast(chat_id, d["src_chat"], d["src_mid"], bc_markup(d), bc_recipients(d.get("aud", "all"))))


async def run_broadcast(admin_chat, src_chat, mid, markup, uids):
    global broadcast_running
    broadcast_running = True
    ok = fail = 0
    st = await send(admin_chat, f"📣 Рассылка запущена: 0/{len(uids)}")
    try:
        for i, t in enumerate(uids, 1):
            try:
                await bot.copy_message(t, src_chat, mid, reply_markup=markup)
                ok += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(min(e.retry_after, 30))
                try:
                    await bot.copy_message(t, src_chat, mid, reply_markup=markup)
                    ok += 1
                except Exception:
                    fail += 1
            except TelegramForbiddenError:
                if t in users:
                    users[t].alive = False
                    dirty.add(t)
                fail += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.04)
            if st and i % 200 == 0:
                bg(edit_safe(admin_chat, st.message_id, f"📣 Рассылка: {i}/{len(uids)}"))
    finally:
        broadcast_running = False
    await send(admin_chat, f"✅ <b>Рассылка завершена</b>\nДоставлено: {ok}\nНе доставлено: {fail}")


# ───────────────────────── ПЕРЕСЫЛКА СООБЩЕНИЙ В ЧАТЕ ─────────────────────────
FREE_TYPES = {ContentType.TEXT, ContentType.STICKER}
PREM_TYPES = FREE_TYPES | {ContentType.PHOTO, ContentType.VIDEO, ContentType.VOICE,
                           ContentType.VIDEO_NOTE, ContentType.ANIMATION, ContentType.AUDIO,
                           ContentType.DOCUMENT}


def flooded(uid) -> bool:
    d = flood.setdefault(uid, deque(maxlen=10))
    t = time.time()
    d.append(t)
    return len(d) == 10 and t - d[0] < 4


@router.message(F.func(lambda m: m.from_user and m.from_user.id in partner))
async def relay(m: Message):
    uid = m.from_user.id
    pid = partner.get(uid)
    if pid is None:
        return
    if flooded(uid):
        return
    prem = is_prem(uid)
    ct = m.content_type
    if ct not in PREM_TYPES:
        if not throttled(hint_ts, uid, 5):
            await m.answer("🚫 Этот тип сообщений в анонимном чате запрещён "
                           "(контакты, геолокация и опросы не передаются).")
        return
    if not prem and (ct not in FREE_TYPES or (ct == ContentType.TEXT and URL_RE.search(m.text or ""))):
        if not throttled(hint_ts, uid, 5):
            what = "Ссылки" if ct == ContentType.TEXT else "Фото, видео и голосовые"
            await m.answer(f"🔒 {what} доступны только в 💎 Premium.",
                           reply_markup=ikb([[btn("💎 Открыть Premium", "prem")]]))
        return
    try:
        await m.copy_to(pid, protect_content=True)
    except TelegramForbiddenError:
        users[pid].alive = False
        dirty.add(pid)
        await end_chat_both(uid, "😔 Собеседник покинул чат.", "")
    except TelegramRetryAfter as e:
        await asyncio.sleep(min(e.retry_after, 5))
        try:
            await m.copy_to(pid, protect_content=True)
        except Exception:
            pass
    except Exception as e:
        log.warning("relay failed: %s", e)
        await m.answer("⚠️ Не удалось доставить сообщение.")


@router.message(F.chat.type == "private")
async def fallback(m: Message, state: FSMContext):
    uid = m.from_user.id
    if uid in queue:
        await m.answer("🔎 Идёт поиск собеседника…\nНажмите «❌ Отменить поиск», чтобы остановиться.",
                       reply_markup=search_kb())
        return
    u = users[uid]
    if not u.ready:
        await onboarding(m, state, u)
        return
    await m.answer("Выберите действие в меню 👇", reply_markup=main_kb(uid))


# ───────────────────────── ФОН / ЗАПУСК ─────────────────────────
async def housekeeper():
    last_exp = now()
    tick = 0
    while True:
        await asyncio.sleep(30)
        tick += 1
        try:
            _pool["ts"] = 0
            t = time.time()
            for k in [k for k, v in tokens.items() if v[2] < t]:
                tokens.pop(k, None)
            for k in [k for k, v in recent.items() if k not in partner and k not in queue][:0]:
                pass
            if tick % 6 == 0:   # раз в 3 минуты держим Neon «тёплым»
                await db.fetch("SELECT 1")
            if tick % 20 == 0:  # раз в 10 минут — уведомления об окончании Premium
                n = now()
                for u in list(users.values()):
                    if last_exp < u.premium_until <= n and not is_admin(u.id):
                        bg(send(u.id, "⌛ <b>Ваш Premium закончился.</b>\nПродлить можно в разделе «💎 Премиум».",
                                reply_markup=ikb([[btn("💎 Продлить", "prem")]])))
                last_exp = n
        except Exception as e:
            log.error("housekeeper: %s", e)


async def health_server():
    app = web.Application()

    async def ok(_):
        return web.Response(text="OK")
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def on_error(event):
    log.error("Update error: %r", event.exception, exc_info=event.exception)
    return True


async def main():
    global bot
    if not BOT_TOKEN or not DATABASE_URL:
        raise SystemExit("Укажите переменные окружения BOT_TOKEN и DATABASE_URL")
    await db.connect()
    await load_all()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(Guard())
    dp.callback_query.outer_middleware(Guard())
    dp.include_router(router)
    dp.errors.register(on_error)
    bg(db.writer())
    bg(flusher())
    bg(housekeeper())
    await health_server()
    await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    log.info("Бот запущен: @%s", me.username)
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query", "pre_checkout_query"])
    finally:
        await flush_dirty()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
