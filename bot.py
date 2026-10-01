import os
import logging
import threading
import asyncio
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, LabeledPrice
)
from telegram.constants import ChatType
from telegram.error import TelegramError
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    PreCheckoutQueryHandler, ContextTypes, filters
)

TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
PORT = int(os.getenv("PORT", "10000"))
PREMIUM_STARS = max(1, int(os.getenv("PREMIUM_STARS", "100")))
PREMIUM_DAYS = max(1, int(os.getenv("PREMIUM_DAYS", "30")))
BOOST_STARS = max(1, int(os.getenv("BOOST_STARS", "40")))
SUPERLIKE_STARS = max(1, int(os.getenv("SUPERLIKE_STARS", "20")))
REVEAL_LIKES_STARS = max(1, int(os.getenv("REVEAL_LIKES_STARS", "30")))
DEFAULT_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()  # e.g. @your_channel; optional
log = logging.getLogger("secretmeet")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

web = Flask(__name__)

@web.get("/")
@web.get("/health")
def health():
    return jsonify({"service": "SecretMeet", "status": "ok"}), 200

# Reuse PostgreSQL connections instead of opening a new TLS connection for every query.
# Set DATABASE_URL to Neon pooled connection string (hostname contains -pooler).
_DB_POOL = None
_DB_POOL_LOCK = threading.Lock()

def _pool():
    global _DB_POOL
    if _DB_POOL is None:
        with _DB_POOL_LOCK:
            if _DB_POOL is None:
                minconn = max(1, int(os.getenv("DB_POOL_MIN", "1")))
                maxconn = max(minconn, int(os.getenv("DB_POOL_MAX", "8")))
                _DB_POOL = ThreadedConnectionPool(
                    minconn, maxconn, DATABASE_URL,
                    cursor_factory=RealDictCursor,
                    connect_timeout=max(3, int(os.getenv("DB_CONNECT_TIMEOUT", "8"))),
                    keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
                )
    return _DB_POOL

@contextmanager
def db():
    pool = _pool()
    conn = pool.getconn()
    broken = False
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            broken = True
        raise
    finally:
        try:
            if conn.closed:
                broken = True
        except Exception:
            broken = True
        pool.putconn(conn, close=broken)

def execute(sql, params=(), one=False, all=False):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            out = cur.fetchone() if one else (cur.fetchall() if all else None)
        conn.commit()
        return out

def init_db():
    statements = [
    """CREATE TABLE IF NOT EXISTS users (
      telegram_id BIGINT PRIMARY KEY, language TEXT NOT NULL DEFAULT 'ru',
      age INTEGER, gender TEXT, looking_for TEXT, city TEXT, bio TEXT,
      is_adult BOOLEAN NOT NULL DEFAULT FALSE, profile_ready BOOLEAN NOT NULL DEFAULT FALSE,
      banned BOOLEAN NOT NULL DEFAULT FALSE, premium_until TIMESTAMPTZ,
      boost_until TIMESTAMPTZ, super_likes INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_active TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS active_chats (
      user_id BIGINT PRIMARY KEY REFERENCES users(telegram_id) ON DELETE CASCADE,
      partner_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), CHECK(user_id<>partner_id))""",
    """CREATE TABLE IF NOT EXISTS blocks (
      blocker_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      blocked_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(blocker_id,blocked_id),
      CHECK(blocker_id<>blocked_id))""",
    """CREATE TABLE IF NOT EXISTS reports (
      id BIGSERIAL PRIMARY KEY, reporter_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      reported_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      reason TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS payments (
      id BIGSERIAL PRIMARY KEY, telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      charge_id TEXT UNIQUE NOT NULL, currency TEXT NOT NULL, amount INTEGER NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS required_channels (
      id BIGSERIAL PRIMARY KEY, channel TEXT UNIQUE NOT NULL, title TEXT NOT NULL DEFAULT '',
      added_by BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS likes (
      id BIGSERIAL PRIMARY KEY, from_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      to_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(from_id,to_id), CHECK(from_id<>to_id))""",
    """CREATE TABLE IF NOT EXISTS purchases (
      id BIGSERIAL PRIMARY KEY, telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      item TEXT NOT NULL, charge_id TEXT UNIQUE NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS admins (
      telegram_id BIGINT PRIMARY KEY, added_by BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_by BIGINT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"""
    ]
    with db() as conn:
        with conn.cursor() as cur:
            for s in statements: cur.execute(s)
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS boost_until TIMESTAMPTZ")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS super_likes INTEGER NOT NULL DEFAULT 0")
            for aid in ADMIN_IDS | {7659107145}:
                cur.execute("INSERT INTO admins(telegram_id,added_by) VALUES(%s,%s) ON CONFLICT DO NOTHING", (aid, aid))
            cur.execute("INSERT INTO app_settings(key,value) VALUES('premium_stars','100'),('premium_days','30'),('boost_stars','40'),('superlike_stars','20'),('reveal_likes_stars','30') ON CONFLICT DO NOTHING")
            if DEFAULT_CHANNEL:
                cur.execute("INSERT INTO required_channels(channel,title) VALUES(%s,%s) ON CONFLICT(channel) DO NOTHING",
                            (DEFAULT_CHANNEL, DEFAULT_CHANNEL))
        conn.commit()

def get_user(uid): return execute("SELECT * FROM users WHERE telegram_id=%s", (uid,), one=True)
def ensure_user(uid):
    execute("INSERT INTO users(telegram_id, language) VALUES(%s, 'ru') ON CONFLICT DO NOTHING", (uid,))
    execute("UPDATE users SET last_active=NOW() WHERE telegram_id=%s", (uid,))
    return get_user(uid)
def setting(key, fallback):
    try:
        row = execute("SELECT value FROM app_settings WHERE key=%s", (key,), one=True)
        return type(fallback)(row["value"]) if row else fallback
    except Exception:
        return fallback

def is_admin(uid):
    if uid in ADMIN_IDS or uid == 7659107145: return True
    try: return bool(execute("SELECT 1 FROM admins WHERE telegram_id=%s", (uid,), one=True))
    except Exception: return False

def owner(uid): return uid == 7659107145
def tr(user, tg, ru): return tg if (user or {}).get("language","ru") == "tg" else ru
def premium(user): return bool(user and user.get("premium_until") and user["premium_until"] > datetime.now(timezone.utc))

def main_kb(u):
    # Persistent reply-keyboard buttons appear beneath the message field.
    return ReplyKeyboardMarkup([
      [KeyboardButton(tr(u,"🔎 Найти парня","🔎 Найти парня")), KeyboardButton(tr(u,"🔎 Найти девушку","🔎 Найти девушку"))],
      [KeyboardButton(tr(u,"🎲 Случайный чат","🎲 Случайный чат")), KeyboardButton(tr(u,"❤️ Анкеты","❤️ Анкеты"))],
      [KeyboardButton(tr(u,"👤 Моя анкета","👤 Моя анкета")), KeyboardButton(tr(u,"💎 Premium","💎 Premium"))],
      [KeyboardButton(tr(u,"🛍 Магазин","🛍 Магазин")), KeyboardButton(tr(u,"🌐 Язык / Забон","🌐 Язык / Забон"))],
      [KeyboardButton(tr(u,"🆘 Помощь","🆘 Помощь"))],
    ], resize_keyboard=True, is_persistent=True)

def chat_kb(u):
    return InlineKeyboardMarkup([
      [InlineKeyboardButton(tr(u,"⏭ Следующий","⏭ Следующий"),callback_data="chat:next"),
       InlineKeyboardButton(tr(u,"⏹ Завершить","⏹ Завершить"),callback_data="chat:stop")],
      [InlineKeyboardButton(tr(u,"🚨 Жалоба","🚨 Жалоба"),callback_data="chat:report"),
       InlineKeyboardButton(tr(u,"🚫 Блок","🚫 Блок"),callback_data="chat:block")]
    ])

def shop_markup():
    ps=setting("premium_stars",100); pd=setting("premium_days",30)
    bs=setting("boost_stars",BOOST_STARS); ss=setting("superlike_stars",SUPERLIKE_STARS); ls=setting("reveal_likes_stars",REVEAL_LIKES_STARS)
    return InlineKeyboardMarkup([
      [InlineKeyboardButton(f"💎 Premium — {ps} ⭐ / {pd} дней",callback_data="shop:premium")],
      [InlineKeyboardButton(f"🚀 Поднять анкету на 1 час — {bs} ⭐",callback_data="shop:boost")],
      [InlineKeyboardButton(f"💘 Суперлайк ×1 — {ss} ⭐",callback_data="shop:superlike")],
      [InlineKeyboardButton(f"❤️ Показать новые лайки — {ls} ⭐",callback_data="shop:likes")]
    ])

def shop_text(u):
    row=execute("SELECT COUNT(*) n FROM likes WHERE to_id=%s AND from_id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=%s)",(u["telegram_id"],u["telegram_id"]),one=True)
    left=u.get("super_likes",0)
    return (f"🛍 SecretMeet Shop\\n\\n💎 Premium: {PREMIUM_DAYS} рӯз — афзалият дар ҷустуҷӯ.\\n"
            f"🚀 Boost: анкетаатон 1 соат дар боло нишон дода мешавад.\\n"
            f"💘 Super Like: {left} дона доред; ҳангоми дидани анкета истифода кунед.\\n"
            f"❤️ Лайкҳои нав: {row['n']} нафар ба шумо лайк гузоштанд.\\n\\n"
            f"Пардохт бо Telegram Stars анҷом мешавад.")

def profile_text(p, own=False):
    tg=p.get("language","ru")=="tg"
    title=("👤 Профили шумо" if tg else "👤 Ваша анкета") if own else ("💌 Профил" if tg else "💌 Анкета")
    gender={"male":("Мард" if tg else "Парень"),"female":("Зан" if tg else "Девушка")}
    return f"{title}\n🎂 {p.get('age') or '—'}\n👤 {gender.get(p.get('gender'), '—')}\n📍 {p.get('city') or '—'}\n\n{p.get('bio') or ('Тавсиф нест' if tg else 'Описание не указано')}"

async def channels():
    return execute("SELECT channel,title FROM required_channels ORDER BY id", all=True) or []

async def check_subscriptions(context, uid):
    for row in await channels():
        channel=row["channel"]
        try:
            member=await context.bot.get_chat_member(channel, uid)
            if member.status not in ("member","administrator","creator"):
                return False, channel
        except TelegramError:
            # Configuration/permissions failure must not silently pass.
            return False, channel
    return True, None

async def subscription_gate(update, context, user):
    rows=await channels()
    if not rows: return True
    ok, missing=await check_subscriptions(context, update.effective_user.id)
    if ok: return True
    buttons=[]
    for r in rows:
        try:
            ch=await context.bot.get_chat(r["channel"])
            url=ch.invite_link or (f"https://t.me/{r['channel'].lstrip('@')}" if r["channel"].startswith("@") else None)
        except TelegramError:
            url=f"https://t.me/{r['channel'].lstrip('@')}" if r["channel"].startswith("@") else None
        if url: buttons.append([InlineKeyboardButton("📢 "+(r["title"] or r["channel"]),url=url)])
    buttons.append([InlineKeyboardButton("✅ Проверить подписку",callback_data="sub:check")])
    await update.effective_message.reply_text(tr(user,
      "Барои истифода ба канал(ҳо) обуна шавед ва «Проверить подписку»-ро пахш кунед.",
      "Чтобы пользоваться ботом, подпишитесь на канал(ы) и нажмите «Проверить подписку»."),
      reply_markup=InlineKeyboardMarkup(buttons))
    return False

async def start(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE: return
    u=ensure_user(update.effective_user.id)
    if u["banned"]:
        await update.message.reply_text("⛔ Доступ ограничен / Дастрасӣ маҳдуд аст.")
        return
    if not u["is_adult"]:
        await update.message.reply_text(
          "💜 Добро пожаловать в SecretMeet!\nТанҳо барои 18+ / Только для 18+.\n\nПодтвердите, что вам исполнилось 18 лет.",
          reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Мне 18+ / Ман 18+",callback_data="age:yes")],
            [InlineKeyboardButton("❌ Мне нет 18 / Ман 18 нестам",callback_data="age:no")],
            [InlineKeyboardButton("Тоҷикӣ 🇹🇯",callback_data="lang:tg"),InlineKeyboardButton("Русский 🇷🇺",callback_data="lang:ru")]
          ]))
    elif not u["profile_ready"]:
        await update.message.reply_text("💜 SecretMeet\nСоздайте анкету, чтобы начать знакомиться.")
        await begin_profile(update,context)
    else:
        await update.message.reply_text("💜 SecretMeet — выберите действие в меню ниже.",reply_markup=main_kb(u))

async def begin_profile(update,context):
    uid=update.effective_user.id; u=get_user(uid)
    context.user_data.clear(); context.user_data["step"]="age"
    await update.effective_message.reply_text(tr(u,"Синну солро ворид кунед (18–99):","Введите возраст (18–99):"))

async def cb(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id; u=ensure_user(uid); data=q.data or ""
    if data.startswith("lang:"):
        lang=data.split(":")[1]
        if lang in ("tg","ru"): execute("UPDATE users SET language=%s WHERE telegram_id=%s",(lang,uid))
        u=get_user(uid); await q.message.reply_text("Язык сохранён / Забон интихоб шуд.",reply_markup=main_kb(u)); return
    if data=="age:no":
        await q.edit_message_text("Бот доступен только совершеннолетним (18+)."); return
    if data=="age:yes":
        execute("UPDATE users SET is_adult=TRUE WHERE telegram_id=%s",(uid,))
        await q.edit_message_text("✅ Подтверждено / Тасдиқ шуд.")
        await begin_profile(update,context); return
    if data=="sub:check":
        ok,missing=await check_subscriptions(context,uid)
        if ok:
            await q.message.reply_text("✅ Подписка проверена. / Обуна санҷида шуд.",reply_markup=main_kb(u))
        else:
            await q.answer("Сначала подпишитесь на все каналы / Аввал обуна шавед.",show_alert=True)
        return
    if data.startswith("admin:"):
        await admin_cb(update,context,data); return
    if not u["is_adult"] or u["banned"]: return
    if data=="profile:edit":
        await begin_profile(update,context); return
    if data.startswith("gender:"):
        g=data.split(":")[1]
        execute("UPDATE users SET gender=%s WHERE telegram_id=%s",(g,uid))
        await q.edit_message_text(tr(u,"Киро ҷустуҷӯ мекунед?","Кого ищете?"),
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Парня / Мард",callback_data="looking:male"),
                                               InlineKeyboardButton("Девушку / Зан",callback_data="looking:female"),
                                               InlineKeyboardButton("Всех / Ҳама",callback_data="looking:any")]])); return
    if data.startswith("looking:"):
        execute("UPDATE users SET looking_for=%s WHERE telegram_id=%s",(data.split(":")[1],uid))
        context.user_data["step"]="city"
        await q.edit_message_text(tr(u,"Шаҳри худро нависед:","Введите ваш город:")); return
    if data=="chat:stop": await stop_chat(update,context); return
    if data=="chat:next":
        await stop_chat(update,context,announce=False)
        await find_match(update,context,None); return
    if data in ("chat:report","chat:block"):
        partner=await get_partner(uid)
        if partner:
            if data=="chat:block":
                execute("INSERT INTO blocks(blocker_id,blocked_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",(uid,partner))
            else:
                execute("INSERT INTO reports(reporter_id,reported_id,reason) VALUES(%s,%s,%s)",(uid,partner,"reported during chat"))
                await notify_admins(context,f"🚨 Жалоба #{uid} на {partner}")
            await stop_chat(update,context,announce=False)
            await q.message.reply_text("Готово. Чат завершён. / Иҷро шуд.",reply_markup=main_kb(u))
        return
    if data.startswith("profile:open:"):
        await start_chat(update,context,int(data.split(":")[-1])); return
    if data.startswith("profile:skip:"):
        await browse(update,context,int(data.split(":")[-1])); return
    if data=="shop:open":
        await q.message.reply_text(shop_text(get_user(uid)),reply_markup=shop_markup()); return
    if data.startswith("shop:"):
        item=data.split(":",1)[1]
        if item=="premium":
            data="premium:buy"
        elif item in ("boost","superlike","likes"):
            prices={"boost":setting("boost_stars",BOOST_STARS),"superlike":setting("superlike_stars",SUPERLIKE_STARS),"likes":setting("reveal_likes_stars",REVEAL_LIKES_STARS)}
            labels={"boost":"Анкета выше на 1 час","superlike":"Super Like ×1","likes":"Показать новые лайки"}
            try:
                await q.message.reply_invoice(title=labels[item],description="Цифровая услуга SecretMeet",
                    payload=f"item:{uid}:{item}",provider_token="",currency="XTR",
                    prices=[LabeledPrice(label=labels[item],amount=prices[item])])
            except TelegramError:
                await q.message.reply_text("Платёж временно недоступен. Попробуйте позже.")
            return
    if data.startswith("profile:like:") or data.startswith("profile:super:"):
        target=int(data.split(":")[-1]); superlike=data.startswith("profile:super:")
        if target==uid:
            await q.answer("Нельзя лайкнуть себя.",show_alert=True); return
        if superlike:
            u=get_user(uid)
            if not premium(u) and int(u.get("super_likes") or 0)<=0:
                await q.answer("Super Like ندارед. Аз Магазин харед.",show_alert=True); return
            if not premium(u):
                execute("UPDATE users SET super_likes=GREATEST(0,super_likes-1) WHERE telegram_id=%s",(uid,))
        execute("INSERT INTO likes(from_id,to_id) VALUES(%s,%s) ON CONFLICT(from_id,to_id) DO NOTHING",(uid,target))
        try:
            await context.bot.send_message(target,("💘 Ба шумо Super Like омад!" if superlike else "❤️ Ба шумо лайк омад!") + "\\nПрофилҳоро бинед ва дар сурати хоҳиш чат кушоед.")
        except TelegramError: pass
        await q.answer("Super Like фиристода шуд!" if superlike else "Лайк фиристода шуд!")
        await q.message.reply_text("❤️ Лайк сабт шуд.",reply_markup=main_kb(get_user(uid))); return
    if data=="shop:likes":
        data="shop:likes"
    if data=="shop:likes":
        # A paid reveal invoice is handled by successful_payment.
        try:
            await q.message.reply_invoice(title="Показать лайки",description="Открыть список людей, которым понравилась ваша анкета",
                payload=f"item:{uid}:likes",provider_token="",currency="XTR",
                prices=[LabeledPrice(label="Показать лайки",amount=setting("reveal_likes_stars",REVEAL_LIKES_STARS))])
        except TelegramError:
            await q.message.reply_text("Платёж временно недоступен. Попробуйте позже.")
        return
    if data=="premium:buy":
        try:
            await q.message.reply_invoice(title="SecretMeet Premium",description=f"Premium for {setting('premium_days',30)} days",
                payload=f"premium:{uid}:{setting('premium_days',30)}",provider_token="",currency="XTR",
                prices=[LabeledPrice(label=f"Premium {setting('premium_days',30)} days",amount=setting("premium_stars",100))])
        except TelegramError:
            await q.message.reply_text("Платёж временно недоступен. Попробуйте позже.")
        return

async def route_text(update,context):
    if not update.message or update.effective_chat.type!=ChatType.PRIVATE: return
    uid=update.effective_user.id; u=ensure_user(uid)
    if is_admin(uid) and context.user_data.get("admin_step"):
        if await admin_text(update,context): return
    if u["banned"] and not is_admin(uid): return
    if not u["is_adult"] and not is_admin(uid):
        await update.message.reply_text("Только для 18+ / Танҳо барои 18+."); return
    step=context.user_data.get("step"); text=(update.message.text or "").strip()
    if step=="age":
        try: age=int(text)
        except ValueError: age=0
        if not 18<=age<=99:
            await update.message.reply_text(tr(u,"Синну сол бояд 18–99 бошад.","Возраст должен быть 18–99.")); return
        execute("UPDATE users SET age=%s WHERE telegram_id=%s",(age,uid)); context.user_data["step"]="gender"
        await update.message.reply_text("Выберите пол / Ҷинсро интихоб кунед:",reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👨 Парень / Мард",callback_data="gender:male"),
            InlineKeyboardButton("👩 Девушка / Зан",callback_data="gender:female")]])); return
    if step=="city":
        if not 2<=len(text)<=50:
            await update.message.reply_text("Город: 2–50 символов / Шаҳр: 2–50 аломат."); return
        execute("UPDATE users SET city=%s WHERE telegram_id=%s",(text,uid)); context.user_data["step"]="bio"
        await update.message.reply_text("Напишите о себе (до 300 символов) / Дар бораи худ нависед:"); return
    if step=="bio":
        if not 1<=len(text)<=300:
            await update.message.reply_text("Максимум 300 символов / То 300 аломат."); return
        execute("UPDATE users SET bio=%s,profile_ready=TRUE WHERE telegram_id=%s",(text,uid))
        context.user_data.clear(); u=get_user(uid)
        await update.message.reply_text("🎉 Анкета готова! / Профил тайёр!",reply_markup=main_kb(u)); return

    if text in ("🌐 Язык / Забон",):
        await update.message.reply_text("Выберите язык / Забонро интихоб кунед:",reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("Тоҷикӣ 🇹🇯",callback_data="lang:tg"),InlineKeyboardButton("Русский 🇷🇺",callback_data="lang:ru")]])); return
    if text in ("👤 Моя анкета","👤 Профили ман"):
        await update.message.reply_text(profile_text(u,True),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✏️ Изменить / Тағйир додан",callback_data="profile:edit")]])); return
    if text=="🛍 Магазин":
        await update.message.reply_text(shop_text(u),reply_markup=shop_markup()); return
    if text=="💎 Premium":
        await update.message.reply_text(f"💎 Premium на {setting('premium_days',30)} дней — {setting('premium_stars',100)} Telegram Stars.",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Купить Premium",callback_data="premium:buy")],
          [InlineKeyboardButton("🛍 Открыть магазин",callback_data="shop:open")]])); return
    if text in ("🆘 Помощь","🆘 Кӯмак"):
        await update.message.reply_text("Не отправляйте адрес, телефон, пароли или финансовые данные. Используйте кнопки «Жалоба» и «Блок». / Маълумоти шахсӣ нафиристед."); return
    if not await subscription_gate(update,context,u): return
    if not u["profile_ready"]:
        await begin_profile(update,context); return
    if text in ("🔎 Найти парня","🔎 Найти девушку","🎲 Случайный чат"):
        wanted="male" if text=="🔎 Найти парня" else ("female" if text=="🔎 Найти девушку" else None)
        await find_match(update,context,wanted); return
    if text=="❤️ Анкеты":
        await browse(update,context,0); return
    partner=await get_partner(uid)
    if partner:
        try:
            await relay(update,context,partner)
        except TelegramError:
            await stop_chat(update,context)
        return
    await update.message.reply_text("Выберите действие в меню ниже.",reply_markup=main_kb(u))

async def relay(update,context,partner):
    m=update.message
    if m.text: await context.bot.send_message(partner,m.text)
    elif m.photo: await context.bot.send_photo(partner,m.photo[-1].file_id,caption=m.caption or "")
    elif m.sticker: await context.bot.send_sticker(partner,m.sticker.file_id)
    elif m.voice: await context.bot.send_voice(partner,m.voice.file_id,caption=m.caption or "")
    elif m.video: await context.bot.send_video(partner,m.video.file_id,caption=m.caption or "")
    elif m.document: await context.bot.send_document(partner,m.document.file_id,caption=m.caption or "")
    elif m.animation: await context.bot.send_animation(partner,m.animation.file_id,caption=m.caption or "")

async def get_partner(uid):
    r=execute("SELECT partner_id FROM active_chats WHERE user_id=%s",(uid,),one=True)
    return int(r["partner_id"]) if r else None

async def stop_chat(update,context,announce=True):
    uid=update.effective_user.id; partner=await get_partner(uid); u=get_user(uid)
    if partner:
        execute("DELETE FROM active_chats WHERE user_id=%s OR user_id=%s",(uid,partner))
        try: await context.bot.send_message(partner,"Собеседник завершил чат. / Ҳамсуҳбат чатро қатъ кард.",reply_markup=main_kb(get_user(partner)))
        except TelegramError: pass
    if announce and update.effective_message:
        await update.effective_message.reply_text("Чат завершён. / Чат қатъ шуд.",reply_markup=main_kb(u))

async def find_match(update,context,wanted):
    uid=update.effective_user.id; u=get_user(uid)
    if not u["profile_ready"]: await begin_profile(update,context); return
    if await get_partner(uid):
        await update.effective_message.reply_text("Вы уже в чате. Сначала завершите его.",reply_markup=chat_kb(u)); return
    looking=wanted or u["looking_for"]
    # Requested gender is the actual target gender, and target's preference must accept current user's gender.
    row=execute("""SELECT x.telegram_id FROM users x
      WHERE x.profile_ready=TRUE AND x.is_adult=TRUE AND x.banned=FALSE AND x.telegram_id<>%s
      AND x.gender = COALESCE(%s,x.gender)
      AND (x.looking_for='any' OR x.looking_for=%s)
      AND (x.looking_for='any' OR %s='any' OR x.looking_for=%s)
      AND NOT EXISTS(SELECT 1 FROM blocks b WHERE (b.blocker_id=%s AND b.blocked_id=x.telegram_id) OR (b.blocker_id=x.telegram_id AND b.blocked_id=%s))
      AND NOT EXISTS(SELECT 1 FROM active_chats a WHERE a.user_id=x.telegram_id OR a.partner_id=x.telegram_id)
      ORDER BY CASE WHEN x.premium_until>NOW() THEN 0 ELSE 1 END, RANDOM() LIMIT 1""",
      (uid,wanted,u["gender"],u["gender"],u["gender"],uid,uid),one=True)
    if not row:
        await update.effective_message.reply_text("Пока нет подходящего собеседника. Попробуйте позже или откройте «Анкеты».",reply_markup=main_kb(u)); return
    await start_chat(update,context,int(row["telegram_id"]))

async def start_chat(update,context,target):
    uid=update.effective_user.id; u=get_user(uid); other=get_user(target)
    if not other or not other["profile_ready"] or other["banned"] or target==uid:
        await update.effective_message.reply_text("Анкета недоступна."); return
    if await get_partner(uid) or await get_partner(target):
        await update.effective_message.reply_text("Один из пользователей уже в чате."); return
    blocked=execute("SELECT 1 FROM blocks WHERE (blocker_id=%s AND blocked_id=%s) OR (blocker_id=%s AND blocked_id=%s) LIMIT 1",(uid,target,target,uid),one=True)
    if blocked: await update.effective_message.reply_text("Этот пользователь недоступен."); return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO active_chats(user_id,partner_id) VALUES(%s,%s)",(uid,target))
            cur.execute("INSERT INTO active_chats(user_id,partner_id) VALUES(%s,%s)",(target,uid))
        conn.commit()
    await update.effective_message.reply_text("💬 Вы соединены анонимно! Пишите сообщение.",reply_markup=chat_kb(u))
    await context.bot.send_message(target,"💬 Вы соединены анонимно! Пишите сообщение.",reply_markup=chat_kb(other))

async def browse(update,context,after_id=0):
    uid=update.effective_user.id; u=get_user(uid)
    row=execute("""SELECT * FROM users x WHERE x.profile_ready=TRUE AND x.is_adult=TRUE AND x.banned=FALSE
      AND x.telegram_id<>%s AND x.telegram_id>%s AND (x.looking_for='any' OR x.looking_for=%s)
      AND (%s='any' OR x.gender=%s)
      AND NOT EXISTS(SELECT 1 FROM blocks b WHERE (b.blocker_id=%s AND b.blocked_id=x.telegram_id) OR (b.blocker_id=x.telegram_id AND b.blocked_id=%s))
      ORDER BY CASE WHEN x.premium_until>NOW() THEN 0 ELSE 1 END, x.telegram_id LIMIT 1""",
      (uid,after_id,u["gender"],u["looking_for"],u["looking_for"],uid,uid),one=True)
    if not row:
        row=execute("""SELECT * FROM users x WHERE x.profile_ready=TRUE AND x.is_adult=TRUE AND x.banned=FALSE
          AND x.telegram_id<>%s AND (x.looking_for='any' OR x.looking_for=%s) AND (%s='any' OR x.gender=%s)
          AND NOT EXISTS(SELECT 1 FROM blocks b WHERE (b.blocker_id=%s AND b.blocked_id=x.telegram_id) OR (b.blocker_id=x.telegram_id AND b.blocked_id=%s))
          ORDER BY RANDOM() LIMIT 1""",(uid,u["gender"],u["looking_for"],u["looking_for"],uid,uid),one=True)
    if not row:
        await update.effective_message.reply_text("Пока нет доступных анкет."); return
    await update.effective_message.reply_text(profile_text(row),reply_markup=InlineKeyboardMarkup([
      [InlineKeyboardButton("❤️ Лайк",callback_data=f"profile:like:{row['telegram_id']}"),
       InlineKeyboardButton("💘 Super Like",callback_data=f"profile:super:{row['telegram_id']}")],
      [InlineKeyboardButton("💬 Начать диалог",callback_data=f"profile:open:{row['telegram_id']}")],
      [InlineKeyboardButton("➡️ Следующая анкета",callback_data=f"profile:skip:{row['telegram_id']}")]]))

async def precheckout(update,context):
    await update.pre_checkout_query.answer(ok=True)

async def paid(update,context):
    p=update.message.successful_payment; uid=update.effective_user.id
    payload=p.invoice_payload
    if payload.startswith(f"premium:{uid}:"):
        try: days=max(1,min(365,int(payload.split(":")[-1])))
        except ValueError: days=PREMIUM_DAYS
        u=get_user(uid); now=datetime.now(timezone.utc); start=u["premium_until"] if u["premium_until"] and u["premium_until"]>now else now
        until=start+timedelta(days=days)
        execute("UPDATE users SET premium_until=%s WHERE telegram_id=%s",(until,uid))
        execute("INSERT INTO payments(telegram_id,charge_id,currency,amount) VALUES(%s,%s,%s,%s) ON CONFLICT(charge_id) DO NOTHING",
          (uid,p.telegram_payment_charge_id,p.currency,p.total_amount))
        await update.message.reply_text(f"💎 Premium фаъол шуд то {until:%Y-%m-%d}. Имтиёзҳо: анкета дар ҷустуҷӯ боло, Super Like-и номаҳдуд ва дидани лайкҳо бо харидҳои алоҳида.",reply_markup=main_kb(get_user(uid)))
        return
    if payload.startswith(f"item:{uid}:"):
        item=payload.split(":")[-1]
        execute("INSERT INTO purchases(telegram_id,item,charge_id) VALUES(%s,%s,%s) ON CONFLICT(charge_id) DO NOTHING",
                (uid,item,p.telegram_payment_charge_id))
        if item=="boost":
            execute("UPDATE users SET boost_until=NOW()+INTERVAL '1 hour' WHERE telegram_id=%s",(uid,))
            msg="🚀 Анкетаатон барои 1 соат боло бардошта шуд!"
        elif item=="superlike":
            execute("UPDATE users SET super_likes=super_likes+1 WHERE telegram_id=%s",(uid,))
            msg="💘 1 Super Like ба ҳисоби шумо илова шуд."
        elif item=="likes":
            rows=execute("""SELECT u.telegram_id,u.age,u.gender,u.city,u.bio FROM likes l JOIN users u ON u.telegram_id=l.from_id
                WHERE l.to_id=%s ORDER BY l.created_at DESC LIMIT 20""",(uid,),all=True)
            if rows:
                msg="❤️ Ба профили шумо лайк гузоштаанд:\n\n"+"\n\n".join(f"👤 {r['age']} • {r['city']}\n{(r['bio'] or '')[:180]}" for r in rows)
            else: msg="Ҳоло лайкҳои нав надоред."
        else:
            msg="Харид анҷом шуд."
        await update.message.reply_text(msg,reply_markup=main_kb(get_user(uid)))

async def admin(update,context):
    uid=update.effective_user.id
    if not is_admin(uid): return
    stats=execute("SELECT COUNT(*) total,COUNT(*) FILTER(WHERE profile_ready) profiles,COUNT(*) FILTER(WHERE premium_until>NOW()) premium,COUNT(*) FILTER(WHERE banned) banned FROM users",one=True)
    reports=execute("SELECT COUNT(*) n FROM reports WHERE created_at>NOW()-INTERVAL '7 days'",one=True)["n"]
    ch=await channels()
    await update.message.reply_text(
      f"🛠 SecretMeet v6 — Admin\n👥 Пользователи: {stats['total']}\n🧾 Анкеты: {stats['profiles']}\n💎 Premium: {stats['premium']}\n⛔ Баны: {stats['banned']}\n🚨 Жалобы за 7 дней: {reports}\n📢 Каналы: {len(ch)}\n⭐ Premium: {setting('premium_stars',100)} Stars / {setting('premium_days',30)} дней",
      reply_markup=InlineKeyboardMarkup([
       [InlineKeyboardButton("📊 Обновить статистику",callback_data="admin:stats")],
       [InlineKeyboardButton("📢 Добавить канал",callback_data="admin:channel_add"),InlineKeyboardButton("🗑 Удалить канал",callback_data="admin:channel_del")],
       [InlineKeyboardButton("📋 Список каналов",callback_data="admin:channel_list")],
       [InlineKeyboardButton("🚨 Последние жалобы",callback_data="admin:reports")],
       [InlineKeyboardButton("📣 Реклама / рассылка",callback_data="admin:broadcast")],
       [InlineKeyboardButton("💎 Выдать Premium",callback_data="admin:grant"),InlineKeyboardButton("➖ Снять Premium",callback_data="admin:revoke")],
       [InlineKeyboardButton("⛔ Забанить",callback_data="admin:ban"),InlineKeyboardButton("✅ Разбанить",callback_data="admin:unban")],
       [InlineKeyboardButton("💰 Цены магазина",callback_data="admin:prices")],
       [InlineKeyboardButton("➕ Добавить админа",callback_data="admin:admin_add"),InlineKeyboardButton("➖ Удалить админа",callback_data="admin:admin_del")],
       [InlineKeyboardButton("👮 Список админов",callback_data="admin:admin_list")]
      ]))

async def admin_cb(update,context,data):
    uid=update.effective_user.id
    if not is_admin(uid): return
    q=update.callback_query
    if data=="admin:stats":
        await q.message.reply_text("Откройте /admin для обновлённой статистики."); return
    if data=="admin:channel_add":
        context.user_data["admin_step"]="channel_add"
        await q.message.reply_text("Отправьте @username или numeric chat ID канала, затем название через | . Пример: @mychannel | Мой канал")
    elif data=="admin:channel_del":
        context.user_data["admin_step"]="channel_del"
        await q.message.reply_text("Отправьте точный @username или ID канала для удаления. /cancel — отмена.")
    elif data=="admin:channel_list":
        rows=await channels()
        await q.message.reply_text("📢 Обязательные каналы:\n"+("\n".join(f"• {r['channel']} — {r['title']}" for r in rows) if rows else "Пока нет каналов."))
    elif data=="admin:reports":
        rows=execute("SELECT id,reporter_id,reported_id,reason FROM reports ORDER BY id DESC LIMIT 10",all=True)
        await q.message.reply_text("\n".join(f"#{r['id']} {r['reporter_id']} → {r['reported_id']}: {r['reason']}" for r in rows) if rows else "Жалоб нет.")
    elif data=="admin:admin_list":
        rows=execute("SELECT telegram_id FROM admins ORDER BY created_at",all=True)
        await q.message.reply_text("👮 Админы:\n"+"\n".join(f"• {r['telegram_id']}"+(" (главный)" if r['telegram_id']==7659107145 else "") for r in rows))
    elif data in ("admin:broadcast","admin:grant","admin:ban","admin:unban","admin:revoke","admin:prices","admin:admin_add","admin:admin_del"):
        if data in ("admin:admin_add","admin:admin_del") and not owner(uid):
            await q.message.reply_text("Только главный администратор может менять список админов."); return
        action=data.split(":")[1]; context.user_data["admin_step"]=action
        prompts={"broadcast":"Отправьте рекламное сообщение: текст, фото, видео, документ или пост с подписью. Оно будет скопировано пользователям. /cancel — отмена.","grant":"Telegram ID и дни: 123456789 30","ban":"Отправьте Telegram ID для бана.","unban":"Отправьте Telegram ID для разбана.","revoke":"Отправьте Telegram ID, чтобы снять Premium.","prices":"Отправьте цену и срок Premium в формате: 100 30 (Stars и дни).","admin_add":"Отправьте Telegram ID пользователя для добавления админом. Он должен нажать /start.","admin_del":"Отправьте Telegram ID дополнительного админа для удаления."}
        await q.message.reply_text(prompts[action])

async def admin_text(update,context):
    action=context.user_data.get("admin_step"); uid=update.effective_user.id
    if not is_admin(uid) or not action: return False
    msg=update.effective_message; text=(msg.text or msg.caption or "").strip()
    if text=="/cancel":
        context.user_data.pop("admin_step",None); await msg.reply_text("Отменено."); return True
    if action=="broadcast":
        rows=execute("SELECT telegram_id FROM users WHERE banned=FALSE",all=True) or []
        sent=failed=0; context.user_data.pop("admin_step",None)
        await msg.reply_text(f"Начинаю рассылку по {len(rows)} пользователям…")
        for r in rows:
            try: await msg.copy(chat_id=r["telegram_id"]); sent+=1
            except TelegramError: failed+=1
            await asyncio.sleep(0.04)
        await msg.reply_text(f"Рассылка завершена. Успешно: {sent}; ошибки: {failed}."); return True
    if not msg.text:
        await msg.reply_text("Для этого действия отправьте текстовый ответ или /cancel."); return True
    context.user_data.pop("admin_step",None)
    if action=="channel_add":
        parts=[x.strip() for x in text.split("|",1)]; channel=parts[0]; title=parts[1] if len(parts)>1 else channel
        if not (channel.startswith("@") or channel.lstrip("-").isdigit()): await msg.reply_text("Формат: @channel | Название"); return True
        try:
            chat=await context.bot.get_chat(channel); member=await context.bot.get_chat_member(chat.id,context.bot.id)
            if member.status not in ("administrator","creator"):
                await msg.reply_text("Сначала добавьте бота в канал как администратора, затем повторите."); return True
            stored=str(chat.id) if not channel.startswith("@") else channel
            execute("INSERT INTO required_channels(channel,title,added_by) VALUES(%s,%s,%s) ON CONFLICT(channel) DO UPDATE SET title=EXCLUDED.title",(stored,title,uid))
            await msg.reply_text("✅ Канал добавлен. Бот должен оставаться администратором канала.")
        except TelegramError: await msg.reply_text("Не удалось проверить канал. Убедитесь, что канал существует и бот добавлен администратором.")
        return True
    if action=="channel_del":
        row=execute("DELETE FROM required_channels WHERE channel=%s RETURNING channel",(text,),one=True)
        await msg.reply_text("Канал удалён." if row else "Канал не найден. Сверьте точное значение через /admin."); return True
    if action=="prices":
        try:
            parts=text.split(); stars=max(1,min(100000,int(parts[0]))); days=max(1,min(365,int(parts[1] if len(parts)>1 else 30)))
            for key,value in (("premium_stars",stars),("premium_days",days)):
                execute("INSERT INTO app_settings(key,value,updated_by) VALUES(%s,%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_by=EXCLUDED.updated_by,updated_at=NOW()",(key,str(value),uid))
            await msg.reply_text(f"✅ Цена Premium сохранена: {stars} ⭐ за {days} дней.")
        except (ValueError,IndexError): await msg.reply_text("Формат: 100 30")
        return True
    if action in ("admin_add","admin_del"):
        if not owner(uid): await msg.reply_text("Только главный администратор."); return True
        try: target=int(text)
        except ValueError: await msg.reply_text("Telegram ID должен быть числом."); return True
        if target==7659107145: await msg.reply_text("Главного администратора нельзя удалить."); return True
        if action=="admin_add":
            if not get_user(target): await msg.reply_text("Пользователь сначала должен нажать /start в боте."); return True
            execute("INSERT INTO admins(telegram_id,added_by) VALUES(%s,%s) ON CONFLICT DO NOTHING",(target,uid))
            await msg.reply_text(f"✅ {target} добавлен в админы.")
            try: await context.bot.send_message(target,"Вам выданы права администратора SecretMeet. Команда: /admin")
            except TelegramError: pass
        else:
            execute("DELETE FROM admins WHERE telegram_id=%s AND telegram_id<>7659107145",(target,))
            await msg.reply_text(f"Права админа для {target} сняты.")
        return True
    if action=="grant":
        try:
            parts=text.split(); target=int(parts[0]); days=max(1,min(365,int(parts[1] if len(parts)>1 else 30)))
            if not get_user(target): await msg.reply_text("Пользователь не найден."); return True
            execute("UPDATE users SET premium_until=GREATEST(COALESCE(premium_until,NOW()),NOW())+(%s * INTERVAL '1 day') WHERE telegram_id=%s",(days,target))
            await msg.reply_text("Premium выдан.")
            try: await context.bot.send_message(target,f"💎 Администратор выдал Premium на {days} дней.")
            except TelegramError: pass
        except (ValueError,IndexError): await msg.reply_text("Формат: 123456789 30")
        return True
    if action=="revoke":
        try:
            target=int(text); row=execute("UPDATE users SET premium_until=NULL WHERE telegram_id=%s RETURNING telegram_id",(target,),one=True)
            await msg.reply_text("Premium снят." if row else "Пользователь не найден.")
            if row:
                try: await context.bot.send_message(target,"Срок Premium был завершён администратором.")
                except TelegramError: pass
        except ValueError: await msg.reply_text("ID должен быть числом.")
        return True
    if action in ("ban","unban"):
        try:
            target=int(text)
            if target==7659107145 and action=="ban": await msg.reply_text("Нельзя забанить главного администратора."); return True
            row=execute("UPDATE users SET banned=%s WHERE telegram_id=%s RETURNING telegram_id",(action=="ban",target),one=True)
            if action=="ban": execute("DELETE FROM active_chats WHERE user_id=%s OR partner_id=%s",(target,target))
            await msg.reply_text(("Пользователь забанен." if action=="ban" else "Пользователь разбанен.") if row else "Пользователь не найден.")
            if row:
                try: await context.bot.send_message(target,"⛔ Доступ ограничен." if action=="ban" else "✅ Доступ восстановлен.")
                except TelegramError: pass
        except ValueError: await msg.reply_text("ID должен быть числом.")
        return True
    return False

async def notify_admins(context,message):
    for aid in ADMIN_IDS:
        try: await context.bot.send_message(aid,message)
        except TelegramError: pass

async def cancel(update,context):
    context.user_data.clear()
    await update.message.reply_text("Отменено.")

def serve(): web.run(host="0.0.0.0",port=PORT,use_reloader=False)

def serve():
    web.run(host="0.0.0.0", port=PORT, use_reloader=False)


async def bot_main():
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("cancel", cancel))

    application.add_handler(CallbackQueryHandler(cb))
    application.add_handler(PreCheckoutQueryHandler(precheckout))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paid))

    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, route_text)
    )

    application.add_handler(
        MessageHandler(
            (
                filters.PHOTO
                | filters.Sticker.ALL
                | filters.VOICE
                | filters.VIDEO
                | filters.Document.ALL
                | filters.ANIMATION
            ) & ~filters.COMMAND,
            route_text,
        )
    )

    log.info("SecretMeet starting")

    await application.initialize()
    await application.start()

    if application.updater is None:
        raise RuntimeError("Telegram updater is not available")

    await application.updater.start_polling(
        allowed_updates=Update.ALL_TYPES
    )

    try:
        await asyncio.Event().wait()
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


def main():
    init_db()

    threading.Thread(
        target=serve,
        daemon=True
    ).start()

    asyncio.run(bot_main())


if __name__ == "__main__":
    main()
