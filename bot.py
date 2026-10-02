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

# Используем пул PostgreSQL-соединений вместо нового TLS-соединения для каждого запроса.
# DATABASE_URL должен содержать pooled-строку Neon (в hostname есть -pooler).
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
      profile_photo TEXT, username TEXT,
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
    """CREATE TABLE IF NOT EXISTS search_usage (
      telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
      search_date DATE NOT NULL DEFAULT CURRENT_DATE, searches INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(telegram_id, search_date))""",
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
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_photo TEXT")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
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
    execute("UPDATE users SET last_active=NOW(), language='ru' WHERE telegram_id=%s", (uid,))
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
def tr(user, tg, ru):
    # SecretMeet работает только на русском языке.
    return ru

def premium(user): return bool(user and user.get("premium_until") and user["premium_until"] > datetime.now(timezone.utc))

def premium_until_text(user):
    until = user.get("premium_until") if user else None
    if not until or until <= datetime.now(timezone.utc):
        return "Премиум не активен"
    return until.astimezone(timezone.utc).strftime("%d.%m.%Y в %H:%M UTC")

def main_kb(u):
    rows = [
      [KeyboardButton("🔎 Найти парня"), KeyboardButton("🔎 Найти девушку")],
      [KeyboardButton("🎲 Случайный чат"), KeyboardButton("❤️ Смотреть анкеты")],
      [KeyboardButton("👤 Моя анкета"), KeyboardButton("💎 Премиум")],
      [KeyboardButton("❤️ Кто поставил лайк"), KeyboardButton("🛍 Магазин")],
      [KeyboardButton("🆘 Помощь")],
    ]
    if is_admin(u["telegram_id"]):
        rows.append([KeyboardButton("🛠 Админ-панель")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)

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
      [InlineKeyboardButton(f"💎 Премиум — {ps} ⭐ / {pd} дней",callback_data="shop:premium")],
      [InlineKeyboardButton(f"🚀 Поднять анкету на 1 час — {bs} ⭐",callback_data="shop:boost")],
      [InlineKeyboardButton(f"💘 Суперлайк ×1 — {ss} ⭐",callback_data="shop:superlike")],
      [InlineKeyboardButton(f"❤️ Показать новые лайки — {ls} ⭐",callback_data="shop:likes")]
    ])

def shop_text(u):
    active = premium(u)
    until = premium_until_text(u)
    if active:
        return ("💎 SECRETMEET PREMIUM АКТИВЕН\n\n"
                f"✅ Действует до: {until}\n\n"
                "Все функции включены бесплатно на время Премиум:\n"
                "• Безлимитный поиск по полу и случайный чат\n"
                "• Просмотр людей, поставивших лайк\n"
                "• Суперлайк без отдельной оплаты\n"
                "• Поднятие анкеты (Продвижение) без отдельной оплаты\n"
                "• Приоритет анкеты в поиске\n\n"
                "Нажмите «Кто поставил лайк», чтобы посмотреть лайки.")
    row=execute("SELECT COUNT(*) n FROM likes WHERE to_id=%s AND from_id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=%s)",(u["telegram_id"],u["telegram_id"]),one=True)
    return ("🛍 МАГАЗИН SECRETMEET\n\n"
            f"💎 Премиум — {setting('premium_stars',100)} ⭐ на {setting('premium_days',30)} дней.\n"
            "В Премиум входят: безлимитный поиск по полу, просмотр лайков, Суперлайк, Продвижение и приоритет анкеты.\n\n"
            "🆓 Без Премиум: поиск парня/девушки — до 5 раз в сутки; случайный чат и анкеты доступны отдельно.\n"
            f"❤️ Сейчас у вашей анкеты лайков: {row['n']}.\n\n"
            "Оплата Премиум проходит через звёзды Telegram.")

def premium_text(u):
    if premium(u) or is_admin(u["telegram_id"]):
        expiry = "Без ограничений для администратора" if is_admin(u["telegram_id"]) and not premium(u) else premium_until_text(u)
        return ("💎 ВСЕ ФУНКЦИИ ДОСТУПНЫ\n\n"
                f"📅 Срок: {expiry}\n\n"
                "Вам доступны бесплатно до окончания срока:\n"
                "✅ Поиск по полу без ограничений\n"
                "✅ Просмотр тех, кто поставил лайк\n"
                "✅ Суперлайк без отдельной оплаты\n"
                "✅ Продвижение анкеты без отдельной оплаты\n"
                "✅ Приоритет анкеты в поиске")
    return (f"💎 SECRETMEET PREMIUM — {setting('premium_stars',100)} ⭐ / {setting('premium_days',30)} дней\n\n"
            "Откройте все возможности знакомств одним оформлением: \n"
            "❤️ Узнавайте, кто поставил вам лайк\n"
            "🔎 Ищите парня или девушку без дневного лимита\n"
            "💘 Используйте Суперлайк без отдельной оплаты\n"
            "🚀 Поднимайте анкету без отдельной оплаты\n"
            "⭐ Ваша анкета получает приоритет в поиске\n\n"
            "После успешной оплаты Премиум действует 30 дней. Нажмите кнопку ниже, чтобы оформить подписку.")

def likes_text(uid):
    rows=execute("""SELECT u.telegram_id,u.age,u.gender,u.city,u.bio FROM likes l JOIN users u ON u.telegram_id=l.from_id
        WHERE l.to_id=%s AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.blocker_id=%s AND b.blocked_id=l.from_id)
        ORDER BY l.created_at DESC LIMIT 30""",(uid,uid),all=True) or []
    if not rows: return "❤️ Пока никто не поставил лайк вашей анкете. Заполните анкету и продолжайте знакомиться!"
    return "❤️ ВАМ ПОСТАВИЛИ ЛАЙК\n\n"+"\n\n".join(
        f"👤 {r['age'] or '—'} лет • {r['city'] or 'Город не указан'}\n{(r['bio'] or 'Описание не указано')[:180]}"
        for r in rows)

def profile_text(p, own=False):
    gender={"male":"Парень","female":"Девушка"}
    title="👤 Ваша анкета" if own else "💌 Анкета"
    # Не показываем username Telegram в публичной анкете.
    return f"{title}\n🎂 {p.get('age') or '—'}\n👤 {gender.get(p.get('gender'), '—')}\n📍 {p.get('city') or '—'}\n\n{p.get('bio') or 'Описание не указано'}"

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
            # Ошибка конфигурации или прав не должна незаметно пропускать проверку.
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
      "Чтобы пользоваться ботом, подпишитесь на обязательные каналы и нажмите «✅ Проверить подписку».",
      "Чтобы пользоваться ботом, подпишитесь на канал(ы) и нажмите «Проверить подписку»."),
      reply_markup=InlineKeyboardMarkup(buttons))
    return False

async def start(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE: return
    u=ensure_user(update.effective_user.id)
    if u["banned"] and not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ваш доступ ограничен администратором.")
        return
    if is_admin(update.effective_user.id):
        await update.message.reply_text("🛠 SecretMeet — панель администратора доступна в меню.", reply_markup=main_kb(u))
        return
    if not u["is_adult"]:
        await update.message.reply_text(
          "💜 Добро пожаловать в SecretMeet!\n\nСервис знакомств доступен только пользователям старше 18 лет. Подтвердите свой возраст.",
          reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Мне уже есть 18 лет",callback_data="age:yes")],
            [InlineKeyboardButton("❌ Мне нет 18 лет",callback_data="age:no")]
          ]))
    elif not u["profile_ready"]:
        await update.message.reply_text("💜 SecretMeet\nСоздайте анкету, чтобы начать знакомиться.")
        await begin_profile(update,context)
    else:
        await update.message.reply_text("💜 SecretMeet — выберите нужное действие в меню ниже.",reply_markup=main_kb(u))

async def begin_profile(update,context):
    uid=update.effective_user.id; u=get_user(uid)
    context.user_data.clear(); context.user_data["step"]="age"
    await update.effective_message.reply_text("Введите возраст (18–99):")

async def cb(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id; u=ensure_user(uid); data=q.data or ""
    if data.startswith("lang:"):
        await q.message.reply_text("В SecretMeet используется русский язык.",reply_markup=main_kb(u)); return
    if data=="age:no":
        await q.edit_message_text("Бот доступен только совершеннолетним (18+)."); return
    if data=="age:yes":
        execute("UPDATE users SET is_adult=TRUE, language='ru' WHERE telegram_id=%s",(uid,))
        await q.edit_message_text("✅ Возраст подтверждён.")
        current=get_user(uid)
        if not await subscription_gate(update,context,current): return
        await begin_profile(update,context); return
    if data=="sub:check":
        ok,missing=await check_subscriptions(context,uid)
        if ok:
            if not u["profile_ready"]:
                await q.message.reply_text("✅ Подписка подтверждена. Давайте создадим вашу анкету.")
                await begin_profile(update,context)
            else:
                await q.message.reply_text("✅ Подписка подтверждена.",reply_markup=main_kb(u))
        else:
            await q.answer("Сначала подпишитесь на все обязательные каналы.",show_alert=True)
        return
    if data.startswith("admin:"):
        await admin_cb(update,context,data); return
    if not u["is_adult"] or u["banned"]: return
    if data=="profile:edit":
        await begin_profile(update,context); return
    if data.startswith("gender:"):
        g=data.split(":")[1]
        execute("UPDATE users SET gender=%s WHERE telegram_id=%s",(g,uid))
        await q.edit_message_text(tr(u,"Кого ищете?","Кого ищете?"),
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Парня",callback_data="looking:male"),
                                               InlineKeyboardButton("Девушку",callback_data="looking:female"),
                                               InlineKeyboardButton("Всех",callback_data="looking:any")]])); return
    if data.startswith("looking:"):
        execute("UPDATE users SET looking_for=%s WHERE telegram_id=%s",(data.split(":")[1],uid))
        context.user_data["step"]="city"
        await q.edit_message_text(tr(u,"Укажите ваш город:","Введите ваш город:")); return
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
                execute("INSERT INTO reports(reporter_id,reported_id,reason) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT 1 FROM reports WHERE reporter_id=%s AND reported_id=%s)",(uid,partner,"reported during chat",uid,partner))
                total=execute("SELECT COUNT(DISTINCT reporter_id) AS n FROM reports WHERE reported_id=%s",(partner,),one=True)["n"]
                await notify_admins(context,f"🚨 Жалоба: пользователь {uid} пожаловался на {partner}. Уникальных жалобщиков: {total}/30.")
                if total >= 30:
                    execute("UPDATE users SET banned=TRUE WHERE telegram_id=%s",(partner,))
                    execute("DELETE FROM active_chats WHERE user_id=%s OR partner_id=%s",(partner,partner))
                    await notify_admins(context,f"⛔ Автобан: пользователь {partner} получил жалобы от {total} разных пользователей и заблокирован.")
                    try: await context.bot.send_message(partner,"⛔ Ваш аккаунт автоматически заблокирован после жалоб от 30 разных пользователей. Если считаете это ошибкой, напишите @ffxdavlatov.")
                    except TelegramError: pass
            await stop_chat(update,context,announce=False)
            await q.message.reply_text("Готово. Чат завершён.",reply_markup=main_kb(u))
        return
    if data.startswith("profile:open:"):
        await start_chat(update,context,int(data.split(":")[-1])); return
    if data.startswith("profile:skip:"):
        await browse(update,context,int(data.split(":")[-1])); return
    if data=="shop:open":
        current=get_user(uid)
        if premium(current) or is_admin(uid):
            await q.message.reply_text(shop_text(current),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❤️ Кто поставил лайк",callback_data="likes:view")]]));
        else:
            await q.message.reply_text(shop_text(current),reply_markup=shop_markup())
        return
    if data=="likes:view":
        current=get_user(uid)
        if not (premium(current) or is_admin(uid)):
            await q.message.reply_text(premium_text(current),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Купить Премиум",callback_data="premium:buy")]])); return
        await q.message.reply_text(likes_text(uid),reply_markup=main_kb(current)); return
    if data.startswith("shop:"):
        item=data.split(":",1)[1]
        if item=="premium":
            data="premium:buy"
        elif item in ("boost","superlike","likes"):
            current=get_user(uid)
            if premium(current) or is_admin(uid):
                if item=="boost":
                    execute("UPDATE users SET boost_until=NOW()+INTERVAL '1 hour' WHERE telegram_id=%s",(uid,))
                    await q.message.reply_text("🚀 Анкета поднята на 1 час бесплатно.",reply_markup=main_kb(current)); return
                if item=="superlike":
                    execute("UPDATE users SET super_likes=super_likes+1 WHERE telegram_id=%s",(uid,))
                    await q.message.reply_text("💘 Суперлайк добавлен бесплатно.",reply_markup=main_kb(current)); return
                await q.message.reply_text(likes_text(uid),reply_markup=main_kb(current)); return
            prices={"boost":setting("boost_stars",BOOST_STARS),"superlike":setting("superlike_stars",SUPERLIKE_STARS),"likes":setting("reveal_likes_stars",REVEAL_LIKES_STARS)}
            labels={"boost":"Анкета выше на 1 час","superlike":"Суперлайк ×1","likes":"Показать новые лайки"}
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
            if not (premium(u) or is_admin(uid)) and int(u.get("super_likes") or 0)<=0:
                await q.answer("У вас нет Суперлайк. Оформите Премиум или используйте магазин.",show_alert=True); return
            if not (premium(u) or is_admin(uid)):
                execute("UPDATE users SET super_likes=GREATEST(0,super_likes-1) WHERE telegram_id=%s",(uid,))
        execute("INSERT INTO likes(from_id,to_id) VALUES(%s,%s) ON CONFLICT(from_id,to_id) DO NOTHING",(uid,target))
        try:
            await context.bot.send_message(target,("💘 Вам отправили Суперлайк!" if superlike else "❤️ Вам поставили лайк!") + "\\nОткройте раздел «❤️ Кто поставил лайк», чтобы посмотреть анкету.")
        except TelegramError: pass
        await q.answer("Суперлайк отправлен!" if superlike else "Лайк отправлен!")
        await q.message.reply_text("❤️ Лайк отправлен.",reply_markup=main_kb(get_user(uid))); return
    if data=="shop:likes":
        current=get_user(uid)
        if premium(current) or is_admin(uid):
            await q.message.reply_text(likes_text(uid),reply_markup=main_kb(current)); return
        # Платный просмотр лайков обрабатывается после успешной оплаты.
        try:
            await q.message.reply_invoice(title="Показать лайки",description="Открыть список людей, которым понравилась ваша анкета",
                payload=f"item:{uid}:likes",provider_token="",currency="XTR",
                prices=[LabeledPrice(label="Показать лайки",amount=setting("reveal_likes_stars",REVEAL_LIKES_STARS))])
        except TelegramError:
            await q.message.reply_text("Платёж временно недоступен. Попробуйте позже.")
        return
    if data=="premium:buy":
        try:
            await q.message.reply_invoice(title="SecretMeet Премиум",description=f"Все функции Премиум на {setting('premium_days',30)} дней",
                payload=f"premium:{uid}:{setting('premium_days',30)}",provider_token="",currency="XTR",
                prices=[LabeledPrice(label=f"Премиум на {setting('premium_days',30)} дней",amount=setting("premium_stars",100))])
        except TelegramError:
            await q.message.reply_text("Платёж временно недоступен. Попробуйте позже.")
        return

async def route_text(update,context):
    if not update.message or update.effective_chat.type!=ChatType.PRIVATE: return
    uid=update.effective_user.id; u=ensure_user(uid)
    execute("UPDATE users SET username=%s WHERE telegram_id=%s", (update.effective_user.username, uid))
    u=get_user(uid)
    if is_admin(uid) and context.user_data.get("admin_step"):
        if await admin_text(update,context): return
    if u["banned"] and not is_admin(uid): return
    if not u["is_adult"] and not is_admin(uid):
        await update.message.reply_text("Сервис доступен только пользователям старше 18 лет."); return
    step=context.user_data.get("step"); text=(update.message.text or "").strip()
    if step=="profile_photo" and update.message.photo:
        photo_id=update.message.photo[-1].file_id
        execute("UPDATE users SET profile_photo=%s,profile_ready=TRUE WHERE telegram_id=%s",(photo_id,uid))
        context.user_data.clear(); u=get_user(uid)
        await update.message.reply_text("🎉 Анкета готова! Теперь можно знакомиться.",reply_markup=main_kb(u)); return
    if step=="profile_photo" and (update.message.video or update.message.text):
        await update.message.reply_text("Для анкеты нужно отправить фото. Видео можно отправлять уже в анонимном диалоге.")
        return
    if step=="age":
        try: age=int(text)
        except ValueError: age=0
        if not 18<=age<=99:
            await update.message.reply_text(tr(u,"Возраст должен быть от 18 до 99 лет.","Возраст должен быть 18–99.")); return
        execute("UPDATE users SET age=%s WHERE telegram_id=%s",(age,uid)); context.user_data["step"]="gender"
        await update.message.reply_text("Выберите ваш пол:",reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👨 Парень",callback_data="gender:male"),
            InlineKeyboardButton("👩 Девушка",callback_data="gender:female")]])); return
    if step=="city":
        if not 2<=len(text)<=50:
            await update.message.reply_text("Укажите город (от 2 до 50 символов)."); return
        execute("UPDATE users SET city=%s WHERE telegram_id=%s",(text,uid)); context.user_data["step"]="bio"
        await update.message.reply_text("Напишите несколько слов о себе (до 300 символов):"); return
    if step=="bio":
        if not 1<=len(text)<=300:
            await update.message.reply_text("Максимум 300 символов."); return
        execute("UPDATE users SET bio=%s WHERE telegram_id=%s",(text,uid))
        context.user_data["step"]="profile_photo"
        await update.message.reply_text("📸 Теперь отправьте фото для анкеты. Оно будет показываться в карточке анкеты. Отправьте именно фото (не файл). В любой момент фото можно заменить через изменение анкеты.")
        return
    if step=="profile_photo":
        if update.message.photo:
            photo_id=update.message.photo[-1].file_id
            execute("UPDATE users SET profile_photo=%s,profile_ready=TRUE WHERE telegram_id=%s",(photo_id,uid))
            context.user_data.clear(); u=get_user(uid)
            await update.message.reply_text("🎉 Анкета готова! Теперь можно знакомиться.",reply_markup=main_kb(u)); return
        await update.message.reply_text("Пожалуйста, отправьте фото как фотографию в Telegram, а не как документ.")
        return

    if text=="👤 Моя анкета":
        if u.get("profile_photo"):
            await update.message.reply_photo(u["profile_photo"],caption=profile_text(u,True),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✏️ Изменить анкету",callback_data="profile:edit")]]))
        else:
            await update.message.reply_text(profile_text(u,True),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✏️ Изменить анкету",callback_data="profile:edit")]]))
        return
    if text=="🛍 Магазин":
        current=get_user(uid)
        if premium(current) or is_admin(uid):
            buttons=[[InlineKeyboardButton("❤️ Кто поставил лайк",callback_data="likes:view")],
                     [InlineKeyboardButton("🚀 Поднять анкету бесплатно",callback_data="shop:boost")],
                     [InlineKeyboardButton("💘 Получить Суперлайк бесплатно",callback_data="shop:superlike")]]
            await update.message.reply_text(shop_text(current),reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await update.message.reply_text(shop_text(current),reply_markup=shop_markup())
        return
    if text=="💎 Премиум":
        current=get_user(uid)
        buttons=[]
        if premium(current) or is_admin(uid):
            buttons.append([InlineKeyboardButton("❤️ Кто поставил лайк",callback_data="likes:view")])
            buttons.append([InlineKeyboardButton("🚀 Поднять анкету бесплатно",callback_data="shop:boost")])
            buttons.append([InlineKeyboardButton("💘 Получить Суперлайк бесплатно",callback_data="shop:superlike")])
        else:
            buttons.append([InlineKeyboardButton("⭐ Купить Премиум",callback_data="premium:buy")])
        buttons.append([InlineKeyboardButton("🛍 Открыть магазин",callback_data="shop:open")])
        await update.message.reply_text(premium_text(current),reply_markup=InlineKeyboardMarkup(buttons)); return
    if text=="❤️ Кто поставил лайк":
        if not (premium(u) or is_admin(uid)):
            await update.message.reply_text(premium_text(u),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Купить Премиум",callback_data="premium:buy")]])); return
        await update.message.reply_text(likes_text(uid),reply_markup=main_kb(u)); return
    if text=="🛠 Админ-панель" and is_admin(uid):
        await admin(update,context); return
    if text=="🆘 Помощь":
        await update.message.reply_text("🆘 ПОМОЩЬ SECRETMEET\n\n"
          "По вопросам разблокировки аккаунта, размещения рекламы, сотрудничества и другим вопросам напишите администратору: @ffxdavlatov\n\n"
          "Не отправляйте адрес, номер телефона, пароли и финансовые данные. Во время анонимного чата используйте кнопки «Жалоба» и «Блок», если собеседник ведёт себя неподобающе.",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✉️ Написать администратору",url="https://t.me/ffxdavlatov")]])); return
    if not await subscription_gate(update,context,u): return
    if not u["profile_ready"]:
        await begin_profile(update,context); return
    if text in ("🔎 Найти парня","🔎 Найти девушку","🎲 Случайный чат"):
        wanted="male" if text=="🔎 Найти парня" else ("female" if text=="🔎 Найти девушку" else None)
        await find_match(update,context,wanted); return
    if text=="❤️ Смотреть анкеты":
        await browse(update,context,0); return
    partner=await get_partner(uid)
    if partner:
        try:
            await relay(update,context,partner)
        except TelegramError:
            await stop_chat(update,context)
        return
    await update.message.reply_text("Выберите действие в меню ниже 👇",reply_markup=main_kb(u))

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
        try: await context.bot.send_message(partner,"Собеседник завершил чат.",reply_markup=main_kb(get_user(partner)))
        except TelegramError: pass
    if announce and update.effective_message:
        await update.effective_message.reply_text("Чат завершён.",reply_markup=main_kb(u))

async def find_match(update,context,wanted):
    uid=update.effective_user.id; u=get_user(uid)
    if not u["profile_ready"]: await begin_profile(update,context); return
    if not is_admin(uid) and not await subscription_gate(update,context,u): return
    if await get_partner(uid):
        await update.effective_message.reply_text("Вы уже в чате. Сначала завершите текущий чат.",reply_markup=chat_kb(u)); return
    # Бесплатный пользователь может выполнить до 5 поисков по полу в день. Для Премиум и администраторов лимит отсутствует.
    gender_search = wanted in ("male", "female")
    if gender_search and not (premium(u) or is_admin(uid)):
        usage=execute("SELECT searches FROM search_usage WHERE telegram_id=%s AND search_date=CURRENT_DATE",(uid,),one=True)
        used=int(usage["searches"]) if usage else 0
        if used >= 5:
            await update.effective_message.reply_text(
                "🔒 Вы использовали 5 бесплатных поисков по полу за сегодня.\n\n"
                f"💎 Премиум за {setting('premium_stars',100)} ⭐ на {setting('premium_days',30)} дней открывает безлимитный поиск и просмотр лайков.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Купить Премиум",callback_data="premium:buy")]])); return
        execute("INSERT INTO search_usage(telegram_id,search_date,searches) VALUES(%s,CURRENT_DATE,1) ON CONFLICT(telegram_id,search_date) DO UPDATE SET searches=search_usage.searches+1",(uid,))
        remaining=4-used
    else:
        remaining=None
    status_message=await update.effective_message.reply_text("🔎 Ищем подходящего собеседника…")
    await context.bot.send_chat_action(chat_id=uid, action="typing")
    looking=wanted or u["looking_for"]
    # Ищем пользователя нужного пола, чьи настройки поиска также подходят текущему пользователю.
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
        suffix = f"\n\nСегодня осталось поисков по полу: {remaining}." if remaining is not None else ""
        try:
            await status_message.edit_text("😕 Пока не удалось найти подходящего собеседника. Попробуйте позже или откройте раздел «Анкеты»."+suffix)
        except TelegramError:
            pass
        return
    try:
        await status_message.delete()
    except TelegramError:
        pass
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
    await update.effective_message.reply_text("💬 Вы соединены анонимно! Можете начинать общение.",reply_markup=chat_kb(u))
    await context.bot.send_message(target,"💬 Вы соединены анонимно! Можете начинать общение.",reply_markup=chat_kb(other))

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
    markup=InlineKeyboardMarkup([
      [InlineKeyboardButton("❤️ Лайк",callback_data=f"profile:like:{row['telegram_id']}"),
       InlineKeyboardButton("💘 Суперлайк",callback_data=f"profile:super:{row['telegram_id']}")],
      [InlineKeyboardButton("💬 Начать диалог",callback_data=f"profile:open:{row['telegram_id']}")],
      [InlineKeyboardButton("➡️ Следующая анкета",callback_data=f"profile:skip:{row['telegram_id']}")]])
    if row.get("profile_photo"):
        await update.effective_message.reply_photo(row["profile_photo"],caption=profile_text(row),reply_markup=markup)
    else:
        await update.effective_message.reply_text(profile_text(row),reply_markup=markup)

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
        await update.message.reply_text(f"🎉 Премиум успешно активирован до {until:%d.%m.%Y %H:%M UTC}!\n\nТеперь бесплатно доступны все функции Премиум: безлимитный поиск по полу, просмотр лайков, Суперлайк, Продвижение и приоритет анкеты.",reply_markup=main_kb(get_user(uid)))
        return
    if payload.startswith(f"item:{uid}:"):
        item=payload.split(":")[-1]
        execute("INSERT INTO purchases(telegram_id,item,charge_id) VALUES(%s,%s,%s) ON CONFLICT(charge_id) DO NOTHING",
                (uid,item,p.telegram_payment_charge_id))
        if item=="boost":
            execute("UPDATE users SET boost_until=NOW()+INTERVAL '1 hour' WHERE telegram_id=%s",(uid,))
            msg="🚀 Ваша анкета поднята на 1 час!"
        elif item=="superlike":
            execute("UPDATE users SET super_likes=super_likes+1 WHERE telegram_id=%s",(uid,))
            msg="💘 1 Суперлайк добавлен на ваш счёт."
        elif item=="likes":
            rows=execute("""SELECT u.telegram_id,u.age,u.gender,u.city,u.bio FROM likes l JOIN users u ON u.telegram_id=l.from_id
                WHERE l.to_id=%s ORDER BY l.created_at DESC LIMIT 20""",(uid,),all=True)
            if rows:
                msg="❤️ На вашу анкету поставили лайк:\n\n"+"\n\n".join(f"👤 {r['age'] or '—'} лет • {r['city'] or 'Город не указан'}\n{(r['bio'] or '')[:180]}" for r in rows)
            else: msg="Пока новых лайков нет."
        else:
            msg="Покупка успешно завершена."
        await update.message.reply_text(msg,reply_markup=main_kb(get_user(uid)))

async def admin(update,context):
    uid=update.effective_user.id
    if not is_admin(uid): return
    stats=execute("SELECT COUNT(*) total,COUNT(*) FILTER(WHERE profile_ready) profiles,COUNT(*) FILTER(WHERE premium_until>NOW()) premium,COUNT(*) FILTER(WHERE banned) banned,COUNT(*) FILTER(WHERE gender='female') girls,COUNT(*) FILTER(WHERE gender='male') boys,COUNT(*) FILTER(WHERE NOT banned) active FROM users",one=True)
    reports=execute("SELECT COUNT(*) n FROM reports WHERE created_at>NOW()-INTERVAL '7 days'",one=True)["n"]
    ch=await channels()
    await update.message.reply_text(
      f"🛠 SecretMeet V7 — Админ-панель\n👥 Всего пользователей: {stats['total']}\n👨 Парней: {stats['boys']}\n👩 Девушек: {stats['girls']}\n🟢 Не забанены: {stats['active']}\n🧾 Заполненные анкеты: {stats['profiles']}\n💎 Активный Премиум: {stats['premium']}\n⛔ Забанены: {stats['banned']}\n🚨 Жалобы за 7 дней: {reports}\n📢 Обязательных каналов: {len(ch)}\n⭐ Цена Премиум: {setting('premium_stars',100)} звёзд / {setting('premium_days',30)} дней",
      reply_markup=InlineKeyboardMarkup([
       [InlineKeyboardButton("📊 Обновить статистику",callback_data="admin:stats")],
       [InlineKeyboardButton("📢 Добавить канал",callback_data="admin:channel_add"),InlineKeyboardButton("🗑 Удалить канал",callback_data="admin:channel_del")],
       [InlineKeyboardButton("📋 Список каналов",callback_data="admin:channel_list")],
       [InlineKeyboardButton("🚨 Последние жалобы",callback_data="admin:reports")],
       [InlineKeyboardButton("📣 Реклама / рассылка",callback_data="admin:broadcast")],
       [InlineKeyboardButton("💎 Выдать Премиум",callback_data="admin:grant"),InlineKeyboardButton("➖ Снять Премиум",callback_data="admin:revoke")],
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
        await q.message.reply_text("Отправьте @username или числовой ID канала, затем название через |. Пример: @mychannel | Мой канал")
    elif data=="admin:channel_del":
        context.user_data["admin_step"]="channel_del"
        await q.message.reply_text("Отправьте точный @username или ID канала для удаления. Для отмены используйте /cancel.")
    elif data=="admin:channel_list":
        rows=await channels()
        await q.message.reply_text("📢 Обязательные каналы:\n"+("\n".join(f"• {r['channel']} — {r['title']}" for r in rows) if rows else "Пока нет каналов."))
    elif data=="admin:reports":
        rows=execute("SELECT r.id,r.reporter_id,r.reported_id,r.reason,u.username FROM reports r LEFT JOIN users u ON u.telegram_id=r.reported_id ORDER BY r.id DESC LIMIT 10",all=True)
        await q.message.reply_text("\n".join(f"#{r['id']} {r['reporter_id']} → {r['reported_id']} (@{r['username']}): {r['reason']}" for r in rows) if rows else "Жалоб нет.")
    elif data=="admin:admin_list":
        rows=execute("SELECT telegram_id FROM admins ORDER BY created_at",all=True)
        await q.message.reply_text("👮 Админы:\n"+"\n".join(f"• {r['telegram_id']}"+(" (главный)" if r['telegram_id']==7659107145 else "") for r in rows))
    elif data in ("admin:broadcast","admin:grant","admin:ban","admin:unban","admin:revoke","admin:prices","admin:admin_add","admin:admin_del"):
        if data in ("admin:admin_add","admin:admin_del") and not owner(uid):
            await q.message.reply_text("Только главный администратор может изменять список администраторов."); return
        action=data.split(":")[1]; context.user_data["admin_step"]=action
        prompts={"broadcast":"Отправьте рекламное сообщение: текст, фото, видео, документ или пост с подписью. Оно будет скопировано пользователям. /cancel — отмена.","grant":"Telegram ID и дни: 123456789 30","ban":"Отправьте Telegram ID для бана.","unban":"Отправьте Telegram ID для разбана.","revoke":"Отправьте Telegram ID, чтобы снять Премиум.","prices":"Отправьте цену и срок Премиум в формате: 100 30 (звёзд и дни).","admin_add":"Отправьте Telegram ID пользователя для добавления админом. Он должен нажать /start.","admin_del":"Отправьте Telegram ID дополнительного админа для удаления."}
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
        await msg.reply_text("Для этого действия отправьте текстовый ответ. Для отмены используйте /cancel."); return True
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
            await msg.reply_text(f"✅ Цена Премиум сохранена: {stars} ⭐ за {days} дней.")
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
            try: await context.bot.send_message(target,"Вам выданы права администратора SecretMeet. Для входа используйте /admin.")
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
            await msg.reply_text("Премиум выдан.")
            try: await context.bot.send_message(target,f"💎 Администратор выдал Премиум на {days} дней.")
            except TelegramError: pass
        except (ValueError,IndexError): await msg.reply_text("Формат: 123456789 30")
        return True
    if action=="revoke":
        try:
            target=int(text); row=execute("UPDATE users SET premium_until=NULL WHERE telegram_id=%s RETURNING telegram_id",(target,),one=True)
            await msg.reply_text("Премиум снят." if row else "Пользователь не найден.")
            if row:
                try: await context.bot.send_message(target,"Срок Премиум был завершён администратором.")
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
    try:
        rows=execute("SELECT telegram_id FROM admins",all=True) or []
        ids={int(r["telegram_id"]) for r in rows} | ADMIN_IDS | {7659107145}
    except Exception:
        ids=ADMIN_IDS | {7659107145}
    for aid in ids:
        try: await context.bot.send_message(aid,message)
        except TelegramError: pass

async def cancel(update,context):
    context.user_data.clear()
    await update.message.reply_text("Отменено.")

def serve(): web.run(host="0.0.0.0",port=PORT,use_reloader=False)

def main():
    # Python 3.14 no longer creates a default event loop automatically.
    # python-telegram-bot 21.x expects one when run_polling() starts.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    init_db()
    threading.Thread(target=serve,daemon=True).start()
    application=Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start",start))
    application.add_handler(CommandHandler("admin",admin))
    application.add_handler(CommandHandler("cancel",cancel))
    application.add_handler(CallbackQueryHandler(cb))
    application.add_handler(PreCheckoutQueryHandler(precheckout))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT,paid))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,route_text))
    application.add_handler(MessageHandler((filters.PHOTO|filters.Sticker.ALL|filters.VOICE|filters.VIDEO|filters.Document.ALL|filters.ANIMATION) & ~filters.COMMAND,route_text))
    log.info("SecretMeet starting")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__=="__main__": main()
