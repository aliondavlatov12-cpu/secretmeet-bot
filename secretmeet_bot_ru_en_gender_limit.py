import os
import logging
import threading
import asyncio
import random
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify
from telegram import (
    Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, LabeledPrice
)
from telegram.constants import ChatType
from telegram.error import TelegramError, BadRequest, NetworkError, TimedOut, RetryAfter
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
    return jsonify({"service": "Анонимный чат знакомства", "status": "ok"}), 200

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
    execute("UPDATE users SET last_active=NOW() WHERE telegram_id=%s", (uid,))
    return get_user(uid)
_SETTING_CACHE = {}
_SETTING_CACHE_LOCK = threading.Lock()

def setting(key, fallback):
    # Price/settings values rarely change; cache briefly to reduce repeated Neon round-trips.
    now = datetime.now(timezone.utc).timestamp()
    with _SETTING_CACHE_LOCK:
        cached = _SETTING_CACHE.get(key)
        if cached and now - cached[0] < 10:
            return cached[1]
    try:
        row = execute("SELECT value FROM app_settings WHERE key=%s", (key,), one=True)
        value = type(fallback)(row["value"]) if row else fallback
        with _SETTING_CACHE_LOCK:
            _SETTING_CACHE[key] = (now, value)
        return value
    except Exception:
        return fallback

def is_admin(uid):
    if uid in ADMIN_IDS or uid == 7659107145: return True
    try: return bool(execute("SELECT 1 FROM admins WHERE telegram_id=%s", (uid,), one=True))
    except Exception: return False

def owner(uid): return uid == 7659107145
def tr(user, tg, ru):
    return tg if (user or {}).get("language") == "en" else ru

# Russian -> English UI dictionary. Applied to every bot-generated outbound message,
# caption, invoice label, and keyboard. User-to-user chat messages bypass this layer.
_EN = {
"🆘 ПОДДЕРЖКА — АНОНИМНЫЙ ЧАТ ЗНАКОМСТВА": "🆘 SUPPORT — ANONYMOUS DATING CHAT", "🔓 Разблокировка аккаунта": "🔓 Account unblocking", "🤝 Сотрудничество и другие вопросы": "🤝 Partnerships and other questions", "Не отправляйте адрес, номер телефона, пароли и финансовые данные. Во время анонимного чата используйте кнопки «Жалоба» и «Блок», если собеседник ведёт себя неподобающе.": "Do not share your address, phone number, passwords, or financial details. Use Report and Block if your chat partner behaves inappropriately.", "💜 Добро пожаловать в «Анонимный чат знакомства»!": "💜 Welcome to Anonymous Dating Chat!", "Создайте анкету, чтобы начать знакомиться.": "Create a profile to start meeting people.", "выберите нужное действие в меню ниже.": "choose an action from the menu below.", "🛍 МАГАЗИН — АНОНИМНЫЙ ЧАТ ЗНАКОМСТВА": "🛍 SHOP — ANONYMOUS DATING CHAT", "🛠 Анонимный чат знакомства — Админ-панель": "🛠 Anonymous Dating Chat — Admin Panel", "💜 Добро пожаловать в «Анонимный чат знакомства»!": "💜 Welcome to Anonymous Dating Chat!", "Анонимный чат знакомства": "Anonymous Dating Chat", "Ваш доступ ограничен администратором.": "Your access has been restricted by an administrator.",
"Панель администратора доступна в меню.": "The admin panel is available in the menu.",
"Сервис знакомств доступен только пользователям старше 18 лет. Подтвердите свой возраст.": "This dating service is available only to users aged 18+. Please confirm your age.",
"Не отправляйте адрес, телефон, пароли или финансовые данные. Используйте кнопки «Жалоба» и «Блок».": "Do not share your address, phone number, passwords, or financial details. Use the Report and Block buttons.",
"Мне уже есть 18 лет": "I am 18 or older", "Мне нет 18 лет": "I am under 18", "Введите возраст (18–99):": "Enter your age (18–99):",
"Язык изменён на русский.": "Language changed to Russian.", "Бот доступен только совершеннолетним (18+).": "This bot is available to adults only (18+).",
"Возраст подтверждён.": "Age confirmed.", "Подписка подтверждена. Давайте создадим вашу анкету.": "Subscription confirmed. Let's create your profile.",
"Подписка подтверждена.": "Subscription confirmed.", "Сначала подпишитесь на все обязательные каналы.": "Please join all required channels first.",
"Чтобы пользоваться ботом, подпишитесь на обязательные каналы и нажмите «✅ Проверить подписку».": "To use the bot, join the required channels and tap “✅ Check subscription”.",
"Чтобы пользоваться ботом, подпишитесь на канал(ы) и нажмите «Проверить подписку».": "To use the bot, join the required channel(s) and tap “Check subscription”.",
"Пока нет доступных анкет.": "No profiles are available right now.", "Ваш пол обновлён.": "Your gender has been updated.", "Главное меню:": "Main menu:",
"Выберите ваш пол:": "Choose your gender:", "Парень": "Man", "Девушка": "Woman", "Назад": "Back", "Кого ищете?": "Who are you looking for?",
"Парня": "Men", "Девушку": "Women", "Всех": "Everyone", "Укажите ваш город:": "Enter your city:", "Введите ваш город:": "Enter your city:",
"Готово. Чат завершён.": "Done. Chat ended.", "Ваш пол обновлён.": "Your gender has been updated.",
"Платёж временно недоступен. Попробуйте позже.": "Payments are temporarily unavailable. Please try again later.",
"Нельзя лайкнуть себя.": "You cannot like your own profile.", "У вас нет Суперлайк. Оформите Премиум или используйте магазин.": "You have no Super Likes. Get Premium or use the shop.",
"Вам отправили Суперлайк!": "You received a Super Like!", "Вам поставили лайк!": "Someone liked your profile!",
"Откройте раздел «❤️ Кто поставил лайк», чтобы посмотреть анкету.": "Open “❤️ Who liked me” to view the profile.", "❤️ Лайк отправлен.": "❤️ Like sent.",
"Сервис доступен только пользователям старше 18 лет.": "This service is available only to users aged 18+.",
"🎉 Анкета готова! Теперь можно знакомиться.": "🎉 Your profile is ready! You can now meet people.",
"Для анкеты нужно отправить фото. Видео можно отправлять уже в анонимном диалоге.": "Please send a photo for your profile. Videos can be shared in an anonymous chat.",
"Возраст должен быть от 18 до 99 лет.": "Age must be between 18 and 99.", "Возраст должен быть 18–99.": "Age must be 18–99.",
"Укажите город (от 2 до 50 символов).": "Enter your city (2–50 characters).", "Напишите несколько слов о себе (до 300 символов):": "Write a few words about yourself (up to 300 characters):",
"Максимум 300 символов.": "Maximum 300 characters.",
"📸 Теперь отправьте фото для анкеты. Оно будет показываться в карточке анкеты. Отправьте именно фото (не файл). В любой момент фото можно заменить через изменение анкеты.": "📸 Now send a profile photo. It will appear on your profile card. Send it as a photo, not as a file. You can replace it later by editing your profile.",
"Пожалуйста, отправьте фото как фотографию в Telegram, а не как документ.": "Please send the image as a Telegram photo, not as a document.",
"Ваша анкета": "Your profile", "Анкета": "Profile", "Описание не указано": "No description provided", "Город не указан": "City not provided",
"Премиум не активен": "Premium is not active", "Бессрочно (администратор)": "Unlimited (administrator)", "дней": "days", "Действует до:": "Active until:",
"Премиум активен": "Premium is active", "ПРЕМИУМ АКТИВЕН": "PREMIUM ACTIVE", "ПРЕМИУМ": "PREMIUM", "МАГАЗИН": "SHOP",
"Безлимитный поиск по полу": "Unlimited gender-based search", "Безлимитный поиск парней и девушек": "Unlimited search for men and women",
"Просмотр лайков": "View likes", "Просмотр тех, кто поставил лайк": "See who liked you", "Суперлайки": "Super Likes", "Продвижение анкеты": "Profile boost", "Приоритет в поиске": "Search priority",
"Случайный чат и просмотр анкет": "Random chat and profile browsing", "Кто поставил лайк": "Who liked me", "Пока никто не поставил лайк вашей анкете. Заполните анкету и продолжайте знакомиться!": "Nobody has liked your profile yet. Complete your profile and keep meeting people!",
"ВАМ ПОСТАВИЛИ ЛАЙК": "SOMEONE LIKED YOU", "лет": "years old", "Выберите нужную функцию в меню ниже.": "Choose a feature from the menu below.",
"После покупки вам будут доступны:": "After purchase, you will get:", "Безлимитный поиск по полу": "Unlimited gender-based search", "Раздел «Кто поставил лайк»": "The “Who liked me” section",
"Суперлайки без дополнительной оплаты": "Super Likes at no extra cost", "Продвижение анкеты без дополнительной оплаты": "Profile boosts at no extra cost", "Приоритет анкеты в поиске": "Priority in search",
"Все функции бота без отдельных платежей": "All bot features with no separate payments", "Без Премиум доступно до 5 поисков по полу в сутки.": "Without Premium, you get up to 5 gender-based searches per day.",
"Оплата проходит безопасно через Telegram Stars. Нажмите кнопку ниже, чтобы купить Премиум.": "Payments are processed securely through Telegram Stars. Tap below to get Premium.",
"Срок действия:": "Valid until:", "Вам доступны все возможности без дополнительной оплаты:": "All these features are included at no extra cost:",
"Откройте все возможности знакомств одной покупкой!": "Unlock all dating features with one purchase!", "ЧТО ВХОДИТ В ПРЕМИУМ": "WHAT PREMIUM INCLUDES",
"Просмотр людей, которые поставили вам лайк": "See people who liked you", "Оплата проходит через Telegram Stars. Бот не запрашивает адрес, телефон, пароль или данные банковской карты.": "Payments use Telegram Stars. The bot never asks for your address, phone number, password, or bank-card details.",
"Нажмите «⭐ Купить Премиум», чтобы активировать доступ на месяц.": "Tap “⭐ Buy Premium” to activate access for a month.",
"Чтобы начать знакомиться.": "to start meeting people.", "Выберите действие в меню ниже 👇": "Choose an action from the menu below 👇",
"Чат завершён.": "Chat ended.", "Собеседник завершил чат.": "Your chat partner ended the chat.", "Вы уже в чате. Сначала завершите текущий чат.": "You are already in a chat. End the current chat first.",
"🔎 Ищем подходящего собеседника…": "🔎 Looking for a match…", "😕 Пока не удалось найти подходящего собеседника. Попробуйте позже или откройте раздел «Анкеты».": "😕 No suitable match found right now. Try again later or browse profiles.",
"Анкета недоступна.": "This profile is unavailable.", "Один из пользователей уже в чате.": "One of the users is already chatting.", "Этот пользователь недоступен.": "This user is unavailable.",
"💬 Вы соединены анонимно! Можете начинать общение.": "💬 You are now connected anonymously! You can start chatting.",
"Отменено.": "Cancelled.", "Недостаточно прав.": "Insufficient permissions.", "Пользователь не найден.": "User not found.", "Пользователь забанен.": "User banned.", "Пользователь разбанен.": "User unbanned.", "Премиум выдан.": "Premium granted.", "Премиум снят.": "Premium removed.",
"ID должен быть числом.": "ID must be a number.", "Telegram ID должен быть числом.": "Telegram ID must be a number.", "Главного администратора нельзя удалить.": "The owner admin cannot be removed.", "Нельзя забанить главного администратора.": "The owner admin cannot be banned.",
"Канал удалён.": "Channel deleted.", "Канал не найден. Сверьте точное значение через /admin.": "Channel not found. Check the exact value with /admin.", "Канал добавлен. Бот должен оставаться администратором канала.": "Channel added. The bot must remain an administrator of the channel.",
"Не удалось проверить канал. Убедитесь, что канал существует и бот добавлен администратором.": "Could not verify the channel. Make sure it exists and the bot is an administrator.",
"Сначала добавьте бота в канал как администратора, затем повторите.": "Add the bot to the channel as an administrator, then try again.", "Жалоб нет.": "No reports.", "Пока нет каналов.": "No channels yet.", "Жалоб нет": "No reports",
"Откройте /admin для обновлённой статистики.": "Open /admin to refresh the statistics.", "Отправьте @username или числовой ID канала, затем название через |. Пример: @mychannel | Мой канал": "Send the channel @username or numeric ID, then its title separated by |. Example: @mychannel | My channel",
"Отправьте точный @username или ID канала для удаления. Для отмены используйте /cancel.": "Send the exact channel @username or ID to delete. Use /cancel to cancel.",
"Для этого действия отправьте текстовый ответ. Для отмены используйте /cancel.": "Send a text reply for this action. Use /cancel to cancel.", "Формат: @channel | Название": "Format: @channel | Title",
"Отправьте рекламное сообщение: текст, фото, видео, документ или пост с подписью. Оно будет скопировано пользователям. /cancel — отмена.": "Send the announcement to broadcast: text, photo, video, document, or a post with a caption. It will be copied to users. /cancel to cancel.",
"Отправьте Telegram ID для бана.": "Send the Telegram ID to ban.", "Отправьте Telegram ID для разбана.": "Send the Telegram ID to unban.", "Отправьте Telegram ID, чтобы снять Премиум.": "Send the Telegram ID to remove Premium.",
"Отправьте цену и срок Премиум в формате: 100 30 (звёзд и дни).": "Send the Premium price and duration in this format: 100 30 (Stars and days).",
"Отправьте Telegram ID пользователя для добавления админом. Он должен нажать /start.": "Send the Telegram ID of the user to add as an admin. They must have pressed /start.",
"Отправьте Telegram ID дополнительного админа для удаления.": "Send the Telegram ID of the additional admin to remove.", "Начинаю рассылку по": "Starting broadcast to", "пользователям…": " users…", "Рассылка завершена. Успешно:": "Broadcast complete. Sent:", "ошибки:": "errors:",
"Формат: 100 30": "Format: 100 30", "Цена Премиум сохранена:": "Premium price saved:", "за": "for", "добавлен в админы.": " added as an admin.", "Права админа для": "Admin permissions removed for", "Вам выданы права администратора. Откройте /start, чтобы увидеть админ-панель.": "You have been granted administrator permissions. Open /start to see the admin panel.",
"Администратор выдал Премиум на": "An administrator granted you Premium for", "Срок Премиум был завершён администратором.": "Your Premium subscription was ended by an administrator.", "Доступ ограничен.": "Access restricted.", "Доступ восстановлен.": "Access restored.",
"Анкета поднята на 1 час бесплатно.": "Your profile has been boosted for 1 hour for free.", "Суперлайк добавлен бесплатно.": "A free Super Like has been added.", "Платёж временно недоступен. Попробуйте позже.": "Payments are temporarily unavailable. Please try again later.",
"Проверить подписку": "Check subscription", "Язык": "Language", "Моя анкета": "My profile", "Изменить пол": "Change gender", "Изменить анкету": "Edit profile",
"Найти парня": "Find a man", "Найти девушку": "Find a woman", "Случайный чат": "Random chat", "Смотреть анкеты": "Browse profiles", "Премиум": "Premium", "Магазин": "Shop", "Помощь": "Help",
"Купить Премиум": "Buy Premium", "Открыть магазин": "Open shop", "Поднять анкету бесплатно": "Boost profile for free", "Получить Суперлайк бесплатно": "Get a free Super Like", "Поднять анкету на 1 час": "Boost profile for 1 hour", "Показать новые лайки": "Reveal new likes",
"Следующий": "Next", "Завершить": "Stop", "Жалоба": "Report", "Блок": "Block", "Начать диалог": "Start chat", "Следующая анкета": "Next profile", "Лайк": "Like", "Админ-панель": "Admin panel",
"Обязательные каналы": "Required channels", "Список каналов": "Channel list", "Последние жалобы": "Recent reports", "Реклама / рассылка": "Broadcast / announcement", "Выдать Премиум": "Grant Premium", "Снять Премиум": "Revoke Premium", "Забанить": "Ban", "Разбанить": "Unban", "Цены магазина": "Shop prices", "Добавить админа": "Add admin", "Удалить админа": "Remove admin", "Список админов": "Admin list", "Обновить статистику": "Refresh statistics", "Добавить канал": "Add channel", "Удалить канал": "Delete channel",
"Поддержка — анонимный чат знакомства": "Support — Anonymous Dating Chat", "Разблокировка аккаунта": "Account unblocking", "Размещение рекламы": "Advertising", "Сотрудничество и другие вопросы": "Partnerships and other questions", "Напишите администратору:": "Contact the administrator:", "Написать администратору": "Contact admin",
"Пока нет доступных анкет.": "No profiles are available right now.", "Сервис доступен только пользователям старше 18 лет.": "This service is available only to users aged 18+.",
"Ваш пол обновлён.": "Your gender has been updated.", "Language set to English.": "Language set to English.", "Выберите язык:": "Choose a language:",
"Найти подходящего собеседника": "Find a suitable chat partner", "Поиск по полу": "Gender-based search", "поисков по полу": "gender-based searches",
"Всего пользователей": "Total users", "Парней": "Men", "Девушек": "Women", "Не забанены": "Not banned", "Заполненные анкеты": "Completed profiles", "Активный Премиум": "Active Premium", "Забанены": "Banned users", "Жалобы за 7 дней": "Reports in the last 7 days", "Обязательных каналов": "Required channels", "Цена Премиум": "Premium price", "звёзд": "Stars", "Пользователь": "User", "Админы": "Admins", "главный": "owner", "Формат": "Format", "Успешно": "Successful", "ошибки": "errors", "ID пользователя": "User ID", "Срок": "Duration", "в сутки": "per day", "Пожалуйста": "Please", "Город": "City", "Описание": "Description", "Выберите": "Choose", "Возраст": "Age", "Ваш": "Your", "ваш": "your", "Пользователь не найден": "User not found", "Анкета не найдена": "Profile not found", "Пока никто": "Nobody yet", "Выберите действие в меню ниже": "Choose an action from the menu below", "Нажмите кнопку ниже": "Tap the button below", "сегодня": "today", "доступно": "available", "бесплатных": "free", "Бесплатно": "Free", "Случайный": "Random", "поиск": "search", "анкета": "profile", "анкеты": "profiles", "чат": "chat", "знакомства": "dating", "доступ": "access", "Администратор": "Administrator", "администратором": "administrator", "Выберите нужное действие в меню ниже.": "Choose an action from the menu below.", "Подписка": "Subscription", "подписку": "subscription", "канал": "channel", "Канал": "Channel", "Каналы": "Channels", "срок действия": "validity period", "Обновить": "Refresh", "Добавить": "Add", "Удалить": "Remove", "Последние": "Recent", "Жалобы": "Reports", "рассылка": "broadcast", "Реклама": "Announcement", "Снять": "Remove", "Выдать": "Grant", "Забанить": "Ban", "Разбанить": "Unban", "Магазин": "Shop", "Профиль": "Profile", "Поддержка": "Support", "платёж": "payment", "Платёж": "Payment", "не удалось": "could not", "Попробуйте позже": "Please try again later", "Пока нет": "There are no", "Нет": "No", "Есть": "Have", "отправьте": "send", "Отправьте": "Send", "Нельзя": "Cannot", "Можно": "Can", "Ваш аккаунт": "Your account", "в чате": "in a chat", "сообщение": "message", "Собеседник": "Chat partner", "завершил": "ended", "Завершён": "Ended", "Открыть": "Open", "купить": "buy", "Купить": "Buy", "бесплатно": "for free", "Бесплатно": "Free", "Новые": "New", "лайк": "like", "Лайк": "Like", "суперлайк": "Super Like", "Суперлайк": "Super Like", "Цена": "Price", "сохранена": "saved", "сохранён": "saved", "Укажите": "Enter", "Напишите": "Write", "Сначала": "First", "затем": "then", "Пример": "Example", "название": "title", "Название": "Title", "для удаления": "to delete", "для добавления": "to add", "Отменено": "Cancelled", "отмены": "cancellation", "используйте": "use", "Премиум успешно активирован": "Premium successfully activated", "Вам доступны": "You have access to", "доступны": "available", "можете начинать общение": "you can start chatting", "соединены анонимно": "connected anonymously", "Имя": "Name", "Описание не указано": "No description provided", "Город не указан": "City not provided",
"👤 Моя анкета": "👤 My profile", "👤 My profile": "👤 My profile", "⚧ Изменить пол": "⚧ Change gender", "✏️ Изменить анкету": "✏️ Edit profile", "🔎 Найти парня": "🔎 Find a man", "🔎 Найти девушку": "🔎 Find a woman", "🎲 Случайный чат": "🎲 Random chat", "❤️ Смотреть анкеты": "❤️ Browse profiles", "💎 Премиум": "💎 Premium", "🛍 Магазин": "🛍 Shop", "🆘 Помощь": "🆘 Help", "❤️ Кто поставил лайк": "❤️ Who liked me", "🛠 Админ-панель": "🛠 Admin panel", "🌐 Language / Язык": "🌐 Language", "⭐ Купить Премиум": "⭐ Buy Premium", "🛍 Открыть магазин": "🛍 Open shop", "❤️ Лайк": "❤️ Like", "💘 Суперлайк": "💘 Super Like", "💬 Начать диалог": "💬 Start chat", "➡️ Следующая анкета": "➡️ Next profile", "🚀 Поднять анкету бесплатно": "🚀 Boost profile for free", "💘 Получить Суперлайк бесплатно": "💘 Get a free Super Like", "🚀 Поднять анкету на 1 час": "🚀 Boost profile for 1 hour", "💘 Суперлайк ×1": "💘 Super Like ×1", "❤️ Показать новые лайки": "❤️ Reveal new likes", "📊 Обновить статистику": "📊 Refresh statistics", "📢 Добавить канал": "📢 Add channel", "🗑 Удалить канал": "🗑 Delete channel", "📋 Список каналов": "📋 Channel list", "🚨 Последние жалобы": "🚨 Recent reports", "📣 Реклама / рассылка": "📣 Broadcast / announcement", "💎 Выдать Премиум": "💎 Grant Premium", "➖ Снять Премиум": "➖ Revoke Premium", "⛔ Забанить": "⛔ Ban", "✅ Разбанить": "✅ Unban", "💰 Цены магазина": "💰 Shop prices", "➕ Добавить админа": "➕ Add admin", "➖ Удалить админа": "➖ Remove admin", "👮 Список админов": "👮 Admin list", "📢 Обязательные каналы": "📢 Required channels", "🇷🇺 Русский": "🇷🇺 Russian", "🇬🇧 English": "🇬🇧 English", "📢 Размещение рекламы": "📢 Advertising", "✉️ Написать администратору": "✉️ Contact admin", "⏭ Следующий": "⏭ Next", "⏹ Завершить": "⏹ Stop", "🚨 Жалоба": "🚨 Report", "🚫 Блок": "🚫 Block", "↩️ Назад": "↩️ Back", "👨 Парень": "👨 Man", "👩 Девушка": "👩 Woman", "Проверить подписку": "Check subscription", "Мне нет 18 лет": "I am under 18", "Мне уже есть 18 лет": "I am 18 or older", "🛠 «Анонимный чат знакомства» — панель администратора доступна в меню.": "🛠 Anonymous Dating Chat — the admin panel is available in the menu.", "👨 Парней:": "👨 Men:", "👩 Девушек:": "👩 Women:", "🟢 Не забанены:": "🟢 Not banned:", "🧾 Заполненные анкеты:": "🧾 Completed profiles:", "💎 Активный Премиум:": "💎 Active Premium:", "⛔ Забанены:": "⛔ Banned users:", "🚨 Жалобы за 7 дней:": "🚨 Reports in the last 7 days:", "📢 Обязательных каналов:": "📢 Required channels:", "⭐ Цена Премиум:": "⭐ Premium price:", "Премиум — Анонимный чат знакомства": "Premium — Anonymous Dating Chat", "Все функции Премиум на": "All Premium features for", "Показать лайки": "Reveal likes", "Открыть список людей, которым понравилась ваша анкета": "Open the list of people who liked your profile", "Анкета поднята на 1 час бесплатно.": "Your profile has been boosted for 1 hour for free.", "Вам отправили Суперлайк!": "You received a Super Like!", "Вам поставили лайк!": "Someone liked your profile!", "Откройте раздел «❤️ Кто поставил лайк», чтобы посмотреть анкету.": "Open “❤️ Who liked me” to view the profile.",
}

# Longer phrases are replaced first to avoid partial replacements corrupting the translation.
def localize_text(text, lang):
    if not isinstance(text, str) or lang != "en":
        return text
    if text in _EN:
        return _EN[text]
    protected=[]
    def hold(match_text):
        protected.append(match_text)
        return f"\uE100{len(protected)-1}\uE101"
    import re
    text=re.sub(r"\uE000.*?\uE001", lambda m: hold(m.group(0)), text, flags=re.S)
    # Translate full phrases and multiword labels, never generic short words.
    for ru in sorted((k for k in _EN if len(k) >= 12), key=len, reverse=True):
        text = text.replace(ru, _EN[ru])
    for i, value in enumerate(protected):
        text=text.replace(f"\uE100{i}\uE101", value.replace("\uE000", "").replace("\uE001", ""))
    return text

_ORIGINAL_BOT_METHODS = {}
_LANGUAGE_CACHE = {}
_LANGUAGE_CACHE_LOCK = threading.Lock()
def _install_outbound_localization():
    """Translate bot-authored text and keyboard labels using the recipient's saved language."""
    methods = ("send_message", "edit_message_text", "send_photo", "send_video", "send_voice", "send_document", "send_animation", "send_invoice")
    for method_name in methods:
        original = getattr(Bot, method_name, None)
        if original is None or method_name in _ORIGINAL_BOT_METHODS:
            continue
        _ORIGINAL_BOT_METHODS[method_name] = original
        async def wrapped(self, *args, __original=original, __method=method_name, **kwargs):
            # send_message/media methods start with chat_id; edit_message_text starts with text.
            if __method == "edit_message_text":
                chat_id = kwargs.get("chat_id", args[1] if len(args) > 1 else None)
            else:
                chat_id = kwargs.get("chat_id", args[0] if args else None)
            lang = "ru"
            try:
                if chat_id is not None:
                    uid_for_language=int(chat_id)
                    with _LANGUAGE_CACHE_LOCK:
                        cached_language = uid_for_language in _LANGUAGE_CACHE
                        lang = _LANGUAGE_CACHE.get(uid_for_language, "ru")
                    if not cached_language and "execute" in globals():
                        row = execute("SELECT language FROM users WHERE telegram_id=%s", (uid_for_language,), one=True)
                        if row:
                            lang = row.get("language") or "ru"
                            with _LANGUAGE_CACHE_LOCK: _LANGUAGE_CACHE[uid_for_language] = lang
            except Exception:
                pass
            if __method == "send_invoice":
                for key in ("title", "description"):
                    if key in kwargs: kwargs[key] = localize_text(kwargs[key], lang)
                if "prices" in kwargs:
                    kwargs["prices"] = [LabeledPrice(label=localize_text(x.label, lang), amount=x.amount) for x in kwargs["prices"]]
            else:
                text_key = "caption" if __method in ("send_photo", "send_video", "send_voice", "send_document", "send_animation") else "text"
                if text_key in kwargs: kwargs[text_key] = localize_text(kwargs[text_key], lang)
                elif args:
                    # PTB positional indexes: send_message(chat_id,text), edit_message_text(text,chat_id,message_id).
                    idx = 1 if __method == "send_message" else (0 if __method == "edit_message_text" else None)
                    if idx is not None and len(args) > idx:
                        args = list(args); args[idx] = localize_text(args[idx], lang); args = tuple(args)
            markup = kwargs.get("reply_markup")
            if markup is not None and lang == "en":
                try:
                    if isinstance(markup, InlineKeyboardMarkup):
                        markup = InlineKeyboardMarkup([[InlineKeyboardButton(localize_text(b.text, lang), url=b.url, callback_data=b.callback_data, web_app=b.web_app, login_url=b.login_url, switch_inline_query=b.switch_inline_query, switch_inline_query_current_chat=b.switch_inline_query_current_chat, callback_game=b.callback_game, pay=b.pay) for b in row] for row in markup.inline_keyboard])
                    elif isinstance(markup, ReplyKeyboardMarkup):
                        markup = ReplyKeyboardMarkup([[KeyboardButton(localize_text(b.text, lang), request_contact=b.request_contact, request_location=b.request_location, request_poll=b.request_poll, web_app=b.web_app, request_users=b.request_users, request_chat=b.request_chat) for b in row] for row in markup.keyboard], resize_keyboard=markup.resize_keyboard, one_time_keyboard=markup.one_time_keyboard, selective=markup.selective, input_field_placeholder=markup.input_field_placeholder, is_persistent=markup.is_persistent)
                    kwargs["reply_markup"] = markup
                except Exception as exc:
                    log.debug("Keyboard localization fallback: %s", exc)
            return await __original(self, *args, **kwargs)
        setattr(Bot, method_name, wrapped)

_install_outbound_localization()

def premium(user): return bool(user and user.get("premium_until") and user["premium_until"] > datetime.now(timezone.utc))

def premium_until_text(user):
    until = user.get("premium_until") if user else None
    en=(user or {}).get("language")=="en"
    if not until or until <= datetime.now(timezone.utc):
        return "Premium is not active" if en else "Премиум не активен"
    stamp=until.astimezone(timezone.utc).strftime("%d.%m.%Y at %H:%M UTC" if en else "%d.%m.%Y в %H:%M UTC")
    return stamp

def main_kb(u):
    en = (u.get("language") or "ru") == "en"
    rows = [
      [KeyboardButton("🔎 Find a man" if en else "🔎 Найти парня"), KeyboardButton("🔎 Find a woman" if en else "🔎 Найти девушку")],
      [KeyboardButton("🎲 Random chat" if en else "🎲 Случайный чат"), KeyboardButton("❤️ Browse profiles" if en else "❤️ Смотреть анкеты")],
      [KeyboardButton("👤 My profile" if en else "👤 Моя анкета"), KeyboardButton("💎 Premium" if en else "💎 Премиум")],
      [KeyboardButton("❤️ Who liked me" if en else "❤️ Кто поставил лайк"), KeyboardButton("🛍 Shop" if en else "🛍 Магазин")],
      [KeyboardButton("🌐 Language" if en else "🌐 Язык")],
      [KeyboardButton("🆘 Help" if en else "🆘 Помощь")],
    ]
    if is_admin(u["telegram_id"]):
        rows.append([KeyboardButton("🛠 Админ-панель")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)

def chat_kb(u):
    en=(u.get("language") or "ru")=="en"
    return InlineKeyboardMarkup([
      [InlineKeyboardButton("⏭ Next" if en else "⏭ Следующий",callback_data="chat:next"),
       InlineKeyboardButton("⏹ Stop" if en else "⏹ Завершить",callback_data="chat:stop")],
      [InlineKeyboardButton("🚨 Report" if en else "🚨 Жалоба",callback_data="chat:report"),
       InlineKeyboardButton("🚫 Block" if en else "🚫 Блок",callback_data="chat:block")]
    ])

def shop_markup(u=None):
    u = u or {}
    en=(u.get("language") or "ru")=="en"
    ps=setting("premium_stars",100); pd=setting("premium_days",30)
    bs=setting("boost_stars",BOOST_STARS); ss=setting("superlike_stars",SUPERLIKE_STARS); ls=setting("reveal_likes_stars",REVEAL_LIKES_STARS)
    return InlineKeyboardMarkup([
      [InlineKeyboardButton((f"💎 Premium — {ps} ⭐ / {pd} days" if en else f"💎 Премиум — {ps} ⭐ / {pd} дней"),callback_data="shop:premium")],
      [InlineKeyboardButton((f"🚀 Boost profile for 1 hour — {bs} ⭐" if en else f"🚀 Поднять анкету на 1 час — {bs} ⭐"),callback_data="shop:boost")],
      [InlineKeyboardButton((f"💘 Super Like ×1 — {ss} ⭐" if en else f"💘 Суперлайк ×1 — {ss} ⭐"),callback_data="shop:superlike")],
      [InlineKeyboardButton((f"❤️ Reveal new likes — {ls} ⭐" if en else f"❤️ Показать новые лайки — {ls} ⭐"),callback_data="shop:likes")]
    ])

def shop_text(u):
    en=(u.get("language") or "ru")=="en"
    active = premium(u) or is_admin(u["telegram_id"])
    if active:
        expiry = ("Unlimited (administrator)" if en else "Бессрочно (администратор)") if is_admin(u["telegram_id"]) and not premium(u) else premium_until_text(u)
        if en:
            return ("🛍 SHOP — ANONYMOUS DATING CHAT\n\n"
                    "💎 YOUR PREMIUM IS ACTIVE\n\n"
                    f"📅 Valid until: {expiry}\n\n"
                    "✨ Included at no extra cost:\n"
                    "• 🔎 Unlimited gender-based search\n• ❤️ See who liked you\n• 💘 Super Likes\n"
                    "• 🚀 Profile boosts\n• ⭐ Search priority\n• 🎲 Random chat and profile browsing\n\n"
                    "Choose a feature from the menu below.")
        return ("🛍 МАГАЗИН — АНОНИМНЫЙ ЧАТ ЗНАКОМСТВА\n\n"
                "💎 ВАШ ПРЕМИУМ АКТИВЕН\n\n"
                f"📅 Действует до: {expiry}\n\n"
                "✨ Все функции Премиум уже включены и не требуют доплаты:\n"
                "• 🔎 Безлимитный поиск парней и девушек\n• ❤️ Просмотр тех, кто поставил лайк\n• 💘 Суперлайки\n"
                "• 🚀 Продвижение анкеты\n• ⭐ Приоритет в поиске\n• 🎲 Случайный чат и просмотр анкет\n\n"
                "Выберите нужную функцию в меню ниже.")
    row=execute("SELECT COUNT(*) n FROM likes WHERE to_id=%s AND from_id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=%s)",(u["telegram_id"],u["telegram_id"]),one=True)
    if en:
        return ("🛍 SHOP — ANONYMOUS DATING CHAT\n\n"
                f"💎 PREMIUM: {setting('premium_stars',100)} ⭐ for {setting('premium_days',30)} days.\n\n"
                "After purchase, you get:\n✅ Unlimited gender-based search\n✅ See who liked you\n"
                "✅ Super Likes at no extra cost\n✅ Free profile boosts\n✅ Search priority\n✅ All bot features without separate payments\n\n"
                "🆓 Without Premium, you get up to 5 gender-based searches per day.\n"
                f"❤️ Likes on your profile: {row['n']}.\n\n"
                "Payments are processed through Telegram Stars. Tap a button below to get Premium.")
    return ("🛍 МАГАЗИН — АНОНИМНЫЙ ЧАТ ЗНАКОМСТВА\n\n"
            f"💎 ПРЕМИУМ: {setting('premium_stars',100)} ⭐ на {setting('premium_days',30)} дней.\n\n"
            "После покупки вам будут доступны:\n✅ Безлимитный поиск по полу\n✅ Раздел «Кто поставил лайк»\n"
            "✅ Суперлайки без дополнительной оплаты\n✅ Продвижение анкеты без дополнительной оплаты\n✅ Приоритет анкеты в поиске\n"
            "✅ Все функции бота без отдельных платежей\n\n🆓 Без Премиум доступно до 5 поисков по полу в сутки.\n"
            f"❤️ Сейчас лайков у вашей анкеты: {row['n']}.\n\n"
            "Оплата проходит через Telegram Stars. Нажмите кнопку ниже, чтобы купить Премиум.")

def premium_text(u):
    en=(u.get("language") or "ru")=="en"
    active = premium(u) or is_admin(u["telegram_id"])
    if active:
        expiry = ("Unlimited (administrator)" if en else "Бессрочно (администратор)") if is_admin(u["telegram_id"]) and not premium(u) else premium_until_text(u)
        if en:
            return ("💎 PREMIUM ACTIVE\n\n" f"📅 Valid until: {expiry}\n\n"
                    "Included at no extra cost:\n🔎 Unlimited gender-based search\n❤️ View likes\n💘 Super Likes\n🚀 Profile boosts\n⭐ Search priority\n🎲 Random chat and profile browsing\n\n"
                    "Tap “❤️ Who liked me” to view likes.")
        return ("💎 ПРЕМИУМ АКТИВЕН\n\n" f"📅 Срок действия: {expiry}\n\n"
                "Вам доступны все возможности без дополнительной оплаты:\n🔎 Безлимитный поиск по полу\n❤️ Просмотр лайков\n💘 Суперлайки\n🚀 Продвижение анкеты\n⭐ Приоритет в поиске\n🎲 Случайный чат и просмотр анкет\n\n"
                "Нажмите «❤️ Кто поставил лайк», чтобы посмотреть лайки.")
    if en:
        return (f"💎 PREMIUM — {setting('premium_stars',100)} ⭐ / {setting('premium_days',30)} days\n\n"
                "Unlock all dating features with one purchase!\n\n✨ WHAT'S INCLUDED\n🔎 Unlimited search for men and women\n❤️ See people who liked you\n"
                "💘 Super Likes at no extra cost\n🚀 Free profile boosts\n⭐ Search priority\n🎲 Random chat and profile browsing\n\n"
                "🛡️ Payment is handled by Telegram Stars. The bot never asks for your address, phone number, password, or bank-card details.\n\n"
                "Tap “⭐ Buy Premium” to activate access.")
    return (f"💎 ПРЕМИУМ — {setting('premium_stars',100)} ⭐ / {setting('premium_days',30)} дней\n\n"
            "Откройте все возможности знакомств одной покупкой!\n\n✨ ЧТО ВХОДИТ В ПРЕМИУМ\n🔎 Безлимитный поиск парней и девушек\n❤️ Просмотр людей, которые поставили вам лайк\n"
            "💘 Суперлайки без отдельной оплаты\n🚀 Продвижение анкеты без отдельной оплаты\n⭐ Приоритет анкеты в поиске\n🎲 Случайный чат и просмотр анкет\n\n"
            "🛡️ Оплата проходит через Telegram Stars. Бот не запрашивает адрес, телефон, пароль или данные банковской карты.\n\n"
            "Нажмите «⭐ Купить Премиум», чтобы активировать доступ.")

def likes_text(uid):
    user=get_user(uid); en=(user or {}).get("language")=="en"
    rows=execute("""SELECT u.telegram_id,u.age,u.gender,u.city,u.bio FROM likes l JOIN users u ON u.telegram_id=l.from_id
        WHERE l.to_id=%s AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.blocker_id=%s AND b.blocked_id=l.from_id)
        ORDER BY l.created_at DESC LIMIT 30""",(uid,uid),all=True) or []
    if not rows:
        return ("❤️ Nobody has liked your profile yet. Complete your profile and keep meeting people!" if en else
                "❤️ Пока никто не поставил лайк вашей анкете. Заполните анкету и продолжайте знакомиться!")
    if en:
        return "❤️ PEOPLE WHO LIKED YOU\n\n"+"\n\n".join(
            f"👤 {r['age'] or '—'} years old • \uE000{r['city'] or 'City not provided'}\uE001\n\uE000{(r['bio'] or 'No description provided')[:180]}\uE001" for r in rows)
    return "❤️ ВАМ ПОСТАВИЛИ ЛАЙК\n\n"+"\n\n".join(
        f"👤 {r['age'] or '—'} лет • \uE000{r['city'] or 'Город не указан'}\uE001\n\uE000{(r['bio'] or 'Описание не указано')[:180]}\uE001" for r in rows)

def profile_text(p, own=False, language=None):
    en=(language or p.get("language") or "ru")=="en"
    gender={"male":("Man" if en else "Парень"),"female":("Woman" if en else "Девушка")}
    title=("👤 Your profile" if own else "💌 Profile") if en else ("👤 Ваша анкета" if own else "💌 Анкета")
    no_city="City not provided" if en else "Город не указан"
    no_bio="No description provided" if en else "Описание не указано"
    # Protect user-entered city and bio from phrase-based UI localization.
    city=f"\uE000{p.get('city') or no_city}\uE001"
    bio=f"\uE000{p.get('bio') or no_bio}\uE001"
    return f"{title}\n🎂 {p.get('age') or '—'}\n👤 {gender.get(p.get('gender'), '—')}\n📍 {city}\n\n{bio}"

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
        await update.message.reply_text("🛠 «Анонимный чат знакомства» — панель администратора доступна в меню.", reply_markup=main_kb(u))
        return
    if not u["is_adult"]:
        await update.message.reply_text(
          "💜 Добро пожаловать в «Анонимный чат знакомства»!\n\nСервис знакомств доступен только пользователям старше 18 лет. Подтвердите свой возраст.\n\nНе отправляйте адрес, телефон, пароли или финансовые данные. Используйте кнопки «Жалоба» и «Блок».",
          reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Мне уже есть 18 лет",callback_data="age:yes")],
            [InlineKeyboardButton("❌ Мне нет 18 лет",callback_data="age:no")]
          ]))
    elif not u["profile_ready"]:
        await update.message.reply_text("💜 Анонимный чат знакомства\nСоздайте анкету, чтобы начать знакомиться.")
        await begin_profile(update,context)
    else:
        await update.message.reply_text("💜 «Анонимный чат знакомства» — выберите нужное действие в меню ниже.",reply_markup=main_kb(u))

async def begin_profile(update,context):
    uid=update.effective_user.id; u=get_user(uid)
    context.user_data.clear(); context.user_data["step"]="age"
    await update.effective_message.reply_text("Введите возраст (18–99):")

async def safe_answer(query, text=None, show_alert=False):
    """Answer callback queries without letting expired Telegram callbacks break a handler."""
    if query is None:
        return False
    try:
        await query.answer(text=text, show_alert=show_alert)
        return True
    except BadRequest as exc:
        msg = str(exc).lower()
        if any(term in msg for term in ("query is too old", "query id is invalid", "query to be answered")):
            log.info("Ignoring expired callback query: %s", exc)
        else:
            log.warning("Callback answer rejected by Telegram: %s", exc)
        return False
    except TelegramError as exc:
        log.warning("Could not answer callback query: %s", exc)
        return False

async def cb(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    if q is None:
        return
    await safe_answer(q)
    uid=q.from_user.id; u=ensure_user(uid); data=q.data or ""
    if data.startswith("lang:"):
        lang=data.split(":",1)[1]
        if lang not in ("ru", "en"): lang="ru"
        execute("UPDATE users SET language=%s WHERE telegram_id=%s",(lang,uid))
        with _LANGUAGE_CACHE_LOCK: _LANGUAGE_CACHE[uid] = lang
        u=get_user(uid)
        await q.message.reply_text("Language set to English." if lang=="en" else "Язык изменён на русский.",reply_markup=main_kb(u)); return
    if data=="age:no":
        await q.edit_message_text("Бот доступен только совершеннолетним (18+)."); return
    if data=="age:yes":
        execute("UPDATE users SET is_adult=TRUE WHERE telegram_id=%s",(uid,))
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
            await safe_answer(q, "Please join all required channels first." if u.get("language")=="en" else "Сначала подпишитесь на все обязательные каналы.", show_alert=True)
        return
    if data.startswith("admin:"):
        await admin_cb(update,context,data); return
    if not u["is_adult"] or u["banned"]: return
    if data=="profile:gender":
        en=(u.get("language") or "ru")=="en"
        await q.message.reply_text("Choose your gender:" if en else "Выберите ваш пол:",reply_markup=InlineKeyboardMarkup([
          [InlineKeyboardButton("👨 Man" if en else "👨 Парень",callback_data="profile:setgender:male"),InlineKeyboardButton("👩 Woman" if en else "👩 Девушка",callback_data="profile:setgender:female")],
          [InlineKeyboardButton("↩️ Back" if en else "↩️ Назад",callback_data="profile:back")]])); return
    if data.startswith("profile:setgender:"):
        g=data.rsplit(":",1)[1]
        if g not in ("male","female"): return
        execute("UPDATE users SET gender=%s WHERE telegram_id=%s",(g,uid))
        en=(u.get("language") or "ru")=="en"
        await q.message.reply_text("Your gender has been updated." if en else "Ваш пол обновлён.",reply_markup=main_kb(get_user(uid))); return
    if data=="profile:back":
        await q.message.reply_text("Main menu:" if u.get("language")=="en" else "Главное меню:",reply_markup=main_kb(u)); return
    if data=="profile:edit":
        await begin_profile(update,context); return
    if data.startswith("gender:"):
        g=data.split(":")[1]
        execute("UPDATE users SET gender=%s WHERE telegram_id=%s",(g,uid))
        en=(u.get("language") or "ru")=="en"
        await q.edit_message_text("Who are you looking for?" if en else "Кого ищете?",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Men" if en else "Парня",callback_data="looking:male"),
                                               InlineKeyboardButton("Women" if en else "Девушку",callback_data="looking:female"),
                                               InlineKeyboardButton("Everyone" if en else "Всех",callback_data="looking:any")]])); return
    if data.startswith("looking:"):
        execute("UPDATE users SET looking_for=%s WHERE telegram_id=%s",(data.split(":")[1],uid))
        context.user_data["step"]="city"
        en=(u.get("language") or "ru")=="en"
        await q.edit_message_text("Enter your city:" if en else "Укажите ваш город:"); return
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
            await q.message.reply_text(shop_text(current),reply_markup=shop_markup(current))
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
                await q.message.reply_invoice(title=labels[item],description="Цифровая услуга «Анонимный чат знакомства»",
                    payload=f"item:{uid}:{item}",provider_token="",currency="XTR",
                    prices=[LabeledPrice(label=labels[item],amount=prices[item])])
            except TelegramError:
                await q.message.reply_text("Платёж временно недоступен. Попробуйте позже.")
            return
    if data.startswith("profile:like:") or data.startswith("profile:super:"):
        target=int(data.split(":")[-1]); superlike=data.startswith("profile:super:")
        if target==uid:
            await safe_answer(q, "You cannot like your own profile." if u.get("language")=="en" else "Нельзя лайкнуть себя.", show_alert=True); return
        if superlike:
            u=get_user(uid)
            if not (premium(u) or is_admin(uid)) and int(u.get("super_likes") or 0)<=0:
                await safe_answer(q, "You have no Super Likes. Get Premium or use the shop." if u.get("language")=="en" else "У вас нет Суперлайк. Оформите Премиум или используйте магазин.", show_alert=True); return
            if not (premium(u) or is_admin(uid)):
                execute("UPDATE users SET super_likes=GREATEST(0,super_likes-1) WHERE telegram_id=%s",(uid,))
        execute("INSERT INTO likes(from_id,to_id) VALUES(%s,%s) ON CONFLICT(from_id,to_id) DO NOTHING",(uid,target))
        try:
            await context.bot.send_message(target,("💘 Вам отправили Суперлайк!" if superlike else "❤️ Вам поставили лайк!") + "\nОткройте раздел «❤️ Кто поставил лайк», чтобы посмотреть анкету.")
        except TelegramError: pass
        await safe_answer(q, ("Super Like sent!" if superlike else "Like sent!") if u.get("language")=="en" else ("Суперлайк отправлен!" if superlike else "Лайк отправлен!"))
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
            en=(u.get("language") or "ru")=="en"
            days=setting("premium_days",30)
            await q.message.reply_invoice(title=("Premium — Anonymous Dating Chat" if en else "Премиум — Анонимный чат знакомства"),description=(f"All Premium features for {days} days" if en else f"Все функции Премиум на {days} дней"),
                payload=f"premium:{uid}:{days}",provider_token="",currency="XTR",
                prices=[LabeledPrice(label=(f"Premium for {days} days" if en else f"Премиум на {days} дней"),amount=setting("premium_stars",100))])
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
        await update.message.reply_text("Choose your gender:" if u.get("language")=="en" else "Выберите ваш пол:",reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👨 Man" if u.get("language")=="en" else "👨 Парень",callback_data="gender:male"),
            InlineKeyboardButton("👩 Woman" if u.get("language")=="en" else "👩 Девушка",callback_data="gender:female")]])); return
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

    if text in ("👤 Моя анкета", "👤 My profile"):

        if u.get("profile_photo"):
            await update.message.reply_photo(u["profile_photo"],caption=profile_text(u,True),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚧ Изменить пол" if u.get("language")!="en" else "⚧ Change gender",callback_data="profile:gender")],[InlineKeyboardButton("✏️ Изменить анкету" if u.get("language")!="en" else "✏️ Edit profile",callback_data="profile:edit")]]))
        else:
            await update.message.reply_text(profile_text(u,True),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚧ Изменить пол" if u.get("language")!="en" else "⚧ Change gender",callback_data="profile:gender")],[InlineKeyboardButton("✏️ Изменить анкету" if u.get("language")!="en" else "✏️ Edit profile",callback_data="profile:edit")]]))
        return
    if text in ("🛍 Магазин", "🛍 Shop"):

        current=get_user(uid)
        if premium(current) or is_admin(uid):
            buttons=[[InlineKeyboardButton("❤️ Кто поставил лайк",callback_data="likes:view")],
                     [InlineKeyboardButton("🚀 Поднять анкету бесплатно",callback_data="shop:boost")],
                     [InlineKeyboardButton("💘 Получить Суперлайк бесплатно",callback_data="shop:superlike")]]
            await update.message.reply_text(shop_text(current),reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await update.message.reply_text(shop_text(current),reply_markup=shop_markup(current))
        return
    if text in ("💎 Премиум", "💎 Premium"):

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
    if text in ("❤️ Кто поставил лайк", "❤️ Who liked me"):

        if not (premium(u) or is_admin(uid)):
            await update.message.reply_text(premium_text(u),reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Купить Премиум",callback_data="premium:buy")]])); return
        await update.message.reply_text(likes_text(uid),reply_markup=main_kb(u)); return
    if text in ("🛠 Админ-панель", "🛠 Admin panel") and is_admin(uid):
        await admin(update,context); return
    if text in ("🌐 Language / Язык", "🌐 Language", "🌐 Язык"):
        await update.message.reply_text("Choose language / Выберите язык:",reply_markup=InlineKeyboardMarkup([
          [InlineKeyboardButton("🇷🇺 Русский",callback_data="lang:ru"),InlineKeyboardButton("🇬🇧 English",callback_data="lang:en")]])); return
    if text in ("🆘 Помощь", "🆘 Help"):
        await update.message.reply_text("🆘 ПОДДЕРЖКА — АНОНИМНЫЙ ЧАТ ЗНАКОМСТВА\n\n"
          "🔓 Разблокировка аккаунта\n📢 Размещение рекламы\n🤝 Сотрудничество и другие вопросы\n\nНапишите администратору: @ffxdavlatov\n\n"
          "Не отправляйте адрес, номер телефона, пароли и финансовые данные. Во время анонимного чата используйте кнопки «Жалоба» и «Блок», если собеседник ведёт себя неподобающе.",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✉️ Написать администратору",url="https://t.me/ffxdavlatov")]])); return
    if not await subscription_gate(update,context,u): return
    if not u["profile_ready"]:
        await begin_profile(update,context); return
    if text in ("🔎 Найти парня","🔎 Найти девушку","🎲 Случайный чат", "🔎 Find a man", "🔎 Find a woman", "🎲 Random chat"):
        wanted="male" if text in ("🔎 Найти парня", "🔎 Find a man") else ("female" if text in ("🔎 Найти девушку", "🔎 Find a woman") else None)
        await find_match(update,context,wanted); return
    if text in ("❤️ Смотреть анкеты", "❤️ Browse profiles"):

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
    # Relay user content verbatim; UI localization must never rewrite private chat messages.
    raw_send = _ORIGINAL_BOT_METHODS.get("send_message", Bot.send_message)
    raw_photo = _ORIGINAL_BOT_METHODS.get("send_photo", Bot.send_photo)
    raw_video = _ORIGINAL_BOT_METHODS.get("send_video", Bot.send_video)
    raw_voice = _ORIGINAL_BOT_METHODS.get("send_voice", Bot.send_voice)
    raw_document = _ORIGINAL_BOT_METHODS.get("send_document", Bot.send_document)
    raw_animation = _ORIGINAL_BOT_METHODS.get("send_animation", Bot.send_animation)
    if m.text: await raw_send(context.bot, partner, m.text)
    elif m.photo: await raw_photo(context.bot, partner, m.photo[-1].file_id, caption=m.caption or "")
    elif m.sticker: await context.bot.send_sticker(partner,m.sticker.file_id)
    elif m.voice: await raw_voice(context.bot, partner, m.voice.file_id, caption=m.caption or "")
    elif m.video: await raw_video(context.bot, partner, m.video.file_id, caption=m.caption or "")
    elif m.document: await raw_document(context.bot, partner, m.document.file_id, caption=m.caption or "")
    elif m.animation: await raw_animation(context.bot, partner, m.animation.file_id, caption=m.caption or "")

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
    if not u["profile_ready"]:
        await begin_profile(update,context); return
    if not is_admin(uid) and not await subscription_gate(update,context,u): return
    if await get_partner(uid):
        en=(u.get("language") or "ru")=="en"
        await update.effective_message.reply_text("You are already in a chat. Stop the current chat first." if en else "Вы уже в чате. Сначала завершите текущий чат.",reply_markup=chat_kb(u)); return

    # Only explicit gender searches count toward the free daily quota. Random chat is unlimited.
    gender_search = wanted in ("male", "female")
    if gender_search and not (premium(u) or is_admin(uid)):
        local_day=datetime.now(ZoneInfo("Asia/Dushanbe")).date()
        usage=execute("SELECT searches FROM search_usage WHERE telegram_id=%s AND search_date=%s",(uid,local_day),one=True)
        used=int(usage["searches"]) if usage else 0
        # Atomic upsert prevents rapid repeated taps from exceeding the five-search quota.
        counted=execute("INSERT INTO search_usage(telegram_id,search_date,searches) VALUES(%s,%s,1) ON CONFLICT(telegram_id,search_date) DO UPDATE SET searches=search_usage.searches+1 WHERE search_usage.searches < 5 RETURNING searches",(uid,local_day),one=True)
        if not counted:
            en=(u.get("language") or "ru")=="en"
            message=(f"🔒 You have reached today's limit of 5 gender-based searches.\n\n💎 Premium ({setting('premium_stars',100)} ⭐ / {setting('premium_days',30)} days) unlocks unlimited gender search." if en else
                f"🔒 Лимит на сегодня исчерпан: доступно 5 поисков по полу в сутки.\n\n💎 Премиум ({setting('premium_stars',100)} ⭐ на {setting('premium_days',30)} дней) открывает безлимитный поиск по полу.")
            await update.effective_message.reply_text(message,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Buy Premium" if en else "⭐ Купить Премиум",callback_data="premium:buy")]])); return
        used_before=used
        remaining=max(0,4-used_before)
    else:
        remaining=None

    en=(u.get("language") or "ru")=="en"
    status_message=await update.effective_message.reply_text("🔎 Looking for a suitable chat partner…" if en else "🔎 Ищем подходящего собеседника…")
    await context.bot.send_chat_action(chat_id=uid, action="typing")
    # Explicit gender selection overrides the profile's general search preference.
    # Random chat is unrestricted by gender preference; candidate must still accept this user's gender.
    # Random chat selects a gender with equal probability, then falls back to the other gender if needed.
    targets = [wanted] if wanted else [random.choice(("male", "female"))]
    if not wanted:
        targets.append("female" if targets[0] == "male" else "male")
    row=None
    for target_gender in targets:
        row=execute("""SELECT x.telegram_id FROM users x
          WHERE x.profile_ready=TRUE AND x.is_adult=TRUE AND x.banned=FALSE AND x.telegram_id<>%s
          AND x.gender=%s
          AND (x.looking_for='any' OR x.looking_for=%s)
          AND (%s='any' OR x.gender=%s)
          AND NOT EXISTS(SELECT 1 FROM blocks b WHERE (b.blocker_id=%s AND b.blocked_id=x.telegram_id) OR (b.blocker_id=x.telegram_id AND b.blocked_id=%s))
          AND NOT EXISTS(SELECT 1 FROM active_chats a WHERE a.user_id=x.telegram_id OR a.partner_id=x.telegram_id)
          ORDER BY CASE WHEN x.premium_until>NOW() THEN 0 ELSE 1 END, RANDOM() LIMIT 1""",
          (uid,target_gender,u["gender"],candidate_preference,candidate_preference,uid,uid),one=True)
        if row: break
    if not row:
        suffix = (f"\n\nGender searches remaining today: {remaining}." if en else f"\n\nСегодня осталось поисков по полу: {remaining}.") if remaining is not None else ""
        message=("😕 No suitable chat partner is available right now. Please try again later or browse profiles." if en else "😕 Сейчас не удалось найти подходящего собеседника. Попробуйте позже или откройте анкеты.") + suffix
        try: await status_message.edit_text(message)
        except TelegramError: pass
        return
    try: await status_message.delete()
    except TelegramError: pass
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
        await update.effective_message.reply_photo(row["profile_photo"],caption=profile_text(row, language=u.get("language")),reply_markup=markup)
    else:
        await update.effective_message.reply_text(profile_text(row, language=u.get("language")),reply_markup=markup)

async def precheckout(update,context):
    await update.pre_checkout_query.answer(ok=True)

async def paid(update,context):
    p=update.message.successful_payment; uid=update.effective_user.id
    payload=p.invoice_payload
    if payload.startswith(f"premium:{uid}:"):
        try: days=max(1,min(365,int(payload.split(":")[-1])))
        except ValueError: days=PREMIUM_DAYS
        # Record the unique charge first; repeated Telegram updates must not extend a subscription twice.
        recorded = execute("INSERT INTO payments(telegram_id,charge_id,currency,amount) VALUES(%s,%s,%s,%s) ON CONFLICT(charge_id) DO NOTHING RETURNING charge_id",
          (uid,p.telegram_payment_charge_id,p.currency,p.total_amount), one=True)
        if not recorded:
            await update.message.reply_text("✅ This payment has already been processed." if get_user(uid).get("language")=="en" else "✅ Этот платёж уже обработан.", reply_markup=main_kb(get_user(uid)))
            return
        u=get_user(uid); now=datetime.now(timezone.utc); start=u["premium_until"] if u["premium_until"] and u["premium_until"]>now else now
        until=start+timedelta(days=days)
        execute("UPDATE users SET premium_until=%s WHERE telegram_id=%s",(until,uid))
        await update.message.reply_text(f"🎉 Премиум успешно активирован до {until:%d.%m.%Y %H:%M UTC}!\n\nВам доступны без дополнительной оплаты: безлимитный поиск, просмотр лайков, Суперлайки, продвижение анкеты и приоритет в поиске.",reply_markup=main_kb(get_user(uid)))
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
      f"🛠 Анонимный чат знакомства — Админ-панель\n👥 Всего пользователей: {stats['total']}\n👨 Парней: {stats['boys']}\n👩 Девушек: {stats['girls']}\n🟢 Не забанены: {stats['active']}\n🧾 Заполненные анкеты: {stats['profiles']}\n💎 Активный Премиум: {stats['premium']}\n⛔ Забанены: {stats['banned']}\n🚨 Жалобы за 7 дней: {reports}\n📢 Обязательных каналов: {len(ch)}\n⭐ Цена Премиум: {setting('premium_stars',100)} звёзд / {setting('premium_days',30)} дней",
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
        if data in ("admin:admin_add","admin:admin_del") and not is_admin(uid):
            await q.message.reply_text("Недостаточно прав."); return
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
            with _SETTING_CACHE_LOCK:
                _SETTING_CACHE.pop("premium_stars", None)
                _SETTING_CACHE.pop("premium_days", None)
            await msg.reply_text(f"✅ Цена Премиум сохранена: {stars} ⭐ за {days} дней.")
        except (ValueError,IndexError): await msg.reply_text("Формат: 100 30")
        return True
    if action in ("admin_add","admin_del"):
        if not is_admin(uid): await msg.reply_text("Недостаточно прав."); return True
        try: target=int(text)
        except ValueError: await msg.reply_text("Telegram ID должен быть числом."); return True
        if target==7659107145: await msg.reply_text("Главного администратора нельзя удалить."); return True
        if action=="admin_add":
            if not get_user(target): await msg.reply_text("Пользователь сначала должен нажать /start в боте."); return True
            execute("INSERT INTO admins(telegram_id,added_by) VALUES(%s,%s) ON CONFLICT DO NOTHING",(target,uid))
            await msg.reply_text(f"✅ {target} добавлен в админы.")
            try: await context.bot.send_message(target,"Вам выданы права администратора. Откройте /start, чтобы увидеть админ-панель.")
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
        except (ValueError,IndexError): await msg.reply_text("Format: 123456789 30" if get_user(uid).get("language")=="en" else "Формат: 123456789 30")
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

def serve():
    # Render expects the web process to listen on its assigned PORT.
    web.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log update errors and safely ignore expected stale callback/network errors."""
    error = context.error
    if isinstance(error, BadRequest):
        msg = str(error).lower()
        if any(term in msg for term in ("query is too old", "query id is invalid", "query to be answered")):
            log.info("Expired callback ignored by global error handler: %s", error)
            return
    if isinstance(error, (TimedOut, NetworkError, RetryAfter)):
        log.warning("Temporary Telegram API/network issue (polling will continue/retry): %s", error)
        return
    log.error("Unhandled error while processing Telegram update", exc_info=(type(error), error, error.__traceback__))

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
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start",start))
    application.add_handler(CommandHandler("admin",admin))
    application.add_handler(CommandHandler("cancel",cancel))
    application.add_handler(CallbackQueryHandler(cb))
    application.add_handler(PreCheckoutQueryHandler(precheckout))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT,paid))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,route_text))
    application.add_handler(MessageHandler((filters.PHOTO|filters.Sticker.ALL|filters.VOICE|filters.VIDEO|filters.Document.ALL|filters.ANIMATION) & ~filters.COMMAND,route_text))
    log.info("Анонимный чат знакомства starting")
    # Retry startup if Telegram is temporarily unreachable. This does not prevent
    # Render Free from sleeping; /health can be pinged by an external monitor.
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        bootstrap_retries=-1,
        poll_interval=1.0,
        timeout=30,
        drop_pending_updates=False,
    )

if __name__=="__main__": main()
