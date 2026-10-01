# SecretMeet v6 — Anonymous Dating Bot (18+)

## Main menu
The bot displays a persistent keyboard below the Telegram message field:
- 🔎 Найти парня — looks for male profiles
- 🔎 Найти девушку — looks for female profiles
- 🎲 Случайный чат
- ❤️ Анкеты
- 👤 Моя анкета
- 💎 Premium
- 🌐 Язык / Забон
- 🆘 Помощь

This is a Telegram ReplyKeyboard, not a message containing inline buttons.

## Included
- Python Telegram bot, PostgreSQL persistence, Render health endpoint.
- Age self-attestation and 18–99 profile age validation.
- Tajik/Russian language choice.
- Gender + looking-for preferences, city and bio.
- Anonymous relay for text, photos, stickers, voice, video, documents and animations.
- Random matching, profile browsing, stop/next/report/block.
- Telegram Stars Premium purchase.
- Admin panel: stats, reports, broadcast, grant Premium, ban/unban, add/list/remove required subscription channels.
- Required-channel subscription gate. Admin must add bot to the channel as an administrator so it can check membership. For private channels, configure an invite link separately if needed; this build generates public @channel links only.

## Setup
1. Create bot with @BotFather. Keep token private.
2. Create PostgreSQL database in Neon.
3. Push these files to a private GitHub repository.
4. Create Render Web Service. Build: `pip install -r requirements.txt`; Start: `python bot.py`.
5. Set environment variables in Render:
   - `BOT_TOKEN` — BotFather token
   - `DATABASE_URL` — Neon/PostgreSQL connection URL with SSL
   - `ADMIN_IDS` — your numeric Telegram ID (comma-separated for multiple admins)
   - `REQUIRED_CHANNEL` — optional default channel, e.g. `@yourchannel`
   - `PREMIUM_STARS=100`
   - `PREMIUM_DAYS=30`
6. Deploy, open `/health`, then send `/start`.

## Admin
Send `/admin` from an ID listed in `ADMIN_IDS`.
- Add channel: enter `@channel | Channel title`. Bot must be channel admin.
- Remove channel: send exact stored channel identifier.
- List channels: displays current required channels.
- Broadcast: plain text to non-banned users.
- Grant Premium: `TELEGRAM_ID 30`.
- Ban/unban: numeric Telegram ID.
- `/cancel` cancels current admin input.

## Important operational notes
- Channel membership checking works only if the bot can access the channel and is an administrator there. Test with a secondary account before launch.
- Age is self-declared; this is not identity or legal age verification.
- Premium benefits should be accurately advertised. Current Premium changes matching priority and grants paid status; do not promise unimplemented features.
- Telegram Stars payments must be tested before accepting real customers.
- Render free web services may sleep and do not guarantee 24/7 availability.
- Anonymous to other users does not mean anonymous to the service operator; Telegram IDs are stored for message delivery and moderation.
- The app has not yet undergone production security, load, privacy/legal, or payment certification testing.


## 🛍 Дӯкон ва харидҳо
- Premium барои `PREMIUM_DAYS` рӯз, нарх `PREMIUM_STARS` ⭐.
- Boost: анкета барои 1 соат дар боло — `BOOST_STARS` ⭐.
- Super Like: 1 дона — `SUPERLIKE_STARS` ⭐.
- Кушодани рӯйхати лайкҳо — `REVEAL_LIKES_STARS` ⭐.
- Нархҳо дар Render → Environment тағйир дода мешаванд.
- Барои хизматрасониҳои рақамӣ дар дохили Telegram, пардохт бояд бо Telegram Stars (XTR) бошад: https://core.telegram.org/bots/payments-stars

Эзоҳ: санҷиши синну сол дар ин версия худэъломкунӣ аст, на тасдиқи шахсияти расмӣ. Пеш аз кушодани оммавӣ пардохтҳои Stars, PostgreSQL ва сценарияҳои чатро бо боти тестӣ санҷед.


## Администратор и поддержка
- Основной Telegram ID администратора: `7659107145` (уже закреплён в коде).
- Админ может через `/admin` выдать/снять права администратора другому пользователю по Telegram ID. Пользователь должен сначала нажать `/start`.
- Кнопка «🆘 Поддержка» открывает Telegram: `@ffxdavlatov`.
- В магазине доступна отдельная информация о преимуществах Premium.
- Основной админ защищён от снятия прав через панель; дополнительных админов можно снимать.
- Для настройки контакта поддержки используйте `SUPPORT_USERNAME=ffxdavlatov` в Render Environment.


## Быстрая работа с Neon PostgreSQL
- В `DATABASE_URL` вставьте **pooled connection string** из Neon (hostname обычно содержит `-pooler`).
- Для небольшого Render-инстанса оставьте `DB_POOL_MIN=1` и `DB_POOL_MAX=8`; не ставьте слишком большое значение на бесплатном плане.
- В коде используется пул соединений PostgreSQL, таймаут подключения и индексы для частых запросов. Это сокращает задержки на повторных подключениях, но реальная скорость зависит также от региона Neon/Render и нагрузки.
- Язык по умолчанию для новых пользователей — русский; пользователь может выбрать таджикский в меню.
- Перед запуском проверьте покупку Telegram Stars в тестовом сценарии и логи Render: этот архив не был проверен с вашим реальным BOT_TOKEN и базой Neon.


## Быстрая работа с Neon PostgreSQL
- В `DATABASE_URL` вставьте **pooled connection string** из Neon (hostname обычно содержит `-pooler`).
- Для небольшого Render-инстанса оставьте `DB_POOL_MIN=1` и `DB_POOL_MAX=8`; не ставьте слишком большое значение на бесплатном плане.
- В коде используется пул соединений PostgreSQL, таймаут подключения и индексы для частых запросов. Это сокращает задержки на повторных подключениях, но реальная скорость зависит также от региона Neon/Render и нагрузки.
- Язык по умолчанию для новых пользователей — русский; пользователь может выбрать таджикский в меню.
- Перед запуском проверьте покупку Telegram Stars в тестовом сценарии и логи Render: этот архив не был проверен с вашим реальным BOT_TOKEN и базой Neon.


## SecretMeet v6 Admin Panel
- Premium default: 100 Telegram Stars / 30 days; can be changed from /admin → Prices and saved in PostgreSQL.
- Broadcast: send text, photo, video, document or another supported message after choosing broadcast; the bot copies it to non-banned users and reports success/failure counts.
- Required channels: add/remove/list; bot must be administrator in each channel to verify membership.
- Users: ban/unban by Telegram ID, grant/revoke Premium.
- Admins: only main admin ID 7659107145 can add/remove extra admins. Extra admin must have started the bot first.
- Neon: use pooled connection string (hostname includes -pooler).
- Keep BOT_TOKEN and DATABASE_URL in Render Environment; never commit real secrets. Test all payment and moderation flows before public launch.
