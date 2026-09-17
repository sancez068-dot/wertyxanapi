# Wertyxan API — FastAPI

This is the separate backend for the Wertyxan static frontend.

## Render

Runtime: Python 3.12

API is FastAPI only; there is no Node/Express backend in this archive.

Build:
`pip install -r requirements.txt`

Start:
`uvicorn app:app --host 0.0.0.0 --port $PORT`

## Required environment

- `DATABASE_URL` or `SUPABASE_DATABASE_URL`
- `BOT_TOKEN`
- `BOOTSTRAP_OWNER`
- `BOOTSTRAP_ADMIN`
- `CORS_ORIGINS` — static-site origin

For Telegram webhook set:
`TELEGRAM_WEBHOOK_URL=https://YOUR-API-DOMAIN.onrender.com/api/webhook/telegram`

Optional blockchain variables are documented in `.env.example`.

## Frontend connection

The static site has one configuration file:
`js/config.js`

Set:

```js
window.WERTYXAN_CONFIG = {
    apiUrl: "https://YOUR-API-DOMAIN.onrender.com/api"
};
```

The frontend does not need the API to be on the same domain.

## Docs

After deployment:
- `/docs`
- `/redoc`
- `/openapi.json`

## Telegram messages

User notifications and broadcasts are sent by the backend with Telegram Bot API `sendMessage` as ordinary bot messages. No frontend bot token is exposed.


## Important

The frontend is deployed separately. Configure its API URL in `js/config.js`.
For production, set `CORS_ORIGINS` to the exact static-site origin(s), comma-separated.
`X-Idempotency-Key` is required for withdrawals and the frontend generates it automatically.
