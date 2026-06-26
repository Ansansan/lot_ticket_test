Always start every response with my name, "Ans"

# lot_ticket_test — Telegram Yappy Receipt-Verification Bot (TEST_1)

Verifies **Yappy** payment receipts. Receipt screenshots are forwarded (by another
bot) into a Telegram **channel**; this bot OCRs each one, matches it against ingested
Yappy payments, and reports a status. It also hosts an admin **premios**
(lottery-results entry) flow and a Telegram Web App. `BOT_VERSION` is `TEST_1_Vnn`
(currently `TEST_1_V10`).

> ⚠️ **This is the RECEIPT bot.** The sibling folders `lot ticket bot`
> (`Ansansan/LotTicket`) and `lot ticket dadan` are **different LOTTERY projects**
> (`lot_ticket.py` / `lot_ticket_dadan.py`, AWARDS table, Nacional tiers — no receipt
> OCR). Do **not** port receipt changes there, and do not pull their code in here.
> There is no separate local "prod" copy of this bot — prod is the PythonAnywhere
> deployment below.

## Architecture

```
Receipts forwarded into RECEIPT_FORWARD_CHANNEL_ID (by another bot)
   │
   ▼
lot_ticket_test.py  (bot worker — PythonAnywhere always-on task, infinity_polling)
   │  - OCRs the receipt (run_receipt_ocr); parses amount / confirmation / time
   │  - matches against Yappy payments (yappy_cache.db); GREEN = matched + verified
   │  - GREEN stays in the channel (original kept); every NON-green outcome relocates
   │    (image + status caption) to RECEIPT_ISSUES_GROUP_ID / topic
   │    RECEIPT_ISSUES_TOPIC_ID and the channel original is DELETED
   │    (relocate_receipt_issue_to_topic)
   │  - pending (🔵) auto-edits to green when the payment arrives
   │    (check_and_notify_pending); admins fix a misread code by replying 5 letters
   │  - Yappy ingestion (ingest_yappy_channel_posts) when YAPPY_DIRECT_INGEST_ENABLED
   │  - premios: admin lottery-results entry via the Web App (save_results), gated by
   │    the ADMIN_USER_IDS allowlist (can_manage_premios)
   │  - support-thread mirroring (reply_to_user_topic / mirror_to_topic)
   ▼
SQLite:  tickets_test.db (tickets / draw results)
         yappy_cache.db  (Yappy payments + pending_verifications)
   │
Web App  (index.html + script_v10.js + style_v10.css — GitHub Pages, THIS repo:
          https://ansansan.github.io/lot_ticket_test/  = WEBAPP_BASE_URL)
History API  (HISTORY_API_BASE = tel.pythonanywhere.com/lot_ticket_dadan/ — a
          SEPARATE PythonAnywhere app, not this bot)
```

## File map

| File | Purpose | Tracked? |
|---|---|---|
| `lot_ticket_test.py` | The entire bot: handlers, OCR pipeline, matching, SQLite, premios, web-app data (one ~8.3k-line file) | Yes |
| `index.html` | Web-app shell (Telegram WebApp) — live entry point | Yes |
| `script_v10.js` | Web-app logic | Yes |
| `style_v10.css` | Web-app styles | Yes |
| `HANDOFF-receipt-issues-topic.md` | Handoff + cold-review record for the receipt-issues-topic feature | Yes |
| `.env` | Real secrets + config (`BOT_TOKEN`, salt, all IDs) | **No** (gitignored; read via `os.getenv` only) |
| `tickets_test.db` | SQLite: tickets / draw results | No (runtime, lives on PythonAnywhere) |
| `yappy_cache.db` | SQLite: Yappy payments + `pending_verifications` | No (runtime, lives on PythonAnywhere) |
| `flag_*.png` | Lottery flag images for ticket rendering | No (live on PythonAnywhere) |

## Deployment

- **Bot:** PythonAnywhere account `tel`, dir `/home/tel/lot_ticket_test/`, run as an
  **always-on task**:
  `cd /home/tel/lot_ticket_test && /home/tel/task_env/bin/python3 -u lot_ticket_test.py`
  (virtualenv `/home/tel/task_env`). Long polling, **not** webhooks. **That dir is NOT a
  git checkout.**
- **Update the bot:** land code on GitHub `main` → **upload `lot_ticket_test.py`** into that
  dir (Files → "Upload a file" overwrites it; or pull in a Bash console) — **never overwrite
  `.env`, `tickets_test.db`, or `yappy_cache.db`** (they are prod state) → set/confirm the
  `.env` keys → **restart the always-on task** (`.env` is read only at startup). The receipt
  feature needs **no DB migration and no new pip deps**.
- **Frontend:** push to `main`; GitHub Pages auto-publishes to
  `https://ansansan.github.io/lot_ticket_test/`. No PythonAnywhere step.
- **Test/prod share the same Telegram IDs** (forward channel `-1003765331250`, admin/issues
  group `-1003595738966`, issues topic `47362`), so `.env` ID values carry over.
- **Syntax check:** `python -m py_compile lot_ticket_test.py`. (No test suite / CI in this repo.)

## Don't break this

- **Deploy gate — channel "Delete messages".** The bot must be an admin of
  `RECEIPT_FORWARD_CHANNEL_ID` with the **"Delete messages"** right, or the non-green
  relocation copies-but-cannot-delete, leaving duplicates in the channel.
- **`RECEIPT_ISSUES_TOPIC_ID` must be set** (no code default → `None`). Without it, relocated
  messages land in the group's "General" thread and the manual 5-letter correction goes inert.
  `RECEIPT_ISSUES_GROUP_ID` defaults to `ADMIN_GROUP_ID`.
- **Green stays / non-green relocates.** The matched+verified (✅) path replies in the channel
  and keeps the original; **every other** outcome must go through
  `relocate_receipt_issue_to_topic`. A new early-return in `process_channel_ocr_task` must
  route through relocate, never a bare `bot.reply_to` in the channel.
- **Pending row points at the topic message.** `pending_verifications` stores the relocated
  blue message's `chat_id` / `reply_message_id` so `check_and_notify_pending` edits the right
  message (and blue→green can't target a deleted channel original).
- **`WEBAPP_BASE_URL` default is stale.** The code default `…/LotTicket/test/` **404s**; the
  live URL works only via the `.env` override (`…/lot_ticket_test/`). Keep it set in every env.
- **Premios authz = `ADMIN_USER_IDS` allowlist.** `/premios`, the admin-menu premios option,
  and `save_results` are gated by `can_manage_premios` (the legacy `ADMIN_USER_ID` super-admin
  is always folded in). Populate `ADMIN_USER_IDS` for every staffer who needs access.
- **Secrets never in tracked source.** `BOT_TOKEN`, the salt, and all IDs come from the
  gitignored `.env` via `os.getenv` only — never write a literal secret into `lot_ticket_test.py`.
