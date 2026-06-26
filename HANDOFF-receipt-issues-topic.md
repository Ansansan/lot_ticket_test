# Cold-review handoff — relocate non-green channel receipts to an admin topic

> For a reviewer with **no prior context**. Everything needed to judge correctness
> independently is below. The work is now **two separate commits** on branch
> `2026-06-25-receipt-issues-topic` (off `main`, not pushed) — review each commit on
> its own (see §0). Cold-review findings from the first pass are in §10.

## 0. Where you are (read first)
- This repo is **`lot ticket bot test`** (`lot_ticket_test.py`) — the **Yappy
  receipt-verification** bot. It is a *different* project from the sibling
  `lot ticket bot` (the lottery bot, which has no receipt OCR). Do not look there.
- Source file changed: **`lot_ticket_test.py`**. Runtime config changed: **`.env`**
  (gitignored — not in `git diff`).
- ⚠️ **Two separate concerns, now in two commits — review them independently:**
  - `a40ee22` **Relocate non-green channel receipts** — THE feature this handoff
    describes (§1–§9). Review against the requirement. `git show a40ee22`.
  - `baedd7e` **premios: allowlist authorization + chunked prize reports** — a
    *pre-existing* change (`ADMIN_USER_IDS`, `can_manage_premios`, `/premios` /
    `admin_menu` / `save_results` gates, `_send_premios_report_chunk`) that was
    already uncommitted in the file when this work began. **Not part of the receipt
    feature** — out of scope for this handoff; judge it on its own. `git show baedd7e`.
  - The split is verified clean: `a40ee22` contains zero premios tokens, `baedd7e`
    zero receipt tokens, and the two together are byte-identical to the working file.
- Review fixes from the first cold-review pass are folded into these commits, not a
  third diff — see §10 for per-finding status (✅ fixed / ⬜ open / ❌ false positive).
- Python syntax verified: `python -m py_compile lot_ticket_test.py` → OK.

## 1. Requirement (what was asked)
Receipt images are forwarded into a Telegram **channel**
(`RECEIPT_FORWARD_CHANNEL_ID = -1003765331250`, link `t.me/c/3765331250/...`). The
bot OCRs each one and replies in that channel with a status. Today **all** outcomes
reply in that channel.

Goal: **everything that is NOT "green"** (green = info extracted, matching payment
found, and verified) must instead go to a **topic in the admin group**
(`t.me/c/3595738966/47362/...` → group `-1003595738966`, forum topic thread `47362`),
as the **receipt image + the status**. Green stays in the channel.

## 2. Decisions taken (confirmed with requester)
1. **Move, not mirror** — non-green results no longer appear in the channel; they go
   only to the topic.
2. **Image + status line** — the topic message is the receipt photo with the status
   text as its caption.
3. **Delete the original** — after copying to the topic, delete the original
   forwarded post from the channel.
4. **Port the correction** — admins can still fix a misread confirmation code by
   replying with 5 letters, now **in the topic**.

## 3. New config
`.env` (and defaults in code at `lot_ticket_test.py:158-159`):
```
RECEIPT_ISSUES_GROUP_ID=-1003595738966   # defaults to ADMIN_GROUP_ID if unset
RECEIPT_ISSUES_TOPIC_ID=47362            # forum topic thread id; None -> group "General"
```
Note: `RECEIPT_ISSUES_GROUP_ID` **equals** `ADMIN_GROUP_ID` here — that equality is
load-bearing (see §6, assumption A).

## 4. Change inventory (file:line — all in `lot_ticket_test.py`)
| # | Site | What changed |
|---|---|---|
| 1 | `158-159` | New `RECEIPT_ISSUES_GROUP_ID` / `RECEIPT_ISSUES_TOPIC_ID` config. |
| 2 | `edit_status_text_or_caption` `:2797` | New helper: edits a message's **caption** first (relocated blue is a photo), falls back to `edit_message_text`. |
| 3 | `relocate_receipt_issue_to_topic` `:5307` | New helper: send photo+caption (or text) to issues group/topic, then `delete_message` the channel original. **Fail-safe:** if issues group unset or send throws → falls back to a channel `reply_to` and does **not** delete. Returns the sent msg (or None). |
| 4 | `process_channel_ocr_task` `:5360` | Each **non-green** branch now calls `relocate_...` instead of `bot.reply_to`: duplicate `:5396`, OCR-fail `:5406`, money-request `:5423/5425`, incomplete `:5434`, stale `:5448`, already-processed 🟡 `:5464`, pending 🔵 `:5485`. The **green ✅** path is unchanged (`bot.reply_to(message, success_msg)` `:5557`). |
| 5 | pending 🔵 block `:5485-5527` | Stores **where the blue message now lives** in `pending_verifications`: `chat_id = blue_reply.chat.id` (issues group, or channel on fallback) and `reply_message_id = blue_reply.message_id` (topic msg). |
| 6 | `check_and_notify_pending` `:2891` | The auto blue→green edit now uses `edit_status_text_or_caption` (blue is a photo caption now), instead of `bot.edit_message_text`. |
| 7 | `apply_confirmation_correction` `:4865` | Extracted shared function from the old channel handler. Reads original status from `.caption or .text`; edits via `edit_status_text_or_caption`. Returns True if it handled a pending correction, else False. |
| 8 | `handle_channel_confirmation_correction` `:5005` | Now a thin wrapper that calls `apply_confirmation_correction` (legacy channel path). |
| 9 | `reply_to_user_topic` `:4725-4736` | Before the normal support-mirroring logic, if the reply is in the issues group+topic, call `apply_confirmation_correction`; `return` if handled. |

## 5. Expected behavior matrix (channel receipt → outcome)
| OCR outcome | Status | Destination after change |
|---|---|---|
| Matched + verified | ✅ green | **Channel** (unchanged); original image stays |
| Not found (pending) | 🔵 | **Topic** (photo+caption); original deleted; later auto-edits → ✅ in topic |
| Already processed | 🟡 | **Topic**; original deleted |
| Duplicate image | notice | **Topic**; original deleted |
| OCR unreadable | ❌ | **Topic**; original deleted |
| Incomplete fields | ⚠️ | **Topic**; original deleted |
| Stale (not today) | — | **Topic**; original deleted |
| Money request | warning | **Topic** + existing alert to `MONEY_REQUEST_WARNING_GROUP_ID` (unchanged); original deleted |

## 6. Invariants / assumptions to verify
- **A. Correction routing depends on `RECEIPT_ISSUES_GROUP_ID == ADMIN_GROUP_ID`.**
  `reply_to_user_topic` only fires for the admin group (`is_admin_chat`). If someone
  repoints the issues topic to a *different* group, topic-side code correction will
  not fire and needs a dedicated handler. (Flagged in a code comment at `:4725`.)
- **B. Blue is a photo now** → every edit-to-green path must use caption-aware edit.
  Confirm both edit sites: `check_and_notify_pending:2891` and inside
  `apply_confirmation_correction:4865`. Plain `edit_message_text` on a photo would
  raise.
- **C. Pending row consistency.** `pending_verifications.chat_id` /
  `reply_message_id` must point at the topic message so the later auto-match
  (`check_and_notify_pending`) edits the right message. Verify the 🔵 block stores
  `blue_reply.chat.id` / `blue_reply.message_id`, not the channel's.
- **D. Green untouched.** Confirm the ✅ match path still replies in the channel and
  does **not** delete the original.
- **E. No echo loop.** The bot's own photo posted to the issues topic must not be
  re-OCR'd. `handle_photo_verification:` ignores `ADMIN_GROUP_ID`/Yappy group, and
  Telegram doesn't deliver the bot's own messages back. Confirm no new handler OCRs
  topic photos.
- **F. Fail-safe never drops a result.** If topic send fails, `relocate_...` must
  fall back to a channel reply and skip deletion. Verify the try/except ordering
  (delete only after a successful send).

## 7. Deploy prerequisites (operational, not code) — HARD GATES
1. **DEPLOY GATE:** the bot must be an **admin of the channel `-1003765331250` with the
   "Delete messages" right** — otherwise the topic copy still happens but the original
   stays in the channel (the runtime logs a warning naming this exact permission). Without
   it the "move" degrades to a "copy", leaving duplicates. Confirm it is granted.
2. Bot must be able to **post in topic `47362`** of group `-1003595738966`.
3. Set the two `.env` vars (`RECEIPT_ISSUES_GROUP_ID`, `RECEIPT_ISSUES_TOPIC_ID`); a
   blank/garbage value no longer crashes startup (falls back to the default + logs).
   Restart the bot.

### 7.1 Where it runs / how to deploy
See **`CLAUDE.md` → "Deployment"** for the full topology (PythonAnywhere always-on task at
`/home/tel/lot_ticket_test/`, upload-`lot_ticket_test.py`-then-restart, frontend on GitHub
Pages, `HISTORY_API_BASE` as a separate app, and the stale `WEBAPP_BASE_URL` default gotcha).

## 8. How to review
```
cd "lot ticket bot test"
git diff -- lot_ticket_test.py          # the whole change
python -m py_compile lot_ticket_test.py # syntax
```
Manual scenarios (against a test channel/group):
1. Forward a receipt with no matching payment → blue photo appears in topic 47362,
   original gone from channel. Then post the matching Yappy payment → the topic
   message edits to green ✅.
2. Reply to that blue topic message with a wrong→right 5-letter code → it re-matches
   / edits, and your reply is deleted.
3. Forward the same image twice → second one shows the duplicate notice in the topic.
4. Forward a receipt that matches immediately → green ✅ stays in the channel.

## 9. Explicitly NOT changed
- The DM flow (`process_ocr_task`) for users messaging the bot directly — out of
  scope; only the **channel** flow was requested.
- Payout/lottery logic, Yappy ingestion, support-thread mirroring.

## 10. Review findings (cold-review panel, 5 independent agents)
Status legend: ✅ fixed in working tree · ⬜ open / decision needed · ❌ false positive.

### Must address before merge
- **1. ✅ Scope split done.** The premios change is now its own commit (`baedd7e`),
  separate from the receipt feature (`a40ee22` + hardening). Verified clean: each commit
  contains zero of the other's tokens.
- **2. ✅ Admin-lockout fallback applied** (in `baedd7e`). `ADMIN_USER_IDS` now always
  folds in the legacy super-admin (`ADMIN_USER_ID`), so an empty/unset value cannot lock
  everyone out of the results workflow. (No `.env.example` exists in this repo, so the var
  is documented in the commit message instead.)
- **3. ✅ Two non-green error edges no longer reply in-channel.** `handle_channel_photo`
  download-failure and `process_channel_ocr_task` `mark_payment_verified`-failure now go
  through `relocate_receipt_issue_to_topic` like every other non-green outcome.

### Latent (config-dependent; not firing on current `.env`)
- **4. ✅ Incoherent `RECEIPT_ISSUES_TOPIC_ID = None` escape removed.** The
  `reply_to_user_topic` guard now requires `RECEIPT_ISSUES_TOPIC_ID is not None` and an
  exact thread match (the old `None or …` would have matched any topic).
- **5. ✅ Decoupled-group correction handled.** Added `handle_issues_topic_correction`,
  a dedicated handler that fires when `RECEIPT_ISSUES_GROUP_ID != ADMIN_GROUP_ID`. Its
  filter is mutually exclusive with the inline equal-group path, so corrections are never
  processed twice and work regardless of which group hosts the issues topic.

### Minor / advisory
- **6. ✅ `edit_status_text_or_caption` no longer over-broad.** Falls back to text only on
  a genuine "no caption" error, treats "not modified" as a no-op, and re-raises other
  errors instead of masking them behind a guaranteed second failure.
- **7. ✅ Topic send now retries on `429 retry-after`.** `relocate_receipt_issue_to_topic`
  wraps its send in `_telegram_send_with_retry`, mirroring `copy_message_with_retry`'s
  flood-wait + exponential backoff.
- **8. ❌ FALSE POSITIVE — "incomplete relocates without parse_mode".** The incomplete
  branch *does* pass `parse_mode="Markdown"`; verified in code. No action.
- **9. ✅ Env parse no longer crashes at import.** `RECEIPT_ISSUES_*` now parse via
  `_safe_int_env`, which falls back to the default (and logs) on a blank/non-numeric value.
- **10. ✅ Operational requirement made discoverable + documented as a deploy gate.** The
  delete-failure log now names the required "Delete messages" admin right, and §7 lists it
  as a hard prerequisite. (Still an operational gate — the bot must actually be granted it.)

### Independently confirmed sound (traced from the diff, not the narrative)
Fail-safe delete-after-send ordering; pending-row points at the topic message so auto
blue→green can't target a deleted original; echo loop cannot occur; green path & DM flow
untouched; `apply_confirmation_correction` refactor is behavior-preserving; `file_id`
cross-chat reuse is valid; money-request branch has no double-send.

## 11. Second cold-review pass (4 independent fresh-context agents, post-fix)
Run against the committed branch (`a40ee22` receipt + `97a3735` hardening + `baedd7e`
premios). Verdicts: cold auditor **ship-able / no blockers**; correctness **APPROVE**;
telebot/API **approve with changes**; premios security **APPROVE** (all premios write/
report paths gated, lockout fallback sound, chunking content-preserving and does not alter
payout math). New edge findings — now fixed in `a` (this pass):

- **A. ✅ Markdown caption could drop the relocation.** On a Markdown parse error the topic
  send failed and the channel fallback re-passed `parse_mode`, re-failing. Now: a parse
  error retries the topic send as plain text (stays in topic); the channel fallback is
  plain text. (`relocate_receipt_issue_to_topic`, reuses `_looks_like_markdown_send_error`.)
- **B. ✅ Retry helper no longer retries permanent 400s.** `_telegram_send_with_retry` now
  retries only floods / 5xx / network errors, failing fast on a plain 400 (less wasted
  backoff; avoids re-posting a once-succeeded send).
- **C. ✅ Green fallback stays in the topic.** The rare `check_and_notify_pending`
  edit-failure fallback now passes `message_thread_id` so the green confirmation doesn't
  land in the group's General thread.
- **D. ⬜ ACCEPTED — photo caption 1024-char ceiling.** Relocated statuses are captions
  (1024) vs the old text replies (4096). All current status strings are far under; on the
  unlikely overflow the send fails and self-heals to a channel text reply. Not guarding
  with truncation (over-engineering for the actual string lengths).
- **E. ⬜ ACCEPTED/LATENT — corrections inert if `RECEIPT_ISSUES_TOPIC_ID` is unset.** The
  bare code default is `None`; live `.env` sets `47362`. Blue→green auto-edit still works;
  only manual 5-letter correction is inert when no topic is configured.
- **F. ✅ Comment corrected** re: the two correction handlers being resolved by registration
  order in the super-admin-user edge (not purely disjoint filters).

### Third review pass (local max-effort, 8 multi-agent finders + manual source verification)
Run 2026-06-26 against head `fb38ecb` (the committed branch). **Verdict: no blockers in the
receipt feature** — independently confirms the §10/§11 passes. No CLAUDE.md governs the repo,
so no convention findings. Decision: **report only, no code changes applied** (per requester).
Status legend as before; **🟢 ACCEPT** = real but intentionally not fixed.

In-scope (receipt) findings:
- **R1. 🟢 ACCEPT (latent, would only bite a decoupled config).** `handle_photo_verification`
  (`:4889`) ignores only `ADMIN_GROUP_ID`/Yappy group. If `RECEIPT_ISSUES_GROUP_ID` were ever
  pointed at a *different* group, an admin posting a photo there would be OCR'd as a fresh
  submission. No-op in the live config (issues group == admin group, already ignored). Cheap
  1-line guard available if the issues topic is ever decoupled.
- **R2. 🟢 ACCEPT (rare fallback path).** Green-confirmation fallback (`:2935`) calls
  `mirror_to_topic(chat_id, …)` with `chat_id` = issues group; when it equals `ADMIN_GROUP_ID`
  (default) this self-mirrors a message already in the admin group. Only fires when the
  caption-edit fallback triggers. Same pattern existed pre-feature (channel id was passed there).
- **R3. 🟢 ACCEPT.** `_telegram_send_with_retry` (`:5377`) can re-post a non-idempotent
  `send_photo` on a 429/post-success network error → duplicate topic photo. Matches existing
  `copy_message_with_retry` behavior; rare.
- **R4. 🟢 ACCEPT (total-outage edge).** If topic send AND channel fallback both fail, `relocate`
  returns `None` → pending row gets `reply_message_id=None` → later green posts bare in channel.
  Original is not deleted (delete only after a confirmed send), so nothing is lost.
- **R5. ⬜ LATENT/ADVISORY.** `edit_status_text_or_caption` (`:2811`) and
  `_looks_like_markdown_send_error` switch on Telegram's English error prose. Correct against
  today's strings; would break silently if Telegram rewords. Deeper fix = record blue-message
  kind (photo/text) on the pending row instead of probing via a guaranteed-to-fail edit.
- **R6. ⬜ ADVISORY (altitude).** Three correction paths gated partly by handler registration
  order; documented in-code, the one overlap resolved by order. Works; brittle to reordering.
- **R7. Pre-existing, not introduced.** `apply_confirmation_correction` (`:4960`) commits the
  corrected code before verify; on `mark_payment_verified` failure it returns True leaving a
  corrected-but-unverified row (self-heals on later auto-match). Identical to the old handler.

Premios-side (commit `baedd7e` — separately owned, out of scope for this handoff):
- **P1.** Authorization tightened to an `ADMIN_USER_IDS` allowlist; non-allowlisted group
  members and anonymous (send-as-group) admins lose `/premios`/`admin_menu`/`save_results`.
  Intentional; super-admin fallback in place. **Owner action:** ensure `ADMIN_USER_IDS` lists
  every staffer who needs it.
- **P2.** `can_manage_premios(message.from_user.id)` derefs `from_user` at the call site (None
  for anonymous admins) before its own try/except. Pre-existing pattern (old `is_admin_chat`).
- **P3.** `ADMIN_USER_IDS` `.isdigit()` silently drops `+`/`-`/space entries (no warning).
- **P4.** Chunked `calculate_and_report` uses a fragile `current_chunk == header_section`
  identity check + parallel footer branches. No content loss found; could simplify.

False positives refuted this pass (do not re-chase):
- "Incomplete branch missing `parse_mode`" — false, passes `parse_mode="Markdown"` (`:5546`).
- "Money-request alert now fires for everyone" — false, both old branches called it identically;
  the hoist is behavior-preserving.
- "`reply_to_user_topic` leaks an admin reply to a user on a non-matched correction" — false,
  it `return`s when the thread is not a registered support thread (`:4811`).
- "Matched-but-unverified relocate loses the receipt/hash" — false, hash is released so a
  re-forward reprocesses (same as old code); the error is visible in the topic.
