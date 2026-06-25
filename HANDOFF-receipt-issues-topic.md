# Cold-review handoff — relocate non-green channel receipts to an admin topic

> For a reviewer with **no prior context**. Everything needed to judge correctness
> independently is below. The change is **uncommitted** in the working tree.

## 0. Where you are (read first)
- This repo is **`lot ticket bot test`** (`lot_ticket_test.py`) — the **Yappy
  receipt-verification** bot. It is a *different* project from the sibling
  `lot ticket bot` (the lottery bot, which has no receipt OCR). Do not look there.
- Source file changed: **`lot_ticket_test.py`**. Runtime config changed: **`.env`**
  (gitignored — not in `git diff`).
- ⚠️ **The working-tree diff is NOT only the receipt change.** When this work began,
  `lot_ticket_test.py` already had **pre-existing uncommitted changes** unrelated to
  receipts — a **premios authorization rewrite** (`ADMIN_USER_IDS`,
  `can_manage_premios`, gates on `/premios` / `admin_menu` / `save_results`) and
  **premios report chunking** (`_send_premios_report_chunk`). Those are **not part of
  this feature** and should be split into their own commit/PR. See §10 finding 1.
- See the diff: `git diff -- lot_ticket_test.py`. The **receipt-relocation** change is
  the subset described in §4; everything premios-related is the pre-existing bundle.
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

## 7. Deploy prerequisites (operational, not code)
1. Bot must be **admin of the channel `-1003765331250` with "Delete messages"** —
   else the topic copy still happens but the original cannot be removed (logs a
   warning, leaves it in the channel).
2. Bot must be able to **post in topic `47362`** of group `-1003595738966`.
3. Set the two `.env` vars; restart the bot.

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
- **1. ⬜ Scope creep — premios change bundled.** The diff also contains a pre-existing
  premios auth rewrite (`ADMIN_USER_IDS` / `can_manage_premios` / `save_results` gate)
  and report chunking (`_send_premios_report_chunk`), unrelated to receipts and **not
  authored by this feature**. → Split into its own commit/PR. *(Owner decision.)*
- **2. ⬜ Latent admin lockout in that bundled change.** `ADMIN_USER_IDS` defaults to an
  **empty set** with no fallback to legacy `ADMIN_USER_ID`; combined with the new
  `save_results`/`/premios`/`admin_menu` gates, an empty/unset value locks out everyone
  incl. the super-admin. **Not live today** (current `.env` populates it), and **not part
  of the receipt feature** — fix it within the premios PR: `ADMIN_USER_IDS = {…} |
  ({ADMIN_USER_ID} if ADMIN_USER_ID else set())`, and add an `.env.example` entry.
- **3. ✅ Two non-green error edges no longer reply in-channel.** `handle_channel_photo`
  download-failure and `process_channel_ocr_task` `mark_payment_verified`-failure now go
  through `relocate_receipt_issue_to_topic` like every other non-green outcome.

### Latent (config-dependent; not firing on current `.env`)
- **4. ✅ Incoherent `RECEIPT_ISSUES_TOPIC_ID = None` escape removed.** The
  `reply_to_user_topic` guard now requires `RECEIPT_ISSUES_TOPIC_ID is not None` and an
  exact thread match (the old `None or …` would have matched any topic).
- **5. ⬜ Correction dies silently if `RECEIPT_ISSUES_GROUP_ID != ADMIN_GROUP_ID`.** Only
  fires inside `is_admin_chat` (== `ADMIN_GROUP_ID`). Documented in the code comment at
  the guard; **not** guarded by a startup warning. Acceptable while the two IDs are equal
  (current config). → Optional: add a startup warning or a dedicated handler.

### Minor / advisory
- **6. ✅ `edit_status_text_or_caption` no longer over-broad.** Falls back to text only on
  a genuine "no caption" error, treats "not modified" as a no-op, and re-raises other
  errors instead of masking them behind a guaranteed second failure.
- **7. ⬜ Topic send doesn't honor `429 retry-after`.** Diverges from the file's
  `copy_message_with_retry` backoff. Degrades safely (receipt survives in channel, not
  deleted). → Optional: reuse the retry helper for the topic send.
- **8. ❌ FALSE POSITIVE — "incomplete relocates without parse_mode".** The incomplete
  branch *does* pass `parse_mode="Markdown"`; verified in code. No action.
- **9. ⬜ `int()` on a garbage `RECEIPT_ISSUES_*` env value crashes at import.** Consistent
  with the file's existing env-parsing pattern; low risk. Not changed.
- **10. ⬜ Operational:** bot must be a **channel admin with "Delete messages"** or
  relocation leaves the original behind (topic copy + undeleted channel post). Confirm the
  permission is actually granted (see §7).

### Independently confirmed sound (traced from the diff, not the narrative)
Fail-safe delete-after-send ordering; pending-row points at the topic message so auto
blue→green can't target a deleted original; echo loop cannot occur; green path & DM flow
untouched; `apply_confirmation_correction` refactor is behavior-preserving; `file_id`
cross-chat reuse is valid; money-request branch has no double-send.
