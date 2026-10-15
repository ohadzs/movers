# movers

Big-drop alerts for the **SPY + QQQ** universe, pushed to Telegram.

Rule: a name whose **previous close was over $100** and that is **down 5% or more**
against that close. Two ways of saying it:

- **live** — polled every minute through the New York session. A name has to
  hold below −5% for `confirm_polls` consecutive polls (default 2 minutes) before it fires, and
  it fires at most once per name per day. The confirmation matters: over the last year, **52% of
  names that touched −5% intraday closed better than −5%**, so unfiltered live
  alerts are wrong about half the time.
- **close** — one batched digest just after the close, listing everything that
  actually finished the day down 5%+. Quiet days send nothing.

Stdlib Python only, no dependencies.

## Scale

Measured over the 250 trading days to 2026-08-03:

| | |
|---|---|
| universe (SPY ∪ QQQ, deduped) | 518 tickers |
| of those, over $100 | 337 |
| close-based alerts | 1,779/yr — **7.1/day** avg, median 4, worst day 59 |
| intraday touches | 3,678/yr — 14.7/day, 52% recover before the close |
| names that fired at least once | 288 of 337 |

Raising the threshold to 7% cuts the volume to roughly a third.

## Usage

```
python3 movers.py refresh          # rebuild universe.json from Wikipedia
python3 movers.py live             # one polling pass (cron)
python3 movers.py close            # end-of-day digest (cron)
python3 movers.py once live        # dry run, prints, pushes nothing
python3 movers.py once close
python3 movers.py test [chat_id]   # hello message, verifies bot + chat
```

`once` bypasses the market-hours gate, so it works for testing at any time.

## Config — `config.json`

Copy `config.example.json` to `config.json` (gitignored) and fill in the chat ids.

| key | meaning |
|---|---|
| `min_price` | floor on the **previous close**, in dollars |
| `drop_pct` | how far down counts, in percent |
| `confirm_polls` | consecutive live polls (= minutes) below the line before firing |
| `max_lines` | tickers per message; the rest collapse to "+N more" |
| `chat_id` | Telegram target (the group) |
| `owner_chat` | optional — your own chat, for feed-health warnings and the daily self-audit |
| `enabled` | `false` mutes pushes, everything else still runs |

Bot token: `TELEGRAM_BOT_TOKEN`, else `secret_config.py`, else
`~/.telegram_bot_token`.

## Data

Yahoo's `v7/finance/spark` endpoint — batched, unauthenticated, capped near 20
symbols per call. A full 337-name sweep is 17 calls in about 1.1 seconds.
`chartPreviousClose` from it matches the official daily bars exactly.

## Market hours

The New York session in Israel time is 16:30–23:00 for most of the year, and
15:30–22:00 during the two short stretches when the Israeli and American DST
switches disagree (26 Oct – 1 Nov 2026, 15–25 Mar 2027). Cron covers the wide
window; the script gates on real `America/New_York` time, so those weeks need no
intervention. Holidays are caught by a staleness check on the quote timestamps
rather than a hardcoded calendar.

## Files

`universe.json` (tickers, rebuilt weekly) · `state.json` (per-day dedup and
streak counters, gitignored) · `secret_config.py` (token, gitignored).

## Cron

Box clock on Israel time (`Asia/Jerusalem`); adjust the hours if yours differs.

```
*/1 15-23 * * 1-5  cd ~/stock-eyal && python3 movers.py live    >> movers.log 2>&1
5 22,23 * * 1-5    cd ~/stock-eyal && python3 movers.py close   >> movers.log 2>&1
20 23 * * 1-5      cd ~/stock-eyal && python3 movers.py verify  >> movers.log 2>&1
0 7 * * 0          cd ~/stock-eyal && python3 movers.py refresh >> movers.log 2>&1
```
