#!/usr/bin/env python3
"""movers — big-drop alerts for the SPY + QQQ universe.

Modes:
  refresh          rebuild universe.json from Wikipedia (weekly)
  live             one intraday polling pass (cron every few minutes)
  close            end-of-day scan, one batched digest
  verify           after-close audit: what we sent vs what really happened
  once [live|close|verify] same scan, printed to stdout, nothing pushed
  test             push a hello message (verifies bot + chat_id)

Stdlib only. Config in config.json, universe in universe.json, per-day
dedup state in state.json.
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import datetime
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(HERE, "config.json")
UNIVERSE = os.path.join(HERE, "universe.json")
STATE = os.path.join(HERE, "state.json")

# Keep this User-Agent short. A full Chrome UA string gets a blanket 429 from
# Yahoo (the claimed browser doesn't match the TLS fingerprint); plain
# "Mozilla/5.0" is served normally from the same IP.
UA = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
NY = ZoneInfo("America/New_York")
IL = ZoneInfo("Asia/Jerusalem")

# Yahoo's spark endpoint rejects more than ~20 symbols in one call.
CHUNK = 20

# Consecutive blind polls (≈ minutes) before we tell the owner the feed is out.
BLIND_ALERT = 15

# Below this many genuinely-fresh quotes we assume the feed is broken, not thin.
MIN_FRESH = 50

# An alert more than this many minutes after the crossing counts as late.
LATE_MIN = 5

# The audit reads 5-minute bars, and Yahoo stamps a bar at its OPEN, so a
# crossing recorded at 16:30 really happened anywhere in 16:30-16:35. Measuring
# lateness from that stamp charges us for up to a whole bar we cannot see
# inside, which flagged two names at exactly +6 min on 2026-08-10 that were
# detected within a minute of the price printing. Allow the bar width before
# calling an alert late; a genuine delay (the 16:30 cross that went out hours
# later) still clears the widened bar easily.
BAR_MIN = 5

# CNBC serves this many symbols per request comfortably.
CNBC_CHUNK = 60

# Percentage points of disagreement between the two feeds we tolerate before
# refusing to send a name at all. Quotes tick, so a small gap is normal.
DISAGREE_PP = 2.0

# Only a quote this recent may veto the other feed. Older ones can still raise
# an alert, they just do not get to block one.
#
# This was 45, the same number as the live freshness window, which made the
# whole rule a no-op live: every quote that reached merge_hits was already
# under 45 minutes old, so every quote got a vote. Measured across the universe
# mid-session, Yahoo's own stamps run median 0.2 min, p99 5.7, max 8.6 (it
# stamps a 5-minute bar at the bar's OPEN, so a live bar reads up to 5 minutes
# old); CNBC's max 3.5. Ten minutes clears a healthy feed with room to spare and
# still refuses a vote to the 20-minute-stale Yahoo quote that silenced STX on
# 2026-08-07 while CNBC watched it fall through -9%.
CONFLICT_AGE_MIN = 10

# At the close there is only one print to agree on, but Yahoo's last 5-minute
# bar is stamped 15:55 and the digest runs at 16:05, so the live window would
# disqualify both feeds and switch the cross-check off exactly when it is
# cheapest to honour. Keep the old, looser number for that one scan.
CLOSE_CONFLICT_AGE_MIN = 45


# ---------------------------------------------------------------- helpers

def load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def save(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def cfg():
    c = load(CONFIG, {})
    c.setdefault("min_price", 100.0)
    c.setdefault("drop_pct", 5.0)
    c.setdefault("confirm_polls", 2)
    c.setdefault("max_lines", 25)
    c.setdefault("chat_id", "")
    c.setdefault("title", "SPY + QQQ")
    c.setdefault("enabled", True)
    return c


def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ---------------------------------------------------------------- universe

def wiki(page):
    url = "https://en.wikipedia.org/wiki/" + urllib.parse.quote(page)
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read().decode("utf-8", "replace")


def first_col_tickers(html_text, table_index=0):
    """Pull the first-column ticker out of the Nth wikitable."""
    import re
    import html as htmlmod
    tables = re.findall(r'<table[^>]*wikitable[\s\S]*?</table>', html_text)
    if len(tables) <= table_index:
        return []
    out = []
    for row in re.findall(r'<tr[\s\S]*?</tr>', tables[table_index]):
        cells = re.findall(r'<t[dh][^>]*>([\s\S]*?)</t[dh]>', row)
        if not cells:
            continue
        txt = htmlmod.unescape(re.sub(r'<[^>]+>', '', cells[0])).strip()
        if re.fullmatch(r'[A-Z]{1,5}(\.[A-Z])?', txt):
            out.append(txt)
    return out


def refresh_universe():
    sp = first_col_tickers(wiki("List of S&P 500 companies"), 0)
    nd = first_col_tickers(wiki("List of NASDAQ-100 companies"), 0)
    if len(sp) < 450 or len(nd) < 90:
        raise SystemExit(f"refusing to write a short universe (sp500={len(sp)}, ndx={len(nd)})")
    syms = sorted(set(sp) | set(nd))
    save(UNIVERSE, {
        "updated": datetime.datetime.now(IL).isoformat(timespec="seconds"),
        "sp500": len(sp), "ndx": len(nd), "count": len(syms), "symbols": syms,
    })
    print(f"universe: {len(syms)} symbols (S&P {len(sp)} + NDX {len(nd)})")
    return syms


def universe():
    u = load(UNIVERSE, None)
    if not u or not u.get("symbols"):
        return refresh_universe()
    return u["symbols"]


# ---------------------------------------------------------------- quotes

def pull_chunk(chunk):
    # Ask for 5-minute bars rather than a single daily bar. Not because
    # interval=1d is broken - checked side by side on a healthy feed the two
    # agree to the cent - but because the bar series carries its own timestamp
    # per symbol. On 2026-08-06 Yahoo served Aug 5 data all session with the
    # quote fields frozen, and as it recovered the bars ran ahead of the meta
    # block (AXON: meta 53 min stale, bars current). Bars therefore fail
    # visibly, where a frozen regularMarketPrice fails silently.
    #
    # This is not immunity. If Yahoo goes stale wholesale the bars go stale
    # too. What actually protects us is fresh_only() below, the blind-feed DM
    # and the nightly self-check - not the choice of interval.
    url = ("https://query1.finance.yahoo.com/v7/finance/spark?symbols="
           + ",".join(s.replace(".", "-") for s in chunk)
           + "&range=1d&interval=5m")
    for attempt in range(3):
        try:
            j = fetch(url)
            out = []
            for item in (j.get("spark", {}).get("result") or []):
                r = item["response"][0]
                m = r["meta"]
                stamps = r.get("timestamp") or []
                closes = ((r.get("indicators", {}).get("quote") or [{}])[0]
                          .get("close") or [])
                last = last_at = None
                for t, c in zip(stamps, closes):
                    if c is not None:
                        last, last_at = c, t
                out.append({
                    "symbol": m["symbol"],
                    # Fall back to the quote fields only when the bar series is
                    # empty (pre-open, or a symbol Yahoo has no intraday for).
                    "price": last if last is not None else m.get("regularMarketPrice"),
                    "prev": m.get("chartPreviousClose") or m.get("previousClose"),
                    "at": last_at if last_at is not None else m.get("regularMarketTime"),
                })
            return out
        except Exception:
            if attempt == 2:
                return []
            time.sleep(1.5 * (attempt + 1))
    return []


def quotes(syms):
    chunks = [syms[i:i + CHUNK] for i in range(0, len(syms), CHUNK)]
    out = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for res in ex.map(pull_chunk, chunks):
            out.extend(res)
    return out


# ------------------------------------------------------------ second feed

def et_epoch(stamp):
    """Turn CNBC's '12:39 PM EDT' into an epoch, or None if unparseable.

    CNBC gives a time of day with NO DATE, so a feed frozen since yesterday
    would hand us a plausible-looking clock. We anchor it to today in New York
    and let the caller compare it against the real New York time: a feed stuck
    at yesterday's 4:00 PM close reads three hours into the future at midday,
    and anything that does not sit close to now is refused.
    """
    stamp = (stamp or "").strip()
    if not stamp:
        return None
    for fmt_ in ("%I:%M %p", "%I:%M:%S %p"):
        try:
            t = datetime.datetime.strptime(stamp.rsplit(" ", 1)[0], fmt_).time()
        except ValueError:
            continue
        now = datetime.datetime.now(NY)
        return datetime.datetime.combine(now.date(), t, tzinfo=NY).timestamp()
    return None


def cnbc_chunk(chunk):
    url = ("https://quote.cnbc.com/quote-html-webservice/quote.htm?symbols="
           + "|".join(chunk)
           + "&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json")
    for attempt in range(3):
        try:
            j = fetch(url)
            rows = (j.get("ITVQuoteResult") or {}).get("ITVQuote") or []
            if isinstance(rows, dict):
                rows = [rows]
            out = []
            for q in rows:
                try:
                    price = float(q["last"])
                    prev = float(q["previous_day_closing"])
                except (KeyError, TypeError, ValueError):
                    continue
                out.append({
                    "symbol": q.get("symbol", ""),
                    "price": price,
                    "prev": prev,
                    "at": et_epoch(q.get("last_timedate")),
                })
            return out
        except Exception:
            if attempt == 2:
                return []
            time.sleep(1.5 * (attempt + 1))
    return []


def cnbc_quotes(syms):
    chunks = [syms[i:i + CNBC_CHUNK] for i in range(0, len(syms), CNBC_CHUNK)]
    out = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for res in ex.map(cnbc_chunk, chunks):
            out.extend(res)
    return out


def norm(sym):
    """Yahoo says BRK-B, CNBC says BRK.B — one key for both."""
    return (sym or "").upper().replace("-", ".")


def read_feeds(syms, max_age_min=45):
    """Both feeds, each already filtered down to quotes that are actually live.

    Returns ({feed: fresh quotes}, "yahoo 518/518 · cnbc 517/518"). A feed that
    is serving yesterday's prices ends up empty here rather than wrong, which
    is what lets the other one carry the session on its own.
    """
    raw = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        jobs = {"yahoo": ex.submit(quotes, syms), "cnbc": ex.submit(cnbc_quotes, syms)}
        for name, job in jobs.items():
            try:
                raw[name] = job.result()
            except Exception:
                raw[name] = []
    feeds = {n: fresh_only(qs, max_age_min) for n, qs in raw.items()}
    sizes = " · ".join("%s %d/%d" % (n, len(feeds[n]), len(raw[n])) for n in feeds)
    return feeds, sizes


def merge_hits(feeds, c, veto_age_min=CONFLICT_AGE_MIN):
    """Decide the day's hits from two independent feeds.

    Two rules, each aimed at one of the ways 2026-08-06 went wrong:

    EITHER source is enough to raise an alert. A drop nobody hears about is
    the expensive failure - twelve of them today - so one working feed must be
    able to carry the day alone. Only quotes that passed their own freshness
    check reach this function, so a frozen feed contributes nothing rather
    than dragging the other one down.

    BOTH sources must agree before we send, when both can see the name. If
    they disagree materially one of them is lying and we do not know which, so
    the name is held back and reported to the owner instead of guessed at.
    """
    seen, hits, conflicts = {}, [], []
    for name, qs in feeds.items():
        for q in qs:
            if not q.get("price") or not q.get("prev") or q["prev"] <= c["min_price"]:
                continue
            seen.setdefault(norm(q["symbol"]), {})[name] = q

    now = time.time()
    cutoff = now - veto_age_min * 60
    for sym, by_feed in seen.items():
        pcts = {n: (q["price"] / q["prev"] - 1.0) * 100.0 for n, q in by_feed.items()}
        if not any(p <= -c["drop_pct"] for p in pcts.values()):
            continue
        # A quote may be old enough to still be worth reporting (the close scan
        # accepts three hours) yet far too old to overrule a live one. Only
        # genuinely current quotes get a vote on whether the feeds disagree,
        # otherwise a lagging feed vetoes the working one and we are back to
        # missing real falls - the failure this whole design exists to stop.
        votes = {n: p for n, p in pcts.items()
                 if (by_feed[n].get("at") or 0) >= cutoff}
        # Disagreement only matters when it changes the ANSWER. Two feeds
        # saying -8.4% and -6.3% differ by more than the tolerance yet agree
        # completely on the thing we alert about: this name is past -5%. Held
        # back on 2026-08-06 for exactly that, which is a miss dressed up as
        # caution. Veto only when the feeds straddle the threshold, i.e. one
        # says fire and the other says nothing happened.
        straddles = (len(votes) > 1
                     and any(p <= -c["drop_pct"] for p in votes.values())
                     and any(p > -c["drop_pct"] for p in votes.values()))
        if straddles and max(votes.values()) - min(votes.values()) > DISAGREE_PP:
            # Carry each voter's age into the warning. The STX holds on
            # 2026-08-07 read "cnbc -9.3% yahoo -3.8%" and looked like two feeds
            # contradicting each other, when the real story was one feed twenty
            # minutes behind the other; the age is what makes that visible
            # without going back to the raw endpoints.
            conflicts.append((sym, votes, {n: (now - (by_feed[n].get("at") or 0)) / 60.0
                                           for n in votes}))
            continue
        # When both feeds agree it qualifies, quote the SHALLOWER of the two.
        # Overstating a fall to people deciding what to do with their money is
        # the worse error, and the number they see should be the one we are
        # most confident is true.
        firing = {n: p for n, p in pcts.items() if p <= -c["drop_pct"]}
        src = max(firing, key=firing.get)
        q = by_feed[src]
        hits.append({"symbol": sym, "pct": pcts[src], "price": q["price"],
                     "prev": q["prev"], "src": src, "sources": len(pcts)})
    hits.sort(key=lambda h: h["pct"])
    conflicts.sort(key=lambda x: x[0])
    return hits, conflicts


def movers(qs, min_price, drop_pct):
    """Names whose PREVIOUS close cleared min_price and that are now down >= drop_pct."""
    hits = []
    for q in qs:
        p, prev = q["price"], q["prev"]
        if not p or not prev or prev <= min_price:
            continue
        pct = (p / prev - 1.0) * 100.0
        if pct <= -drop_pct:
            hits.append({"symbol": q["symbol"], "pct": pct, "price": p, "prev": prev})
    hits.sort(key=lambda h: h["pct"])
    return hits


# ---------------------------------------------------------------- market clock

def market_open(now=None):
    """NYSE regular session, ignoring holidays (staleness check covers those)."""
    n = (now or datetime.datetime.now(NY)).astimezone(NY)
    if n.weekday() > 4:
        return False
    mins = n.hour * 60 + n.minute
    return 9 * 60 + 30 <= mins < 16 * 60


def data_is_stale(qs, max_age_min=45):
    """True when every quote's timestamp is old — i.e. a holiday or a dead feed."""
    stamps = [q["at"] for q in qs if q.get("at")]
    if not stamps:
        return True
    age = time.time() - max(stamps)
    return age > max_age_min * 60


def fresh_only(qs, max_age_min=45):
    """Keep only quotes whose own timestamp is recent.

    data_is_stale() looks at the NEWEST stamp in the batch, so a feed that
    refreshes a handful of symbols while serving yesterday's close for the
    rest sails through it — and then every stale name's yesterday-vs-the-day-
    before move gets reported as if it happened today (Yahoo did exactly that
    on 2026-08-06). Judge each quote on its own stamp instead.
    """
    cutoff = time.time() - max_age_min * 60
    return [q for q in qs if q.get("at") and q["at"] >= cutoff]


# ---------------------------------------------------------------- telegram

def token():
    t = os.environ.get("TELEGRAM_BOT_TOKEN")
    if t:
        return t.strip()
    for p in (os.path.join(HERE, "secret_config.py"), os.path.expanduser("~/.telegram_bot_token")):
        try:
            with open(p) as f:
                raw = f.read().strip()
        except OSError:
            continue
        if p.endswith(".py"):
            for line in raw.splitlines():
                if "TELEGRAM_BOT_TOKEN" in line and "=" in line:
                    return line.split("=", 1)[1].strip().strip("'\"")
        elif raw:
            return raw
    raise SystemExit("no telegram token (env TELEGRAM_BOT_TOKEN, secret_config.py, or ~/.telegram_bot_token)")


def push(chat_id, text):
    if not chat_id:
        raise SystemExit("config.json has no chat_id — set it before pushing")
    data = urllib.parse.urlencode({
        "chat_id": str(chat_id), "text": text, "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token()}/sendMessage", data=data, headers=UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        j = json.loads(r.read())
    if not j.get("ok"):
        raise SystemExit(f"telegram refused: {j}")
    return j


# ---------------------------------------------------------------- state

def today():
    return datetime.datetime.now(IL).strftime("%Y-%m-%d")


def get_state():
    s = load(STATE, {})
    if s.get("date") != today():
        s = {"date": today(), "alerted": {}, "streak": {}}
    s.setdefault("alerted", {})
    s.setdefault("streak", {})
    s.setdefault("blind", 0)
    # When each name went out, so the nightly self-check can measure lateness.
    s.setdefault("alert_times", {})
    # message_id of every push, so a wrong one can be deleted inside Telegram's
    # 48-hour window instead of standing in the group forever.
    s.setdefault("sent", [])
    return s


def record_send(st, j, names):
    try:
        mid = j["result"]["message_id"]
    except (KeyError, TypeError):
        mid = None
    st["sent"].append({
        "at": datetime.datetime.now(IL).strftime("%H:%M"),
        "message_id": mid,
        "names": list(names),
    })


def blind_poll(c, reason):
    """Count a session minute we could not see, and say so once if it drags on.

    A dead feed used to be invisible: the run logged a line to movers.log and
    nobody read movers.log. Silence looks exactly like a calm market, so the
    monitor could be down for hours before anyone noticed. After BLIND_ALERT
    consecutive blind polls we tell the owner (once a day, owner only — the
    subscriber group hears about markets, not about our plumbing).
    """
    st = get_state()
    st["blind"] = st.get("blind", 0) + 1
    if st["blind"] == BLIND_ALERT and c.get("owner_chat"):
        push(c["owner_chat"],
             "⚠️ movers is blind — %d minutes with no usable quotes.\n%s\n"
             "No alerts are going out to %s until the feed recovers."
             % (st["blind"], reason, c["title"]))
    save(STATE, st)


def seeing(st):
    """Clear the blind counter after a poll that actually saw the market."""
    if st.get("blind"):
        st["blind"] = 0


# ---------------------------------------------------------------- messages

def fmt(hits, c, mode, extra=""):
    when = datetime.datetime.now(IL).strftime("%a %d %b, %H:%M")
    head = "📉 %s — down %g%%+ today" % (c["title"], c["drop_pct"])
    sub = "%s · %s" % ("live" if mode == "live" else "market close", when)
    lines = [head, sub, ""]
    shown = hits[:c["max_lines"]]
    for h in shown:
        lines.append("%-6s %6.1f%%   $%.2f" % (h["symbol"], h["pct"], h["price"]))
    if len(hits) > len(shown):
        lines.append("… +%d more" % (len(hits) - len(shown)))
    if extra:
        lines.append("")
        lines.append(extra)
    return "\n".join(lines)


# ---------------------------------------------------------------- modes

def run_live(c, dry=False):
    now = datetime.datetime.now(NY)
    if not dry and not market_open(now):
        print(f"market closed ({now:%H:%M} ET) — skip")
        return
    feeds, sizes = read_feeds(universe())
    if not any(feeds.values()):
        print("both feeds dark (%s) — skip" % sizes)
        if not dry:
            blind_poll(c, "Neither Yahoo nor CNBC returned usable prices.")
        return
    print("feeds: %s" % sizes)
    if not dry and not all(feeds.values()):
        # One feed alone still alerts — that is the whole point of having two —
        # but a feed that goes dark mid-session is worth knowing about.
        blind_poll(c, "One feed is down: %s. Still alerting from the other." % sizes)

    st = get_state()
    seeing(st)

    hits, conflicts = merge_hits(feeds, c)
    # A name that already went out today cannot go out again — the dedup below
    # sees to that — so holding it back withholds nothing, and saying "nothing
    # sent for STX" twenty minutes after STX was sent is simply false. Warn only
    # about names where the disagreement is actually costing the group an alert.
    conflicts = [k for k in conflicts if k[0] not in st["alerted"]]
    if conflicts:
        print("held back %d disagreements: %s"
              % (len(conflicts), ", ".join(k[0] for k in conflicts)))
        if not dry and c.get("owner_chat"):
            push(c["owner_chat"], "⚠️ feeds disagree, nothing sent for:\n" + "\n".join(
                "%-6s %s" % (s, "  ".join("%s %.1f%% (%.0fm)" % (n, v[n], a[n])
                                          for n in sorted(v)))
                for s, v, a in conflicts[:15]))
    fresh = []
    now_down = {h["symbol"] for h in hits}
    for h in hits:
        s = h["symbol"]
        st["streak"][s] = st["streak"].get(s, 0) + 1
        if st["streak"][s] >= c["confirm_polls"] and s not in st["alerted"]:
            st["alerted"][s] = round(h["pct"], 2)
            st["alert_times"][s] = datetime.datetime.now(IL).strftime("%H:%M")
            fresh.append(h)
    # A vetoed name is one we cannot read, not one that recovered. Zeroing its
    # streak made every disagreement cost the full confirmation over again on
    # top of the poll it already cost, which is how a name can cross at 16:30
    # and go out three hours later. Freeze those streaks instead.
    held = {k[0] for k in conflicts}
    for s in list(st["streak"]):
        if s not in now_down and s not in held:
            st["streak"][s] = 0

    print("down %d · confirmed-new %d" % (len(hits), len(fresh)))
    if dry:
        for h in hits:
            print("  %-6s %6.1f%%  $%.2f (prev $%.2f)  [%s, %d feed%s]"
                  % (h["symbol"], h["pct"], h["price"], h["prev"], h["src"],
                     h["sources"], "" if h["sources"] == 1 else "s"))
        return
    if fresh and c["enabled"]:
        record_send(st, push(c["chat_id"], fmt(fresh, c, "live")),
                    [h["symbol"] for h in fresh])
    save(STATE, st)


def run_close(c, dry=False):
    # Cron fires this at both 22:05 and 23:05 Israel time so the two weeks a
    # year when the DST offsets disagree stay covered; this gate means only
    # the run that lands just after the New York close actually reports.
    n = datetime.datetime.now(NY)
    if not dry and (n.weekday() > 4 or not (16 * 60 <= n.hour * 60 + n.minute < 17 * 60 + 30)):
        print(f"not just after the close ({n:%H:%M} ET) — skip")
        return
    feeds, sizes = read_feeds(universe(), max_age_min=180)
    if not any(feeds.values()):
        print("both feeds dark (%s) — skip" % sizes)
        return
    hits, conflicts = merge_hits(feeds, c, veto_age_min=CLOSE_CONFLICT_AGE_MIN)
    if conflicts:
        print("held back %d disagreements: %s"
              % (len(conflicts), ", ".join(k[0] for k in conflicts)))
    print("feeds: %s · closed down %d" % (sizes, len(hits)))
    if dry:
        for h in hits:
            print("  %-6s %6.1f%%  $%.2f (prev $%.2f)" % (h["symbol"], h["pct"], h["price"], h["prev"]))
        return
    if not hits:
        return  # a quiet day stays quiet
    scanned = len({norm(q["symbol"]) for qs in feeds.values() for q in qs
                   if q["prev"] and q["prev"] > c["min_price"]})
    extra = "%d of %d names over $%g" % (len(hits), scanned, c["min_price"])
    if c["enabled"]:
        st = get_state()
        record_send(st, push(c["chat_id"], fmt(hits, c, "close", extra)),
                    [h["symbol"] for h in hits])
        for h in hits:
            st["alerted"].setdefault(h["symbol"], round(h["pct"], 2))
        save(STATE, st)


# ---------------------------------------------------------------- self-check

def day_truth(sym):
    """What a symbol ACTUALLY did today, read back one symbol at a time.

    Deliberately a different code path from pull_chunk: its own endpoint (v8
    chart, not v7 spark), its own parsing, one symbol per request. If both
    agreed by construction the check would only prove we are consistently
    wrong — the whole point is that a bug in the live path does not travel
    into the audit that is supposed to catch it.
    """
    try:
        j = fetch("https://query1.finance.yahoo.com/v8/finance/chart/"
                  "%s?range=1d&interval=5m" % sym.replace(".", "-"))
        r = j["chart"]["result"][0]
        prev = r["meta"].get("chartPreviousClose")
        stamps = r.get("timestamp") or []
        closes = (r["indicators"]["quote"][0].get("close") or [])
    except Exception:
        return None
    bars = [(t, c) for t, c in zip(stamps, closes) if c is not None]
    if not prev or not bars:
        return None
    pct = lambda c: (c / prev - 1.0) * 100.0
    trough_t, trough_c = min(bars, key=lambda b: b[1])
    return {
        "symbol": sym, "prev": prev,
        "low_pct": pct(trough_c), "low_at": trough_t,
        "close_pct": pct(bars[-1][1]),
        "bars": bars,
    }


def sa_truth(sym):
    """The day's low and close from stockanalysis.com — the audit's second eye.

    Yahoo cannot be both the suspect and the witness: the nightly check reading
    only Yahoo would have signed off on 2026-08-06 as a quiet day. This gives
    an independent intraday low and, unlike CNBC's quote, a stamp that carries
    a DATE, so we can tell a frozen feed from a live one.
    """
    try:
        d = fetch("https://stockanalysis.com/api/quotes/s/%s" % sym)["data"]
        prev, low, last = float(d["cl"]), float(d["l"]), float(d["p"])
    except Exception:
        return None
    if not prev:
        return None
    stamp = str(d.get("u") or "")
    if datetime.datetime.now(NY).strftime("%b %-d, %Y") not in stamp:
        return None  # not today's data — treat as no opinion, never as calm
    return {"symbol": sym, "prev": prev,
            "low_pct": (low / prev - 1.0) * 100.0,
            "close_pct": (last / prev - 1.0) * 100.0}


def traded_today():
    """Did the market actually trade today? Cheaper and truer than a calendar."""
    t = day_truth("SPY")
    return bool(t and t["bars"])


def first_cross(truth, drop_pct):
    for t, c in truth["bars"]:
        if (c / truth["prev"] - 1.0) * 100.0 <= -drop_pct:
            return t
    return None


def hhmm(ts):
    # A name can be real on the second witness while Yahoo has no bar crossing
    # for it at all — exactly the case the second witness exists to catch, so
    # it must print, not crash.
    if not ts:
        return "  ?  "
    return datetime.datetime.fromtimestamp(ts, IL).strftime("%H:%M")


def run_verify(c, dry=False):
    """Compare what we sent today against what the market actually did.

    Runs after the close. Answers three questions in order of how much they
    cost us: what did we MISS, what did we send that was WRONG, and what did
    we send LATE. Goes to the owner, never to the subscriber group.
    """
    until = c.get("verify_until")
    if until and today() > until:
        print("self-check window ended %s — skip" % until)
        return
    # Before today's close there is nothing to audit, and worse: Yahoo's 1-day
    # chart still holds the PREVIOUS session, so traded_today() says yes and
    # every one of last session's real falls is counted as a miss for today.
    n = datetime.datetime.now(NY)
    if n.weekday() > 4 or n.hour * 60 + n.minute < 16 * 60:
        print("session has not closed yet — skip")
        return
    if not traded_today():
        print("market did not trade today — skip")
        return

    st = get_state()
    # "alerted" only means the poller decided to fire — with sending paused, or
    # a push that failed, that decision never reached anybody. Trust the record
    # of actual pushes for what the group really saw.
    sent = dict(st.get("alerted", {}))
    delivered = {n for msg in st.get("sent", []) for n in msg.get("names", [])}
    times = st.get("alert_times", {})
    syms = universe()
    truths, second = {}, {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        for t in ex.map(day_truth, syms):
            if t:
                truths[t["symbol"]] = t
    # CNBC in bulk for the second opinion: 9 requests for the whole universe.
    # stockanalysis is one request per symbol and starts refusing after about
    # forty, so it is spent only on the names actually in dispute below.
    for q in fresh_only(cnbc_quotes(syms), 180):
        if q.get("prev"):
            pct = (q["price"] / q["prev"] - 1.0) * 100.0
            second[norm(q["symbol"])] = {"symbol": norm(q["symbol"]),
                                         "prev": q["prev"], "low_pct": pct,
                                         "close_pct": pct}

    # A name counts as real if EITHER witness saw it fall. Requiring both would
    # let one broken source hide a genuine drop, which is the failure being
    # audited in the first place.
    def fell(t):
        return t and t["prev"] > c["min_price"] and t["low_pct"] <= -c["drop_pct"]

    real = {s: truths[s] for s in truths if fell(truths[s])}
    for s, t in second.items():
        if fell(t) and s not in real and s in truths:
            real[s] = truths[s]
    # CNBC only knows the last price, so a name that fell 5% intraday and
    # recovered looks calm to it. Only the reverse — CNBC saw a fall Yahoo
    # missed — is evidence of a real blind spot, and each of those is put to a
    # third source before it is reported as ours.
    split = []
    for s in sorted(set(second) & set(truths)):
        if fell(second[s]) and not fell(truths[s]):
            third = sa_truth(s)
            split.append((s, truths[s]["low_pct"], second[s]["low_pct"],
                          third["low_pct"] if third else None))
            if third and fell(third):
                real.setdefault(s, truths[s])
    missed = sorted(set(real) - set(sent), key=lambda s: real[s]["low_pct"])
    wrong = sorted(set(sent) - set(real), key=lambda s: sent[s])
    late = []
    for s in sorted(set(real) & set(sent)):
        cross, when = first_cross(real[s], c["drop_pct"]), times.get(s)
        if not cross or not when:
            continue
        gap = (datetime.datetime.strptime(when, "%H:%M").hour * 60
               + datetime.datetime.strptime(when, "%H:%M").minute) \
            - (datetime.datetime.fromtimestamp(cross, IL).hour * 60
               + datetime.datetime.fromtimestamp(cross, IL).minute)
        if gap > LATE_MIN + BAR_MIN:
            late.append((s, hhmm(cross), when, gap))

    undelivered = sorted((set(real) & set(sent)) - delivered)
    lines = ["🔍 movers self-check · %s" % datetime.datetime.now(IL).strftime("%a %d %b"),
             "owed %d · delivered %d · missed %d · wrong %d · late %d"
             % (len(real), len(delivered & set(real)), len(missed), len(wrong), len(late))]
    if undelivered:
        lines += ["", "DETECTED BUT NEVER SENT (%d)" % len(undelivered),
                  "  " + ", ".join(undelivered)]
    if missed:
        lines += ["", "MISSED (real %g%%+ drop, never alerted)" % c["drop_pct"]]
        for s in missed[:15]:
            lines.append("%-6s %6.1f%%  crossed %s"
                         % (s, real[s]["low_pct"], hhmm(first_cross(real[s], c["drop_pct"]))))
    if wrong:
        lines += ["", "WRONG (alerted, never actually fell that far)"]
        for s in wrong[:15]:
            got = truths.get(s)
            lines.append("%-6s sent %6.1f%%  actual low %s"
                         % (s, sent[s], "%.1f%%" % got["low_pct"] if got else "no data"))
    if late:
        lines += ["", "LATE (alerted over %d min after the crossing bar)"
                  % (LATE_MIN + BAR_MIN)]
        for s, cross, when, gap in late[:15]:
            lines.append("%-6s crossed in the %s bar, sent %s  (+%d min at worst)"
                         % (s, cross, when, gap))
    if split:
        lines += ["", "BLIND SPOTS (%d) — CNBC saw a fall Yahoo did not" % len(split)]
        for sym, yp, cp, tp in split[:10]:
            lines.append("%-6s yahoo %6.1f%%  cnbc %6.1f%%  third %s"
                         % (sym, yp, cp, "%.1f%%" % tp if tp is not None else "n/a"))
    lines += ["", "coverage: yahoo %d/%d · cnbc %d/%d"
              % (len(truths), len(syms), len(second), len(syms))]
    if st.get("blind"):
        lines.append("ended the day blind for %d polls" % st["blind"])
    if not c.get("enabled"):
        lines.append("⏸ SENDING IS OFF — nothing reached the group today")
    if not (missed or wrong or late):
        lines.append("✅ everything we owed, we sent, on time")

    report = "\n".join(lines)
    print(report)
    if not dry and c.get("owner_chat"):
        push(c["owner_chat"], report)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "once"
    c = cfg()
    if mode == "refresh":
        refresh_universe()
    elif mode == "live":
        run_live(c)
    elif mode == "close":
        run_close(c)
    elif mode == "verify":
        run_verify(c)
    elif mode == "once":
        which = sys.argv[2] if len(sys.argv) > 2 else "live"
        {"close": run_close, "verify": run_verify}.get(which, run_live)(c, dry=True)
    elif mode == "test":
        chat = sys.argv[2] if len(sys.argv) > 2 else c["chat_id"]
        push(chat, "✅ movers is wired up.\n%s, over $%g, down %g%%+.\nLive during the session + one summary at the close."
             % (c["title"], c["min_price"], c["drop_pct"]))
        print("sent to", chat)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
