#!/bin/bash
# One press: post the rules to the group, then today's end-of-day alert,
# then un-mute the automatic alerts.
set -e
cd "$(dirname "$0")"
T=$(cat ~/.telegram_bot_token)
CHAT=$(python3 -c 'import json;print(json.load(open("config.json"))["chat_id"])')

post() {  # post <file>
  curl -sS -X POST "https://api.telegram.org/bot$T/sendMessage" \
    --data-urlencode "chat_id=$CHAT" \
    --data-urlencode "text@$1" \
    --data-urlencode "disable_web_page_preview=true" | grep -q '"ok":true'
}

# Rebuild the end-of-day message from live data, so it is never stale.
PYTHONPATH=. python3 - <<'PY'
import movers
c = movers.cfg()
qs = movers.quotes(movers.universe())
hits = movers.movers(qs, c["min_price"], c["drop_pct"])
scanned = sum(1 for q in qs if q["prev"] and q["prev"] > c["min_price"])
extra = "%d of %d names over $%g" % (len(hits), scanned, c["min_price"])
open("eod.txt", "w").write(movers.fmt(hits, c, "close", extra))
PY

post intro.txt
echo "· rules posted"
post eod.txt
echo "· end-of-day alert posted"

python3 - <<'PY'
import json
c = json.load(open("config.json")); c["enabled"] = True
json.dump(c, open("config.json", "w"), indent=2)
PY
echo "· alerts are now live (cron takes over tomorrow)"
