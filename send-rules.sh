#!/bin/bash
# Post the current rules text to the group. Nothing else.
set -e
cd "$(dirname "$0")"
T=$(cat ~/.telegram_bot_token)
CHAT=$(python3 -c 'import json;print(json.load(open("config.json"))["chat_id"])')
curl -sS -X POST "https://api.telegram.org/bot$T/sendMessage" \
  --data-urlencode "chat_id=$CHAT" \
  --data-urlencode "text@intro.txt" \
  --data-urlencode "disable_web_page_preview=true" | grep -q '"ok":true'
echo "rules posted to the group"
