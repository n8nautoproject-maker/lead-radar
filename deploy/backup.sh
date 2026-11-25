#!/usr/bin/env bash
# Ежедневная копия базы заявок. В копию не попадают файл сессии Telegram и .env (ключи).
# Хранится 7 последних копий в /opt/tg-leads/backups. Запускается таймером tg-leads-backup.timer.
set -euo pipefail

APP=/opt/tg-leads
OUT="$APP/backups"
STAMP=$(date +%Y-%m-%d)
mkdir -p "$OUT"

# sqlite3.backup даёт целостную копию, даже пока служба пишет в базу
"$APP/venv/bin/python" - "$APP/data/leads.db" "$OUT/leads-$STAMP.db" <<'PY'
import sqlite3, sys
src = sqlite3.connect(sys.argv[1])
dst = sqlite3.connect(sys.argv[2])
src.backup(dst)
dst.close()
PY

gzip -f "$OUT/leads-$STAMP.db"
ls -1t "$OUT"/leads-*.db.gz | tail -n +8 | xargs -r rm --
echo "копия готова: $OUT/leads-$STAMP.db.gz ($(du -h "$OUT/leads-$STAMP.db.gz" | cut -f1))"
