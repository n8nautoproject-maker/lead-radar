#!/usr/bin/env bash
# Установка tg-leads на сервер Ubuntu/Debian. Запускать от root: sudo bash setup.sh
# Предполагается, что файлы проекта уже лежат в /opt/tg-leads.
set -euo pipefail

APP=/opt/tg-leads
USER_NAME=tgleads

[ "$(id -u)" -eq 0 ] || { echo "Запустите от root: sudo bash setup.sh"; exit 1; }
[ -f "$APP/tg.py" ] || { echo "Нет $APP/tg.py — сначала загрузите файлы проекта"; exit 1; }

apt-get update -y
apt-get install -y python3 python3-venv python3-pip ufw fail2ban

id -u "$USER_NAME" >/dev/null 2>&1 || useradd --system --home "$APP" --shell /usr/sbin/nologin "$USER_NAME"

python3 -m venv "$APP/venv"
"$APP/venv/bin/pip" install --upgrade pip
"$APP/venv/bin/pip" install -r "$APP/deploy/requirements.txt"

mkdir -p "$APP/data"
chown -R "$USER_NAME":"$USER_NAME" "$APP"
chmod 700 "$APP" "$APP/data"
[ -f "$APP/.env" ] && chmod 600 "$APP/.env"
[ -f "$APP/tg_leads.session" ] && chmod 600 "$APP/tg_leads.session"

install -m 644 "$APP/deploy/tg-leads.service" /etc/systemd/system/tg-leads.service
systemctl daemon-reload

# Файрвол: снаружи доступен только SSH
ufw allow OpenSSH
ufw --force enable
systemctl enable --now fail2ban

echo
echo "Готово. Дальше: войти в Telegram на сервере (один раз), затем запустить сервис:"
echo "  sudo -u $USER_NAME $APP/venv/bin/python $APP/tg.py login"
echo "  systemctl enable --now tg-leads"
echo "  journalctl -u tg-leads -f"
