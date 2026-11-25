"""Чтение бирж фриланса без входа в аккаунты: Kwork (список проектов в странице) и FL.ru (RSS).

Заказы превращаются в «сообщения» служебных чатов (-1001 Kwork, -1002 FL.ru) и идут через тот же конвейер,
что и Telegram: правила, проверка заказа, ИИ-фильтр, группа, таблица. Откликаться на биржах вы будете руками.
"""
import html
import json
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0 Safari/537.36")
MSK = timezone(timedelta(hours=3))
KWORK_CHAT, FL_CHAT, FREELANCE_CHAT = -1001, -1002, -1003
BOARD_CHATS = {KWORK_CHAT: "Kwork · заказы", FL_CHAT: "FL.ru · проекты", FREELANCE_CHAT: "Freelance.ru · задания"}
FREELANCE_URLS = ["https://freelance.ru/task", "https://freelance.ru/task?page=2"]

# все категории (свежие заказы вперемешку) + «Разработка и IT» (c=11) + дизайн (c=15, там карточки и инфографика)
KWORK_URLS = [
    "https://kwork.ru/projects", "https://kwork.ru/projects?page=2", "https://kwork.ru/projects?page=3",
    "https://kwork.ru/projects?c=11", "https://kwork.ru/projects?c=11&page=2", "https://kwork.ru/projects?c=11&page=3",
    "https://kwork.ru/projects?c=15",
]
FL_URLS = [
    "https://www.fl.ru/rss/all.xml",
    "https://www.fl.ru/rss/all.xml?category=5",   # программирование
    "https://www.fl.ru/rss/all.xml?category=2",   # сайты
]
MAX_DESC = 1500


def _get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def _money(value):
    try:
        return f"{int(float(value)):,}".replace(",", " ")
    except (TypeError, ValueError):
        return None


def _clean(text):
    text = re.sub(r"<[^>]+>", " ", html.unescape(text or ""))
    return " ".join(text.split())


def kwork_items():
    """Открытые заказы Kwork с первых страниц. Страница отдаёт список прямо в HTML (блок "wants")."""
    seen, out = set(), []
    for url in KWORK_URLS:
        try:
            page = _get(url)
            i = page.find('"wants":[')
            wants = json.JSONDecoder().raw_decode(page[i + 8:])[0] if i >= 0 else []
        except Exception:
            wants = []
        for w in wants:
            wid = w.get("id")
            if not wid or wid in seen or w.get("status") != "active":
                continue
            seen.add(wid)
            try:  # время создания на странице московское
                created = datetime.strptime(w.get("date_create"), "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
            except (TypeError, ValueError):
                created = datetime.now(timezone.utc)
            low, high = _money(w.get("priceLimit")), _money(w.get("possiblePriceLimit"))
            budget = f"\n💳 Бюджет: от {low} ₽ до {high} ₽" if low and high else ""
            desc = _clean(w.get("description"))[:MAX_DESC]
            text = f"📌 {_clean(w.get('name'))}\n📝 {desc}{budget}\nОтклик: https://kwork.ru/projects/{wid}"
            out.append({"chat_id": KWORK_CHAT, "msg_id": int(wid), "date": created.astimezone(timezone.utc),
                        "text": text})
        time.sleep(1.0)
    return out


def fl_items():
    """Свежие проекты FL.ru из RSS (в каждой ленте около 60 последних)."""
    seen, out = set(), []
    for url in FL_URLS:
        try:
            root = ET.fromstring(_get(url).encode("utf-8"))
        except Exception:
            continue
        for it in root.findall(".//item"):
            link = (it.findtext("link") or "").strip()
            m = re.search(r"/projects/(\d+)/", link)
            if not m or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            try:
                created = parsedate_to_datetime(it.findtext("pubDate")).astimezone(timezone.utc)
            except (TypeError, ValueError):
                created = datetime.now(timezone.utc)
            desc = _clean(it.findtext("description"))[:MAX_DESC]
            text = (f"📌 {_clean(it.findtext('title'))}\n📝 {desc}\n"
                    f"Категория: {_clean(it.findtext('category'))}\nОтклик: {link}")
            out.append({"chat_id": FL_CHAT, "msg_id": int(m.group(1)), "date": created, "text": text})
        time.sleep(1.0)
    return out


def freelance_items():
    """Свежие задания Freelance.ru: карточки лежат прямо в HTML страницы ленты."""
    seen, out = set(), []
    for url in FREELANCE_URLS:
        try:
            page = _get(url)
        except Exception:
            continue
        for card in re.split(r'<article class="task-card[^"]*">', page)[1:]:
            card = card.split("</article>")[0]
            m = re.search(r'href="/task/view/(\d+)"\s+title="([^"]*)"', card)
            if not m or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            tid, title = m.group(1), _clean(m.group(2))
            desc = _clean((re.search(r'<p class="task-card__desc">(.*?)</p>', card, re.S) or [None, ""])[1])[:MAX_DESC]
            cat = _clean((re.search(r'task-chip--cat">(.*?)</span>', card, re.S) or [None, ""])[1])
            pay = _clean((re.search(r'class="task-card__budget[^"]*">(.*?)</div>', card, re.S) or [None, ""])[1])
            when = re.search(r'title="(\d\d\.\d\d\.\d{4} \d\d:\d\d)"', card)
            try:  # время на странице московское
                created = datetime.strptime(when.group(1), "%d.%m.%Y %H:%M").replace(tzinfo=MSK).astimezone(timezone.utc)
            except (AttributeError, ValueError):
                created = datetime.now(timezone.utc)
            budget = f"\n💳 Бюджет: {pay}" if re.search(r"\d", pay) else ""
            text = (f"📌 {title}\n📝 {desc}{budget}\nКатегория: {cat}\n"
                    f"Отклик: https://freelance.ru/task/view/{tid}")
            out.append({"chat_id": FREELANCE_CHAT, "msg_id": int(tid), "date": created, "text": text})
        time.sleep(1.0)
    return out


def fetch_all():
    """Все заказы со всех бирж. Сетевой вызов без доступа к базе: безопасно запускать в отдельном потоке."""
    return kwork_items() + fl_items() + freelance_items()


def store(con, items, max_age_hours=24):
    """Кладёт новые заказы в базу как непроверенные сообщения. Возвращает число новых."""
    now = datetime.now(timezone.utc)
    for chat_id, title in BOARD_CHATS.items():
        con.execute("INSERT OR IGNORE INTO chats (id, username, title, kind, peer, participants, found_by, tracked,"
                    " added_at) VALUES (?,?,?,?,?,?,?,0,?)",
                    (chat_id, None, title, "channel", "chat", None, "биржа", now.isoformat()))
    new = 0
    for it in items:
        if now - it["date"] > timedelta(hours=max_age_hours):
            continue
        cur = con.execute(
            "INSERT OR IGNORE INTO messages (chat_id, msg_id, date, sender_id, sender_username, sender_name, text,"
            " checked) VALUES (?,?,?,?,?,?,?,0)",
            (it["chat_id"], it["msg_id"], it["date"].isoformat(), None, None, None, it["text"]))
        new += cur.rowcount
    con.commit()
    return new
