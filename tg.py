"""tg-leads: поиск групп в Telegram, сбор сообщений и отбор лидов.

Запуск:  .\\tg <команда> [аргументы]      (или .venv\\Scripts\\python tg.py ...)
Справка: .\\tg -h   и   .\\tg <команда> -h
"""
import argparse
import asyncio
import hashlib
import inspect
import json
import math
import os
import random
import re
import shutil
import sqlite3
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boards
from telethon import TelegramClient, errors, events
from telethon.tl.functions.channels import CreateChannelRequest, GetFullChannelRequest, JoinChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import ExportChatInviteRequest, ImportChatInviteRequest, SearchGlobalRequest
from telethon.tl.types import (Channel, Chat, InputMessagesFilterEmpty, InputPeerEmpty, Message, PeerChannel, PeerChat,
                               PeerUser, User)
from telethon.utils import resolve_id

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DB_PATH = DATA / "leads.db"
SESSION = str(ROOT / "tg_leads")  # -> tg_leads.session (это доступ к аккаунту, никому не передавать)
KEYWORDS = ROOT / "keywords.json"

BLOCKED_TITLE_RE = re.compile(r"(?!)")  # названия чатов, которые не подключать (по умолчанию пусто)
JOINS_PER_DAY = 20  # больше вступлений в сутки — заметный риск ограничений от Telegram
KIND_RU = {"group": "группа", "channel": "канал"}
STATUS_RU = {"new": "новый", "ok": "лид", "no": "не лид", "done": "в работе"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
  id INTEGER PRIMARY KEY, username TEXT, title TEXT, kind TEXT, peer TEXT, participants INTEGER,
  found_by TEXT, tracked INTEGER NOT NULL DEFAULT 0, last_msg_id INTEGER NOT NULL DEFAULT 0,
  added_at TEXT, parsed_at TEXT);
CREATE TABLE IF NOT EXISTS messages (
  chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, date TEXT NOT NULL,
  sender_id INTEGER, sender_username TEXT, sender_name TEXT, text TEXT NOT NULL,
  checked INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (chat_id, msg_id));
CREATE TABLE IF NOT EXISTS leads (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL,
  niche TEXT, matched TEXT, score INTEGER, text_hash TEXT, dup_count INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'new', note TEXT, created_at TEXT, sent_at TEXT, UNIQUE (chat_id, msg_id));
CREATE INDEX IF NOT EXISTS idx_leads_hash ON leads(text_hash);
CREATE TABLE IF NOT EXISTS pains (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL,
  kind TEXT, pain TEXT, hot INTEGER NOT NULL DEFAULT 0, draft TEXT, created_at TEXT, sheet_at TEXT,
  UNIQUE (chat_id, msg_id));
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, text_hash TEXT,
  title TEXT, salary TEXT, fmt TEXT, kind TEXT, employer TEXT, why TEXT, created_at TEXT, sent_at TEXT,
  UNIQUE (chat_id, msg_id));
CREATE TABLE IF NOT EXISTS joins (chat_id INTEGER, joined_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
"""


# ---------- общее ----------

def now_utc():
    return datetime.now(timezone.utc)


def fmt_local(iso):
    return datetime.fromisoformat(iso).astimezone().strftime("%d.%m.%Y %H:%M")


def db():
    DATA.mkdir(exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    columns = {r["name"] for r in con.execute("PRAGMA table_info(leads)")}
    if "sent_at" not in columns:
        con.execute("ALTER TABLE leads ADD COLUMN sent_at TEXT")  # база из прошлых версий
    if "sent_msg_id" not in columns:
        con.execute("ALTER TABLE leads ADD COLUMN sent_msg_id INTEGER")
    if "sheet_at" not in columns:
        con.execute("ALTER TABLE leads ADD COLUMN sheet_at TEXT")  # когда заявка записана в Google-таблицу
    if "llm" not in columns:
        con.execute("ALTER TABLE leads ADD COLUMN llm TEXT")  # JSON: тип, суть, причина от ИИ и данные Kwork
    if "pain_checked" not in {r["name"] for r in con.execute("PRAGMA table_info(messages)")}:
        con.execute("ALTER TABLE messages ADD COLUMN pain_checked INTEGER NOT NULL DEFAULT 0")  # поиск болей селлеров
    if "sheet_at" not in {r["name"] for r in con.execute("PRAGMA table_info(jobs)")}:
        con.execute("ALTER TABLE jobs ADD COLUMN sheet_at TEXT")  # когда вакансия записана на лист «Вакансии»
    if "job_checked" not in {r["name"] for r in con.execute("PRAGMA table_info(messages)")}:
        con.execute("ALTER TABLE messages ADD COLUMN job_checked INTEGER NOT NULL DEFAULT 0")  # поиск вакансий
    chat_columns = {r["name"] for r in con.execute("PRAGMA table_info(chats)")}
    if "trial_state" not in chat_columns:
        con.execute("ALTER TABLE chats ADD COLUMN trial_state TEXT")  # trial / kept / dropped, пусто у чатов, добавленных вручную
    if "trial_until" not in chat_columns:
        con.execute("ALTER TABLE chats ADD COLUMN trial_until TEXT")
    return con


def load_env():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
    api_id = os.environ.get("TG_API_ID", "")
    api_hash = os.environ.get("TG_API_HASH", "")
    if not api_id.isdigit() or not api_hash:
        sys.exit("Заполните TG_API_ID и TG_API_HASH в файле .env (образец — .env.example)")
    return int(api_id), api_hash


def make_client():
    api_id, api_hash = load_env()
    # FloodWait до 2 минут Telethon переждёт сам, дольше — вернёт ошибку
    return TelegramClient(SESSION, api_id, api_hash, flood_sleep_threshold=120)


_lock_handle = None


def take_lock():
    """Одна сессия Telegram за раз: два подключения с одним ключом Telegram может отозвать.

    Блокировку снимает сама система при завершении процесса, поэтому после сбоя
    ничего чистить руками не нужно.
    """
    global _lock_handle
    DATA.mkdir(exist_ok=True)
    handle = open(DATA / "tg.lock", "a+")
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        sys.exit("Telegram уже занят другой командой — вероятно, работает watch. Остановите её и повторите.")
    _lock_handle = handle  # держим файл открытым, пока работает программа


async def connect():
    take_lock()
    client = make_client()
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        sys.exit("Нет входа в Telegram. Сначала выполните в терминале: .\\tg login")
    return client


def display_name(entity):
    if isinstance(entity, User):
        return " ".join(filter(None, [entity.first_name, entity.last_name])) or None
    return getattr(entity, "title", None)


def chat_kind(entity):
    if isinstance(entity, Channel):
        return "group" if (entity.megagroup or entity.gigagroup) else "channel"
    if isinstance(entity, Chat):
        return "group"
    return None


def parse_ref(ref):
    """@name, name, t.me/name, t.me/+hash, t.me/c/123/..., 123, -100123 -> (тип, значение)."""
    ref = ref.strip()
    m = re.match(r"(?:https?://)?(?:t|telegram)\.me/(.+)", ref, re.I)
    if m:
        path = m.group(1).strip("/")
        if path.startswith("+") or path.startswith("joinchat/"):
            return "invite", path.split("/")[-1].lstrip("+")
        if path.startswith("c/"):
            return "id", int(path.split("/")[1])
        return "username", path.split("/")[0]
    if re.fullmatch(r"-?\d+", ref):
        n = int(ref)
        if n < 0:
            s = str(-n)
            n = int(s[3:]) if s.startswith("100") else int(s)
        return "id", n
    return "username", ref.lstrip("@")


def find_chat_row(con, ref):
    kind, value = parse_ref(ref)
    if kind == "id":
        return con.execute("SELECT * FROM chats WHERE id=?", (value,)).fetchone()
    if kind == "username":
        return con.execute("SELECT * FROM chats WHERE lower(username)=lower(?)", (value,)).fetchone()
    return None


async def resolve_entity(client, ref):
    kind, value = parse_ref(ref)
    if kind == "invite":
        raise ValueError("это приватная ссылка-приглашение — используйте команду join")
    if kind == "id":
        try:
            return await client.get_entity(PeerChannel(value))
        except ValueError:
            try:
                return await client.get_entity(PeerChat(value))
            except ValueError:
                raise ValueError("чат не найден — если вы в нём состоите, выполните .\\tg dialogs") from None
    return await client.get_entity(value)


async def input_entity_for_row(client, row):
    # сначала по id из кэша сессии (без лишних запросов), для открытых чатов — запасной вариант по username
    peer = PeerChat(row["id"]) if row["peer"] == "chat" else PeerChannel(row["id"])
    try:
        return await client.get_input_entity(peer)
    except ValueError:
        if not row["username"]:
            raise ValueError("закрытый чат не найден в кэше сессии — выполните .\\tg dialogs") from None
        return await client.get_input_entity(row["username"])


def save_chat(con, entity, found_by=None, tracked=None):
    """Добавляет или обновляет чат. Возвращает True, если чат новый."""
    participants = getattr(entity, "participants_count", None)
    username = getattr(entity, "username", None)
    peer = "chat" if isinstance(entity, Chat) else "channel"
    row = con.execute("SELECT found_by FROM chats WHERE id=?", (entity.id,)).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO chats (id, username, title, kind, peer, participants, found_by, tracked, added_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (entity.id, username, entity.title, chat_kind(entity), peer, participants, found_by,
             int(bool(tracked)), now_utc().isoformat()))
        return True
    found = row["found_by"] or ""
    if found_by and found_by not in found.split(" | "):
        found = f"{found} | {found_by}" if found else found_by
    con.execute(
        "UPDATE chats SET username=?, title=?, kind=?, peer=?, participants=COALESCE(?, participants), found_by=?,"
        " tracked=CASE WHEN ? IS NULL THEN tracked ELSE ? END WHERE id=?",
        (username, entity.title, chat_kind(entity), peer, participants, found or None,
         tracked, int(bool(tracked)), entity.id))
    return False


def print_chats(rows):
    for r in rows:
        mark = "✔" if r["tracked"] else "·"
        members = r["participants"] if r["participants"] is not None else "?"
        handle = f"@{r['username']}" if r["username"] else "(закрытый)"
        print(f"{mark} {r['id']:>13}  {KIND_RU.get(r['kind'], r['kind']):<6} {members:>8}  {handle:<32} {r['title']}")


# ---------- ключевые слова ----------

def normalize(text):
    return text.lower().replace("ё", "е")


def compile_pattern(pattern):
    # * = любое окончание, пробел = любые пробелы; границы слова добавляются автоматически
    body = normalize(pattern).replace("*", r"[\w-]*").replace(" ", r"\s+")
    return re.compile(rf"(?<!\w)(?:{body})(?!\w)")


def load_keywords():
    cfg = json.loads(KEYWORDS.read_text(encoding="utf-8"))
    mode = cfg.get("lead_mode", "dev")
    if mode != "dev":  # блок режима заменяет списки заявок, остальное общее
        cfg = {**cfg, **cfg[mode]}
    return {
        "lead_mode": mode,
        "search_queries": cfg["search_queries"],
        "discovery_queries": cfg.get("discovery_queries", []),
        "global_queries": cfg.get("global_queries", []),
        "boards_enabled": cfg.get("boards_enabled", True),
        "pain_topic": [compile_pattern(p) for p in cfg.get("pains", {}).get("topic", [])],
        "pain_signal": [compile_pattern(p) for p in cfg.get("pains", {}).get("signal", [])],
        "pain_skip": [compile_pattern(p) for p in cfg.get("pains", {}).get("skip", [])],
        "job_tech": [compile_pattern(p) for p in cfg.get("jobs", {}).get("tech", [])],
        "job_offer": [compile_pattern(p) for p in cfg.get("jobs", {}).get("offer", [])],
        "job_skip": [compile_pattern(p) for p in cfg.get("jobs", {}).get("skip", [])],
        "pain_chat": re.compile(cfg.get("pains", {}).get("chat_title") or ".", re.I),
        "min_length": cfg.get("min_length", 25),
        "max_length": cfg.get("max_length", 3000),
        "max_hashtags": cfg.get("max_hashtags", 6),
        "min_price_per_video": cfg.get("min_price_per_video", 3000),
        "min_budget_top": cfg.get("min_budget", 15000),
        "soft_chats": {str(c).lower().lstrip("@") for c in cfg.get("soft_chats", [])},
        "whitelist": {k.lower(): [u.lower().lstrip("@") for u in v]
                      for k, v in cfg.get("whitelist", {}).items()},
        "intent": [compile_pattern(p) for p in cfg["intent"]],
        "negative": [compile_pattern(p) for p in cfg["negative"]],
        "niches": {name: [compile_pattern(p) for p in pats] for name, pats in cfg["niches"].items()},
        "require": {name: [compile_pattern(p) for p in pats] for name, pats in cfg.get("require", {}).items()},
    }


def matches(patterns, text):
    found = []
    for rx in patterns:
        m = rx.search(text)
        if m and m.group(0) not in found:
            found.append(m.group(0))
    return found


def evaluate(text, kw, soft=False, board=False):
    """None — не лид; иначе (ниши, совпадения, балл).

    soft=True — для сообществ предпринимателей: там заказчик пишет «нужен контент»,
    не упоминая нейросети, и требовать ИИ-слова бессмысленно.
    board=True — заказ с биржи (Kwork, FL.ru): это уже заказ, фраз «ищу исполнителя» может и не быть.
    """
    t = normalize(text)
    if not (kw["min_length"] <= len(t) <= kw["max_length"]):
        return None
    if t.count("#") >= kw["max_hashtags"]:
        return None  # склейка хэштегов: «#смм #таргет #дизайн…» — это не заявка
    if matches(kw["negative"], t):
        return None
    if cheap_piecework(t, kw["min_price_per_video"]):
        return None
    if low_budget_range(t, kw["min_budget_top"]):
        return None
    intent = matches(kw["intent"], t)
    if not intent and not board:
        return None
    niche_hits = []
    for name, pats in kw["niches"].items():
        hits = matches(pats, t)
        if not hits:
            continue
        required = None if soft else kw["require"].get(name)
        if required:  # ниша проходит только с нужной составляющей, например с ИИ
            extra = matches(required, t)
            if not extra:
                continue
            hits += extra
        niche_hits.append((name, hits))
    niche_hits.sort(key=lambda h: -len(h[1]))
    if not niche_hits:
        return None
    words = intent + [w for _, hits in niche_hits for w in hits]
    score = len(intent) + 2 * sum(len(hits) for _, hits in niche_hits)
    return ", ".join(name for name, _ in niche_hits), words, score


AMOUNT_RE = re.compile(r"(\d[\d\s]{0,9}?)\s*(₽|руб\w*|р\.|тыс\w*|тр|k|к)(?!\w)")
PER_VIDEO_RE = re.compile(r"за\s+(ролик|видео|рилс\w*|шт\w*|единиц\w*)|/\s*(ролик|видео)")


def cheap_piecework(text, floor):
    """Сдельная ставка ниже порога: «оплата 300 ₽ за ролик» — такие заказы не берём."""
    for m in AMOUNT_RE.finditer(text):
        raw, unit = m.group(1).replace(" ", ""), m.group(2)
        if not raw.isdigit():
            continue
        value = int(raw) * (1000 if unit.startswith(("тыс", "тр", "k", "к")) else 1)
        around = text[max(0, m.start() - 60):m.end() + 60]
        if PER_VIDEO_RE.search(around) and value < floor:
            return True
    return False


BUDGET_RANGE_RE = re.compile(r"бюджет[^\d]{0,20}(?:от\s*([\d\s,]+)₽\s*)?до\s*([\d\s,]+)\s*₽")
BUDGET_ONE_RE = re.compile(r"бюджет[^\d\n]{0,20}(\d[\d\s]*?)\s*(₽|руб\w*|тыс\w*|к|k)(?!\w)")


def low_budget_range(text, floor):
    """Заказ с биржи вида «Бюджет: от 20 000 ₽ до 60 000 ₽».

    На Kwork нижнее число — желаемый бюджет заказчика, верхнее — максимум, который он допускает
    за лучшее предложение. Реальная цена около нижнего, поэтому сравниваем с порогом именно его.
    """
    m = BUDGET_RANGE_RE.search(text)
    if m:
        digits = re.sub(r"\D", "", m.group(1) or m.group(2))
        return bool(digits) and int(digits) < floor
    m = BUDGET_ONE_RE.search(text)  # «(Бюджет: 1 000 ₽)» у FL.ru, «бюджет 30к» в чатах
    if not m:
        return False
    digits = re.sub(r"\D", "", m.group(1))
    if not digits:
        return False
    return int(digits) * (1000 if m.group(2).startswith(("тыс", "к", "k")) else 1) < floor


def kwork_links(msg):
    """Ссылка для отклика с кнопки под постом ленты Kwork (без реферального кода владельца канала)."""
    found = []
    for row in (msg.buttons or []):
        for b in row:
            url = getattr(b, "url", None)
            if url and "kwork.ru/projects" in url:
                found.append(re.sub(r"\?ref=\d+", "", url))
    return found


def text_hash(text):
    return hashlib.sha1(re.sub(r"\W+", " ", normalize(text)).strip()[:500].encode()).hexdigest()


def extract_contacts(text):
    found = re.findall(r"(?<![\w.])@[A-Za-z]\w{3,}", text)
    found += re.findall(r"(?:https?://)?t\.me/[\w+/]+", text)
    found += re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", text)
    found += re.findall(r"(?:\+7|8)[\s(-]*\d{3}[\s)-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}", text)
    return ", ".join(dict.fromkeys(found))


# ---------- команды ----------

async def cmd_login(args):
    client = make_client()
    await client.start()  # спросит телефон, код из Telegram и пароль 2FA (если есть)
    me = await client.get_me()
    print(f"Вход выполнен: {display_name(me)} (@{me.username or '—'})")
    await client.disconnect()


async def cmd_me(args):
    client = await connect()
    try:
        me = await client.get_me()
        print(f"Подключено: {display_name(me)} (@{me.username or '—'}), id {me.id}")
    finally:
        await client.disconnect()


async def cmd_search(args):
    queries = args.queries or load_keywords()["search_queries"]
    con = db()
    client = await connect()
    seen = set()
    try:
        for i, q in enumerate(queries):
            if i:
                await asyncio.sleep(random.uniform(2, 5))
            try:
                res = await client(SearchRequest(q=q, limit=args.limit))
            except errors.FloodWaitError as e:
                print(f"Telegram просит подождать {e.seconds} с — поиск остановлен на «{q}»")
                break
            new = 0
            for ent in res.chats:
                if chat_kind(ent) is None or not getattr(ent, "username", None):
                    continue
                if getattr(ent, "scam", False) or getattr(ent, "fake", False):
                    continue
                if args.min_members and (ent.participants_count or 0) < args.min_members:
                    continue
                new += save_chat(con, ent, found_by=q)
                seen.add(ent.id)
            con.commit()
            print(f"«{q}»: подходящих {sum(1 for c in res.chats if c.id in seen)}, новых {new}")
    finally:
        await client.disconnect()
    if seen:
        marks = ",".join("?" * len(seen))
        rows = con.execute(f"SELECT * FROM chats WHERE id IN ({marks}) ORDER BY participants DESC", list(seen))
        print(f"\nНайдено чатов: {len(seen)}  (✔ — уже отслеживается)")
        print_chats(rows)


async def cmd_dialogs(args):
    """Группы и каналы, в которых состоит аккаунт, включая закрытые и платные."""
    con = db()
    client = await connect()
    ids = []
    try:
        async for dialog in client.iter_dialogs():
            ent = dialog.entity
            if chat_kind(ent) is None or getattr(ent, "deactivated", False):
                continue
            if args.groups and chat_kind(ent) != "group":
                continue
            save_chat(con, ent, found_by="мои чаты")
            ids.append(ent.id)
        con.commit()
    finally:
        await client.disconnect()
    if ids:
        marks = ",".join("?" * len(ids))
        print_chats(con.execute(f"SELECT * FROM chats WHERE id IN ({marks}) ORDER BY kind DESC, participants DESC", ids))
    print(f"\nВаших групп и каналов: {len(ids)}  (✔ — уже отслеживается; добавить: .\\tg track <id>)")


def cmd_chats(args):
    con = db()
    sql, params = "SELECT * FROM chats WHERE 1=1", []
    if args.tracked:
        sql += " AND tracked=1"
    if args.filter:
        sql += " AND (lower(title) LIKE ? OR lower(username) LIKE ? OR lower(found_by) LIKE ?)"
        params += [f"%{args.filter.lower()}%"] * 3
    sql += " ORDER BY tracked DESC, participants DESC LIMIT ?"
    rows = con.execute(sql, params + [args.limit]).fetchall()
    print_chats(rows)
    print(f"\nПоказано: {len(rows)}")


async def cmd_track(args):
    con = db()
    missing = []
    for ref in args.refs:
        row = find_chat_row(con, ref)
        if row:
            con.execute("UPDATE chats SET tracked=1 WHERE id=?", (row["id"],))
            print(f"✔ {row['title']}")
        else:
            missing.append(ref)
    con.commit()
    if not missing:
        return
    client = await connect()
    try:
        for ref in missing:
            try:
                ent = await resolve_entity(client, ref)
            except (ValueError, TypeError, errors.RPCError) as e:
                print(f"✖ {ref}: {e}")
                continue
            if chat_kind(ent) is None:
                print(f"✖ {ref}: это не группа и не канал")
                continue
            save_chat(con, ent, found_by="вручную", tracked=True)
            con.commit()
            print(f"✔ {ent.title}")
    finally:
        await client.disconnect()


def cmd_untrack(args):
    con = db()
    for ref in args.refs:
        row = find_chat_row(con, ref)
        if row:
            con.execute("UPDATE chats SET tracked=0 WHERE id=?", (row["id"],))
            print(f"· {row['title']} — больше не отслеживается")
        else:
            print(f"✖ {ref}: нет в базе")
    con.commit()


async def cmd_join(args):
    con = db()
    day_ago = (now_utc() - timedelta(days=1)).isoformat()
    done_today = con.execute("SELECT COUNT(*) FROM joins WHERE joined_at>=?", (day_ago,)).fetchone()[0]
    budget = JOINS_PER_DAY - done_today
    if budget <= 0:
        sys.exit(f"За последние сутки уже {done_today} вступлений — лимит {JOINS_PER_DAY}. Продолжим завтра.")
    refs = args.refs[:budget]
    if len(refs) < len(args.refs):
        print(f"Лимит на сутки: вступлю только в {len(refs)} из {len(args.refs)}")
    client = await connect()
    try:
        for i, ref in enumerate(refs):
            if i:
                pause = random.uniform(25, 50)
                print(f"  пауза {pause:.0f} с…")
                await asyncio.sleep(pause)
            try:
                kind, value = parse_ref(ref)
                if kind == "invite":
                    ent = (await client(ImportChatInviteRequest(value))).chats[0]
                else:
                    ent = await resolve_entity(client, ref)
                    await client(JoinChannelRequest(ent))
            except errors.UserAlreadyParticipantError:
                print(f"= {ref}: уже участник")
                continue
            except errors.InviteRequestSentError:
                print(f"… {ref}: заявка отправлена, ждёт одобрения админа")
                continue
            except errors.FloodWaitError as e:
                print(f"Telegram просит подождать {e.seconds} с — останавливаюсь")
                break
            except errors.ChannelsTooMuchError:
                print("Достигнут лимит Telegram на число групп у аккаунта — отпишитесь от лишних")
                break
            except (ValueError, TypeError, errors.RPCError) as e:
                print(f"✖ {ref}: {e}")
                continue
            save_chat(con, ent, found_by="вступление", tracked=True)
            con.execute("INSERT INTO joins VALUES (?,?)", (ent.id, now_utc().isoformat()))
            con.commit()
            print(f"✔ вступил: {ent.title}")
    finally:
        await client.disconnect()


async def fetch_messages(client, con, chat_id, input_entity, min_id, since, max_count):
    added, last_id = 0, min_id
    # wait_time=1: секунда между пачками по 100 сообщений — спокойный темп для больших чатов
    async for msg in client.iter_messages(input_entity, min_id=min_id, limit=max_count, wait_time=1):
        if msg.date < since:
            break
        last_id = max(last_id, msg.id)
        text = (msg.message or "").strip()
        if not text or msg.out:  # свои сообщения не нужны
            continue
        links = kwork_links(msg)
        if links:
            text += "\nОтклик: " + links[0]
        sender = msg.sender
        cur = con.execute(
            "INSERT OR IGNORE INTO messages (chat_id, msg_id, date, sender_id, sender_username, sender_name, text)"
            " VALUES (?,?,?,?,?,?,?)",
            (chat_id, msg.id, msg.date.isoformat(), msg.sender_id,
             getattr(sender, "username", None) if sender else None,
             display_name(sender) if sender else None, text))
        added += cur.rowcount
    return added, last_id


async def parse_chats(con, client, rows, days, max_count, refetch):
    since = now_utc() - timedelta(days=days)
    total = 0
    for i, row in enumerate(rows):
        if i:
            await asyncio.sleep(random.uniform(1.5, 3.5))
        try:
            input_entity = await input_entity_for_row(client, row)
            added, last_id = await fetch_messages(
                client, con, row["id"], input_entity, 0 if refetch else row["last_msg_id"], since, max_count)
        except errors.FloodWaitError as e:
            print(f"Telegram просит подождать {e.seconds} с — сбор остановлен")
            break
        except (ValueError, TypeError, errors.RPCError) as e:
            print(f"✖ {row['title']}: {e.__class__.__name__}: {e}")
            continue
        con.execute("UPDATE chats SET last_msg_id=MAX(last_msg_id, ?), parsed_at=? WHERE id=?",
                    (last_id, now_utc().isoformat(), row["id"]))
        con.commit()
        print(f"  {row['title']}: +{added}")
        total += added
    return total


async def cmd_parse(args):
    con = db()
    client = await connect()
    try:
        if args.refs:
            rows = []
            for ref in args.refs:
                row = find_chat_row(con, ref)
                if row is None:
                    try:
                        ent = await resolve_entity(client, ref)
                    except (ValueError, TypeError, errors.RPCError) as e:
                        print(f"✖ {ref}: {e}")
                        continue
                    save_chat(con, ent, found_by="вручную", tracked=True)
                    con.commit()
                    row = con.execute("SELECT * FROM chats WHERE id=?", (ent.id,)).fetchone()
                rows.append(row)
        else:
            rows = con.execute("SELECT * FROM chats WHERE tracked=1 ORDER BY participants DESC").fetchall()
        if not rows:
            print("Нет отслеживаемых чатов. Найдите их (search) и добавьте (track).")
            return
        print(f"Собираю сообщения за {args.days} дн. из {len(rows)} чатов…")
        total = await parse_chats(con, client, rows, args.days, args.max, args.refetch)
        print(f"Новых сообщений: {total}")
    finally:
        await client.disconnect()


def is_soft_chat(kw, chat_id, username):
    return str(chat_id) in kw["soft_chats"] or (username or "").lower() in kw["soft_chats"]


def sender_blocked(con, sender_id, sender_username, days=14):
    """Автор не годится: бот, либо уже есть заявка от этого человека за последние `days` дней.

    Повторные посты одного человека в разных чатах — один заказчик, а не пять заявок.
    Проверка по автору только для людей (положительный id): у каналов с лентами заказов автор общий.
    """
    if (sender_username or "").lower().endswith("bot"):
        return True
    if not sender_id or sender_id <= 0:
        return False
    since = (now_utc() - timedelta(days=days)).isoformat()
    row = con.execute(
        "SELECT 1 FROM leads l JOIN messages m ON m.chat_id=l.chat_id AND m.msg_id=l.msg_id"
        " WHERE m.sender_id=? AND l.created_at>=? LIMIT 1", (sender_id, since)).fetchone()
    return row is not None


_llm_client = None  # None — ещё не пробовали, False — выключен или не настроен


def llm_check(text):
    """ИИ-проверка кандидата после правил. None — фильтр выключен или сбой (тогда решают правила).

    Иначе ("Y"|"N", заметка). Выключить: LLM_FILTER=0 в .env. Модель: GIGACHAT_MODEL (по умолчанию GigaChat-Pro).
    """
    global _llm_client
    if os.environ.get("LLM_FILTER", "1") == "0" or _llm_client is False:
        return None
    try:
        if _llm_client is None:
            import llm_filter
            _llm_client = llm_filter.Client(os.environ.get("GIGACHAT_MODEL", "GigaChat-Pro"))
            print("ИИ-фильтр включён:", _llm_client.model)
        r = _llm_client.classify(text, mode=json.loads(KEYWORDS.read_text(encoding="utf-8")).get("lead_mode", "dev"))
    except Exception as e:  # нет ключа или модуля: работаем по правилам, не падаем
        print(f"ИИ-фильтр выключен: {e.__class__.__name__}: {e}")
        _llm_client = False
        HEALTH["llm_disabled"] = f"{e.__class__.__name__}: {e}"[:80]
        return None
    if r["v"] is None:
        print(f"ИИ-фильтр: сбой ({r['r']}), заявка принята по правилам")
        HEALTH["llm_fail"] += 1
        return None
    HEALTH["llm_fail"] = 0
    return r["v"], f"ИИ: {r['c']}, {r['r']}", r


# ---------- самопроверка и оповещения о сбоях ----------

HEALTH = {"llm_fail": 0, "llm_disabled": None, "sheet_fail": 0, "pain_sheet_fail": 0, "idle": 0, "flags": set()}
_alert_last = {}


async def alert(client, notify_entity, key, text, cooldown_h=6):
    """Сообщение о проблеме в группу заявок. Повтор того же сообщения не чаще, чем раз в cooldown_h часов."""
    now = now_utc()
    last = _alert_last.get(key)
    if last and now - last < timedelta(hours=cooldown_h):
        return
    _alert_last[key] = now
    HEALTH["flags"].add(key)
    print("ОПОВЕЩЕНИЕ:", text)
    try:
        await client.send_message(notify_entity, "⚠️ " + text, parse_mode=None, link_preview=False)
    except Exception as e:
        print(f"оповещение не отправилось: {e.__class__.__name__}")


async def recovered(client, notify_entity, key, text):
    if key in HEALTH["flags"]:
        HEALTH["flags"].discard(key)
        _alert_last.pop(key, None)
        try:
            await client.send_message(notify_entity, "✅ " + text, parse_mode=None, link_preview=False)
        except Exception:
            pass


async def check_health(client, notify_entity, new_messages, chats):
    """Вызывается после каждого цикла опроса."""
    if HEALTH["llm_disabled"]:
        await alert(client, notify_entity, "llm_off",
                    f"ИИ-фильтр выключен ({HEALTH['llm_disabled']}). Заявки идут только по правилам, мусора будет больше. "
                    "Проверьте ключ GIGACHAT_CREDENTIALS в .env на сервере и перезапустите службу.", 12)
    elif HEALTH["llm_fail"] >= 3:
        await alert(client, notify_entity, "llm_fail",
                    f"ИИ-фильтр не отвечает {HEALTH['llm_fail']} раз подряд (GigaChat недоступен, кончился лимит токенов "
                    "или устарел ключ). Заявки временно идут только по правилам. Если не пройдёт само, проверьте "
                    "ключ и остаток токенов в кабинете Сбера.")
    elif HEALTH["llm_fail"] == 0:
        await recovered(client, notify_entity, "llm_fail", "ИИ-фильтр снова отвечает.")
    if HEALTH["sheet_fail"] >= 3:
        await alert(client, notify_entity, "sheet",
                    "Таблица не принимает заявки уже несколько циклов (Apps Script недоступен, сменился токен или скрипт "
                    "не обновлён: нужна версия из deploy/apps_script_sheet.gs). Заявки остаются в группе и допишутся позже.")
    elif HEALTH["sheet_fail"] == 0:
        await recovered(client, notify_entity, "sheet", "Запись в таблицу снова работает.")
    if HEALTH["pain_sheet_fail"] >= 6:
        await alert(client, notify_entity, "pain_sheet",
                    f"Лист «{PAINS_SHEET}» не принимает строки. Вероятно, в таблице не обновлён Apps Script "
                    "(нужна версия с действием add_sheet, файл deploy/apps_script_sheet.gs) или нет самого листа. "
                    "Боли копятся на сервере и запишутся, когда это исправят.", 24)
    elif HEALTH["pain_sheet_fail"] == 0:
        await recovered(client, notify_entity, "pain_sheet", f"Лист «{PAINS_SHEET}» снова принимает строки.")
    HEALTH["idle"] = HEALTH["idle"] + 1 if new_messages == 0 else 0
    if HEALTH["idle"] >= 4:
        await alert(client, notify_entity, "idle",
                    f"Час без единого нового сообщения из {chats} чатов: чтение, возможно, зависло. "
                    "Если не пройдёт, перезапустите службу: systemctl restart tg-leads.")
    else:
        await recovered(client, notify_entity, "idle", "Новые сообщения снова поступают.")
    free_gb = shutil.disk_usage("/").free / 1e9
    if free_gb < 1.5:
        await alert(client, notify_entity, "disk", f"На сервере осталось {free_gb:.1f} ГБ диска. "
                    "Удалите старые копии в /opt/tg-leads/backups или расширьте диск.", 24)


KWORK_URL_RE = re.compile(r"https://kwork\.ru/projects/(\d+)")
BOARD_URL_RE = re.compile(r"(https://kwork\.ru/projects/\d+|https://www\.fl\.ru/projects/\d+/[\w.-]+\.html"
                          r"|https://freelance\.ru/task/view/\d+)")


def board_duplicate(con, m):
    """Тот же заказ биржи уже приходил (например, из Telegram-ленты Kwork и напрямую с сайта)."""
    bm = BOARD_URL_RE.search(m["text"])
    if not bm:
        return None
    return con.execute("SELECT l.id FROM leads l JOIN messages x ON x.chat_id=l.chat_id AND x.msg_id=l.msg_id"
                       " WHERE x.text LIKE ? AND NOT (l.chat_id=? AND l.msg_id=?) LIMIT 1",
                       (f"%{bm.group(1)}%", m["chat_id"], m["msg_id"])).fetchone()
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0 Safari/537.36")


def _money(value):
    return f"{int(float(value)):,}".replace(",", " ")


def kwork_state(url):
    """Заказ на Kwork ещё открыт? ("active"|"closed"|"deleted", подпись) или None, если проверить не удалось.

    Удалённый заказ Kwork перенаправляет на общий список, у существующего в странице есть блок wantData.
    Для открытого в подписи желаемый и максимальный бюджет и число откликов.
    """
    m = KWORK_URL_RE.search(url)
    if not m:
        return None
    pid = m.group(1)
    req = urllib.request.Request(f"https://kwork.ru/projects/{pid}",
                                 headers={"User-Agent": BROWSER_UA, "Accept-Language": "ru-RU,ru"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            final, page = r.geturl(), r.read().decode("utf-8", "replace")
    except Exception:
        return None
    if f"/projects/{pid}" not in final:
        return "deleted", "заказ убран с биржи"
    w = re.search(r'"wantData":\{"id":%s,"status":"(\w+)"' % pid, page)
    if not w:
        return None
    if w.group(1) != "active":
        return "closed", "заказ закрыт"
    tail = page[w.start():]
    want = re.search(r'"priceLimit":"?([\d.]+)', tail)
    top = re.search(r'"possiblePriceLimit":"?([\d.]+)', tail)
    offers = re.search(r'"kwork_count":"?(\d+)', tail)
    label = "заказ открыт"
    if want and top:
        label = f"желаемый {_money(want.group(1))} ₽, максимум {_money(top.group(1))} ₽"
    if offers:
        label += f" · откликов {offers.group(1)}"
    return "active", label


def judge(text):
    """Последние проверки кандидата после правил: открыт ли заказ Kwork и что скажет ИИ.

    Возвращает (отклонить?, заметка, JSON для таблицы). Ничего не пишет в базу, поэтому безопасно
    вызывается из другого потока.
    """
    info = {}
    km = KWORK_URL_RE.search(text)
    if km:
        state = kwork_state(km.group(0))
        if state:
            if state[0] != "active":
                return True, f"Kwork: {state[1]}", None
            info["k"] = state[1]
    verdict = llm_check(text)
    if verdict:
        v, note, ai = verdict
        if v == "N":
            return True, note, None
        info.update(t=ai.get("t", ""), q=ai.get("q", ""), r=ai.get("r", ""), c=ai.get("c", ""))
    return False, None, (json.dumps(info, ensure_ascii=False) if info else None)


# ---------- запись в Google-таблицу ----------

MSK = timezone(timedelta(hours=3))
SHEET_STATUS_NEW = "Новый"


def env_value(name):
    value = os.environ.get(name, "")
    if not value and (ROOT / ".env").exists():
        for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
            if line.startswith(name + "="):
                value = line.split("=", 1)[1]
    return value.strip().strip("\"'")


def sheet_row(r):
    """Строка таблицы: дата, площадка, автор, ссылка, тип, запрос, статус, комментарий, первое сообщение."""
    info = json.loads(r["llm"]) if r["llm"] else {}
    bm = BOARD_URL_RE.search(r["text"])
    link = bm.group(1) if bm else msg_link(r)
    if not link:
        return None
    date = datetime.fromisoformat(r["date"]).astimezone(MSK).strftime("%d.%m.%Y")
    if bm:
        site = "Kwork" if "kwork.ru" in link else "FL.ru" if "fl.ru" in link else "Freelance.ru"
        author = f"заказчик на {site}"
    else:
        author = " ".join(filter(None, [f"@{r['sender_username']}" if r["sender_username"] else None,
                                        r["sender_name"]])) or "без имени"
    comment = "; ".join(filter(None, [info.get("r"), info.get("k")]))
    query = info.get("q") or " ".join(r["text"].split())[:200]
    return [date, r["chat_title"][:60], author, link, info.get("t") or r["niche"], query,
            SHEET_STATUS_NEW, comment, ""]


DIRECT_SHEET, JOBS_SHEET = "Директ", "Вакансии"


def sheet_post(payload):
    """Отправляет строки в Apps Script. True — записано, иначе False (повторим в следующем цикле)."""
    url, token = env_value("SHEETS_WEBHOOK_URL"), env_value("SHEETS_WEBHOOK_TOKEN")
    if not url or not token:
        return False
    body = json.dumps({"token": token, **payload}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    where = payload.get("sheet", "Фриланс")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            answer = json.loads(resp.read())
    except Exception as e:
        print(f"таблица недоступна ({where}: {e.__class__.__name__}), повторю в следующем цикле")
        HEALTH["sheet_fail"] += 1
        return False
    if not answer.get("ok"):
        print(f"таблица отклонила запрос ({where}): {answer.get('error')}")
        HEALTH["sheet_fail"] += 1
        return False
    HEALTH["sheet_fail"] = 0
    print(f"в лист «{where}» добавлено строк: {answer.get('added')} из {len(payload['rows'])}")
    return True


def sheet_sync(con):
    """Дописывает в таблицу заявки и вакансии, которые ещё туда не попали. Новые окажутся сверху.

    Заявки на Директ идут на лист «Директ», остальные на «Фриланс», вакансии на «Вакансии»."""
    since = (now_utc() - timedelta(days=30)).isoformat()
    rows = con.execute(LEADS_SQL + " WHERE l.status='new' AND l.sheet_at IS NULL AND l.sent_at IS NOT NULL"
                       " AND l.created_at>=? ORDER BY l.id DESC LIMIT 200", (since,)).fetchall()
    batches = {"direct": ([], []), "other": ([], [])}
    for r in rows:
        row = sheet_row(r)
        if row is None:
            con.execute("UPDATE leads SET sheet_at='нет ссылки' WHERE id=?", (r["id"],))
            continue
        batch, ids = batches["direct" if "Директ" in (r["niche"] or "") else "other"]
        batch.append(row)
        ids.append(r["id"])
    con.commit()
    total = 0
    for kind, (batch, ids) in batches.items():
        if not batch:
            continue
        payload = ({"action": "add_sheet", "sheet": DIRECT_SHEET, "rows": batch} if kind == "direct"
                   else {"action": "add", "rows": batch})
        if sheet_post(payload):
            con.executemany("UPDATE leads SET sheet_at=? WHERE id=?", [(now_utc().isoformat(), i) for i in ids])
            con.commit()
            total += len(batch)
    return total + jobs_sync(con)


def jobs_sync(con):
    """Вакансии на лист «Вакансии» (только найм; старые проектные заказы туда не пишем)."""
    rows = con.execute(JOBS_SQL + " WHERE j.sheet_at IS NULL AND j.kind IN ('вакансия','подработка')"
                       " ORDER BY j.id DESC LIMIT 200").fetchall()
    batch, ids = [], []
    for r in rows:
        link = msg_link(r)
        if not link:
            continue
        date = datetime.fromisoformat(r["date"]).astimezone(MSK).strftime("%d.%m.%Y")
        batch.append([date, r["chat_title"][:60], r["title"] or "", link, r["salary"] or "не указана",
                      " · ".join(filter(None, [r["fmt"], r["kind"], r["employer"]])), "Новый", r["why"] or "",
                      " ".join(r["text"].split())[:500]])
        ids.append(r["id"])
    if not batch or not sheet_post({"action": "add_sheet", "sheet": JOBS_SHEET, "rows": batch}):
        return 0
    con.executemany("UPDATE jobs SET sheet_at=? WHERE id=?", [(now_utc().isoformat(), i) for i in ids])
    con.commit()
    return len(batch)


# ---------- боли селлеров по поставкам и остаткам (не заявки, а исследование спроса) ----------

PAINS_SHEET = "Боли селлеров"
PAINS_SCREEN_PER_RUN = 120  # дешёвых проверок за проход (лёгкая модель)
PAINS_PRO_PER_RUN = 25      # полных разборов за проход (Pro): остальное досмотрим в следующем цикле
PAINS_SCREEN_PER_DAY = 1500
PAINS_PRO_PER_DAY = 150     # потолок расхода токенов Pro на боли в сутки
PAINS_SQL = """
SELECT p.*, m.date, m.text, m.sender_username, m.sender_name,
       c.title AS chat_title, c.username AS chat_username, c.peer AS chat_peer
FROM pains p JOIN messages m ON m.chat_id=p.chat_id AND m.msg_id=p.msg_id JOIN chats c ON c.id=p.chat_id
"""


def llm_client():
    """Общий клиент ИИ-фильтра. None — выключен или не настроен."""
    global _llm_client
    if os.environ.get("LLM_FILTER", "1") == "0" or _llm_client is False:
        return None
    if _llm_client is None:
        try:
            import llm_filter
            _llm_client = llm_filter.Client(os.environ.get("GIGACHAT_MODEL", "GigaChat-Pro"))
        except Exception as e:
            print(f"ИИ-фильтр выключен: {e.__class__.__name__}: {e}")
            _llm_client = False
            HEALTH["llm_disabled"] = f"{e.__class__.__name__}: {e}"[:80]
            return None
    return _llm_client


def pain_candidate(text, kw):
    """Дешёвый предфильтр: тема поставок и остатков + признак проблемы или вопроса."""
    t = normalize(text)
    if not (40 <= len(t) <= 1500) or t.count("#") >= kw["max_hashtags"]:
        return False
    if matches(kw["pain_skip"], t):
        return False
    return bool(matches(kw["pain_topic"], t) and matches(kw["pain_signal"], t))


def _daily_used(con, key):
    day = now_utc().strftime("%Y-%m-%d")
    raw = get_setting(con, key) or ""
    d, _, n = raw.partition(":")
    return day, (int(n) if d == day and n.isdigit() else 0)


async def find_pains(con, days=2):
    """Ищет в сообщениях селлеров боли, которые можно автоматизировать. Возвращает число новых записей.

    Три ступени: правила (бесплатно) → лёгкая модель Y/N → Pro пишет суть и черновик. Вызовы ИИ идут в отдельном
    потоке, чтобы не стопорить приём сообщений; база трогается только здесь."""
    kw = load_keywords()
    if not kw["pain_topic"]:
        return 0
    since = (now_utc() - timedelta(days=days)).isoformat()
    rows = con.execute("SELECT m.*, c.title AS chat_title FROM messages m JOIN chats c ON c.id=m.chat_id"
                       " WHERE m.pain_checked=0 AND m.date>=? AND m.chat_id NOT BETWEEN -1003 AND -1001"
                       " ORDER BY m.date DESC", (since,)).fetchall()
    day, screen_used = _daily_used(con, "pain_screen_day")
    _, pro_used = _daily_used(con, "pain_pro_day")
    done, new, screens, pros = [], 0, 0, 0
    for m in rows:
        key = (m["chat_id"], m["msg_id"])
        if (m["sender_username"] or "").lower().endswith("bot") or not kw["pain_chat"].search(m["chat_title"] or "") \
                or not pain_candidate(m["text"], kw):
            done.append(key)
            continue
        client = llm_client()
        if client is None:
            break
        if screens >= PAINS_SCREEN_PER_RUN or screen_used + screens >= PAINS_SCREEN_PER_DAY:
            continue  # кандидат подождёт следующего прохода или следующих суток
        screens += 1
        s1 = await asyncio.to_thread(client.pain_screen, m["text"])
        if s1["v"] == "N":
            done.append(key)
            continue
        if pros >= PAINS_PRO_PER_RUN or pro_used + pros >= PAINS_PRO_PER_DAY:
            continue
        pros += 1
        r = await asyncio.to_thread(client.pain, m["text"])
        if r["v"] is None:
            HEALTH["llm_fail"] += 1
            continue  # сбой ИИ: попробуем позже
        HEALTH["llm_fail"] = 0
        done.append(key)
        if r["v"] == "Y":
            cur = con.execute("INSERT OR IGNORE INTO pains (chat_id, msg_id, kind, pain, hot, draft, created_at)"
                              " VALUES (?,?,?,?,?,?,?)",
                              (m["chat_id"], m["msg_id"], r["k"], r["p"], r["h"], r["m"], now_utc().isoformat()))
            new += cur.rowcount
    con.executemany("UPDATE messages SET pain_checked=1 WHERE chat_id=? AND msg_id=?", done)
    set_setting(con, "pain_screen_day", f"{day}:{screen_used + screens}")
    set_setting(con, "pain_pro_day", f"{day}:{pro_used + pros}")
    con.commit()
    if rows:
        print(f"[{datetime.now():%H:%M}] боли: сообщений {len(rows)}, лёгких проверок {screens}, Pro {pros}, новых {new}")
    return new


def pain_row(r):
    link = msg_link(r)
    if not link:
        return None
    date = datetime.fromisoformat(r["date"]).astimezone(MSK).strftime("%d.%m.%Y")
    author = " ".join(filter(None, [f"@{r['sender_username']}" if r["sender_username"] else None,
                                    r["sender_name"]])) or "без имени"
    return [date, r["chat_title"][:60], author, link, ("🔥 " if r["hot"] else "") + (r["kind"] or ""),
            r["pain"] or "", "Новый", " ".join(r["text"].split())[:300], r["draft"] or ""]


def pains_sync(con):
    """Дописывает новые боли на лист «Боли FBS». Старый скрипт таблицы это действие не знает: тогда ничего не пишется."""
    url, token = env_value("SHEETS_WEBHOOK_URL"), env_value("SHEETS_WEBHOOK_TOKEN")
    if not url or not token:
        return 0
    rows = con.execute(PAINS_SQL + " WHERE p.sheet_at IS NULL ORDER BY p.id DESC LIMIT 200").fetchall()
    batch, ids = [], []
    for r in rows:
        row = pain_row(r)
        if row is None:
            con.execute("UPDATE pains SET sheet_at='нет ссылки' WHERE id=?", (r["id"],))
        else:
            batch.append(row)
            ids.append(r["id"])
    con.commit()
    if not batch:
        return 0
    body = json.dumps({"token": token, "action": "add_sheet", "sheet": PAINS_SHEET, "rows": batch}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            answer = json.loads(resp.read())
    except Exception as e:
        print(f"лист болей недоступен ({e.__class__.__name__}), повторю позже")
        HEALTH["pain_sheet_fail"] = HEALTH.get("pain_sheet_fail", 0) + 1
        return 0
    if not answer.get("ok"):
        print(f"лист болей отклонил запрос: {answer.get('error')}")
        HEALTH["pain_sheet_fail"] = HEALTH.get("pain_sheet_fail", 0) + 1
        return 0
    HEALTH["pain_sheet_fail"] = 0
    marks = now_utc().isoformat()
    con.executemany("UPDATE pains SET sheet_at=? WHERE id=?", [(marks, i) for i in ids])
    con.commit()
    print(f"на лист «{PAINS_SHEET}» добавлено строк: {answer.get('added', len(batch))} из {len(batch)}")
    return answer.get("added", len(batch))


# ---------- вакансии под профиль пользователя ----------

JOBS_PER_RUN = 25   # вызовов ИИ за проход
JOBS_PER_DAY = 80   # потолок расхода токенов Pro в сутки
JOBS_SQL = """
SELECT j.*, m.date, m.text, m.sender_username, m.sender_name,
       c.title AS chat_title, c.username AS chat_username, c.peer AS chat_peer
FROM jobs j JOIN messages m ON m.chat_id=j.chat_id AND m.msg_id=j.msg_id JOIN chats c ON c.id=j.chat_id
"""


def job_candidate(text, kw):
    t = normalize(text)
    if not (80 <= len(t) <= 2500):
        return False
    if matches(kw["job_skip"], t):
        return False
    return bool(matches(kw["job_tech"], t) and matches(kw["job_offer"], t))


def format_job(r):
    head = {"вакансия": "💼 Вакансия", "подработка": "💼 Подработка", "проект": "💼 Проектная работа"}.get(r["kind"], "💼 Работа")
    meta = " · ".join(filter(None, [r["salary"] or "зарплата не указана", r["fmt"], r["employer"]]))
    parts = [f"{head}: {r['title'] or 'без названия'}", meta]
    if r["why"]:
        parts.append(r["why"])
    link = msg_link(r)
    if link:
        parts.append(link)
    parts += ["", " ".join(r["text"].split())[:700]]
    return "\n".join(parts)


async def find_jobs(client, con, notify_entity, days=7):
    """Ищет в сообщениях чатов вакансии под профиль пользователя и присылает их в группу. Возвращает число найденных."""
    kw = load_keywords()
    if not kw["job_tech"]:
        return 0
    since = (now_utc() - timedelta(days=days)).isoformat()
    rows = con.execute("SELECT * FROM messages WHERE job_checked=0 AND date>=? AND chat_id NOT BETWEEN -1003 AND -1001"
                       " ORDER BY date DESC", (since,)).fetchall()
    day, used = _daily_used(con, "job_pro_day")
    done, new, calls, seen = [], 0, 0, set()
    for m in rows:
        key = (m["chat_id"], m["msg_id"])
        if (m["sender_username"] or "").lower().endswith("bot") or not job_candidate(m["text"], kw):
            done.append(key)
            continue
        h = text_hash(m["text"])
        if h in seen or con.execute("SELECT 1 FROM jobs WHERE text_hash=?", (h,)).fetchone():
            done.append(key)  # то же объявление уже было (репост в другом канале)
            continue
        seen.add(h)
        ai = llm_client()
        if ai is None:
            break
        if calls >= JOBS_PER_RUN or used + calls >= JOBS_PER_DAY:
            continue
        calls += 1
        r = await asyncio.to_thread(ai.job, m["text"])
        if r["v"] is None:
            HEALTH["llm_fail"] += 1
            continue
        HEALTH["llm_fail"] = 0
        done.append(key)
        if r["v"] == "Y" and r.get("k") in ("вакансия", "подработка"):  # только найм, проектные заказы не нужны
            cur = con.execute("INSERT OR IGNORE INTO jobs (chat_id, msg_id, text_hash, title, salary, fmt, kind, employer, why,"
                              " created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                              (m["chat_id"], m["msg_id"], h, r.get("t"), r.get("s"), r.get("f"), r.get("k"), r.get("e"),
                               r.get("w"), now_utc().isoformat()))
            new += cur.rowcount
    con.executemany("UPDATE messages SET job_checked=1 WHERE chat_id=? AND msg_id=?", done)
    set_setting(con, "job_pro_day", f"{day}:{used + calls}")
    con.commit()
    if rows:
        print(f"[{datetime.now():%H:%M}] вакансии: сообщений {len(rows)}, вызовов ИИ {calls}, найдено {new}")
    for r in con.execute(JOBS_SQL + " WHERE j.sent_at IS NULL ORDER BY j.id").fetchall():
        try:
            await client.send_message(notify_entity, format_job(r), parse_mode=None, link_preview=False)
        except Exception as e:
            print(f"вакансия не отправилась: {e.__class__.__name__}")
            break
        con.execute("UPDATE jobs SET sent_at=? WHERE id=?", (now_utc().isoformat(), r["id"]))
        con.commit()
        await asyncio.sleep(random.uniform(1.5, 3))
    jobs_sync(con)
    return new


def find_leads(con, days, recheck=False):
    kw = load_keywords()
    soft_by_chat = {r["id"]: is_soft_chat(kw, r["id"], r["username"])
                    for r in con.execute("SELECT id, username FROM chats")}
    allowed_by_chat = {r["id"]: kw["whitelist"].get((r["username"] or "").lower())
                       for r in con.execute("SELECT id, username FROM chats")}
    if recheck:
        con.execute("UPDATE messages SET checked=0")
    since = (now_utc() - timedelta(days=days)).isoformat()
    rows = con.execute("SELECT * FROM messages WHERE checked=0 AND date>=? ORDER BY date", (since,)).fetchall()
    new_ids, dups = [], 0
    for m in rows:
        allowed = allowed_by_chat.get(m["chat_id"])
        if allowed and (m["sender_username"] or "").lower() not in allowed:
            result = None  # в шумных чатах берём только проверенных авторов, как и в watch
        else:
            result = evaluate(m["text"], kw, soft=soft_by_chat.get(m["chat_id"], False),
                              board=m["chat_id"] in boards.BOARD_CHATS)
            # запрос к базе тяжёлый, поэтому только для прошедших правила: бот или повтор того же автора
            if result and sender_blocked(con, m["sender_id"], m["sender_username"]):
                result = None
        if result:
            niche, words, score = result
            h = text_hash(m["text"])
            same = con.execute("SELECT id FROM leads WHERE text_hash=? AND NOT (chat_id=? AND msg_id=?)",
                               (h, m["chat_id"], m["msg_id"])).fetchone() or board_duplicate(con, m)
            if same:
                con.execute("UPDATE leads SET dup_count=dup_count+1 WHERE id=?", (same["id"],))
                dups += 1
            else:
                rejected, note, info = judge(m["text"])
                cur = con.execute(
                    "INSERT OR IGNORE INTO leads (chat_id, msg_id, niche, matched, score, text_hash, created_at,"
                    " status, note, llm) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (m["chat_id"], m["msg_id"], niche, ", ".join(words), score, h, now_utc().isoformat(),
                     "no" if rejected else "new", note, info))
                if cur.rowcount and not rejected:
                    new_ids.append(cur.lastrowid)
        con.execute("UPDATE messages SET checked=1 WHERE chat_id=? AND msg_id=?", (m["chat_id"], m["msg_id"]))
    con.commit()
    print(f"Проверено сообщений: {len(rows)}, новых кандидатов: {len(new_ids)}, повторов: {dups}")
    return new_ids


def unsent_leads(con):
    """Заявки, которые не ушли в группу (например, Telegram притормозил отправку): досылаем в следующем цикле."""
    since = (now_utc() - timedelta(days=2)).isoformat()
    return [r["id"] for r in con.execute(
        "SELECT id FROM leads WHERE status='new' AND sent_at IS NULL AND created_at>=? ORDER BY id", (since,))]


LEADS_SQL = """
SELECT l.*, m.date, m.text, m.sender_username, m.sender_name,
       c.title AS chat_title, c.username AS chat_username, c.peer AS chat_peer
FROM leads l JOIN messages m ON m.chat_id=l.chat_id AND m.msg_id=l.msg_id JOIN chats c ON c.id=l.chat_id
"""


def msg_link(r):
    if r["chat_username"]:
        return f"https://t.me/{r['chat_username']}/{r['msg_id']}"
    if r["chat_peer"] == "chat":
        return ""  # у старых маленьких групп ссылок на сообщения не бывает
    return f"https://t.me/c/{r['chat_id']}/{r['msg_id']}"  # откроется у участников закрытого чата


def print_leads(rows, full=False):
    for r in rows:
        text = r["text"] if full else (r["text"][:400] + ("…" if len(r["text"]) > 400 else ""))
        author = " ".join(filter(None, [r["sender_name"], f"@{r['sender_username']}" if r["sender_username"] else None]))
        dup = f" · повторов {r['dup_count'] - 1}" if r["dup_count"] > 1 else ""
        print(f"\n#{r['id']} [{STATUS_RU.get(r['status'], r['status'])}] {r['niche']} · {fmt_local(r['date'])}"
              f" · {r['chat_title']}{dup}")
        link = msg_link(r)
        print(f"Автор: {author or '—'}" + (f" · {link}" if link else ""))
        if r["note"]:
            print(f"Заметка: {r['note']}")
        print(f"Слова: {r['matched']}")
        print(text.replace("\n", " ⏎ ") if not full else text)


def cmd_leads(args):
    con = db()
    new_ids = find_leads(con, args.days, args.recheck)
    if new_ids:
        marks = ",".join("?" * len(new_ids))
        print_leads(con.execute(LEADS_SQL + f" WHERE l.id IN ({marks}) ORDER BY m.date DESC", new_ids), args.full)


def cmd_show(args):
    con = db()
    statuses = args.status.split(",")
    sql = LEADS_SQL + f" WHERE l.status IN ({','.join('?' * len(statuses))})"
    params = list(statuses)
    if args.ids:
        sql += f" AND l.id IN ({','.join('?' * len(args.ids))})"
        params += args.ids
    rows = con.execute(sql + " ORDER BY m.date DESC LIMIT ?", params + [args.limit]).fetchall()
    print_leads(rows, args.full)
    print(f"\nПоказано: {len(rows)}")


def cmd_mark(args):
    con = db()
    for lead_id in args.ids:
        if args.note is not None:
            cur = con.execute("UPDATE leads SET status=?, note=? WHERE id=?", (args.status, args.note, lead_id))
        else:
            cur = con.execute("UPDATE leads SET status=? WHERE id=?", (args.status, lead_id))
        print(f"#{lead_id} → {STATUS_RU[args.status]}" if cur.rowcount else f"#{lead_id}: не найден")
    con.commit()


def cmd_export(args):
    from openpyxl import Workbook
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    from openpyxl.styles import Alignment, Font, PatternFill

    con = db()
    statuses = args.status.split(",")
    rows = con.execute(
        LEADS_SQL + f" WHERE l.status IN ({','.join('?' * len(statuses))})"
        " ORDER BY CASE l.status WHEN 'ok' THEN 0 WHEN 'new' THEN 1 WHEN 'done' THEN 2 ELSE 3 END, m.date DESC",
        statuses).fetchall()

    wb = Workbook()
    ws = wb.active
    ws.title = "Лиды"
    headers = [("ID", 6), ("Статус", 10), ("Ниша", 22), ("Дата", 16), ("Чат", 28), ("Сообщение", 16),
               ("Автор", 22), ("Написать автору", 18), ("Контакты из текста", 26), ("Текст", 80),
               ("Совпадения", 26), ("Повторов", 9), ("Заметка", 24)]
    ws.append([h for h, _ in headers])
    for col, (_, width) in enumerate(headers, start=1):
        ws.column_dimensions[ws.cell(1, col).column_letter].width = width
        ws.cell(1, col).font = Font(bold=True, color="FFFFFF")
        ws.cell(1, col).fill = PatternFill("solid", fgColor="2F5597")

    for r in rows:
        author_link = f"https://t.me/{r['sender_username']}" if r["sender_username"] else ""
        values = [r["id"], STATUS_RU.get(r["status"], r["status"]), r["niche"], fmt_local(r["date"]),
                  r["chat_title"], msg_link(r), r["sender_name"] or "", author_link,
                  extract_contacts(r["text"]), r["text"], r["matched"], r["dup_count"] - 1, r["note"] or ""]
        ws.append([ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v for v in values])
        row = ws.max_row
        for col in (6, 8):
            cell = ws.cell(row, col)
            if cell.value:
                cell.hyperlink = cell.value
                cell.value = "открыть"
                cell.font = Font(color="0563C1", underline="single")
        for cell in ws[row]:
            if isinstance(cell.value, str) and cell.value.startswith("="):
                cell.data_type = "s"  # не превращать текст сообщения в формулу
            cell.alignment = Alignment(vertical="top", wrap_text=cell.column in (10, 11, 9, 13))
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    out = Path(args.out) if args.out else DATA / f"leads_{datetime.now():%Y-%m-%d}.xlsx"
    try:
        wb.save(out)
    except PermissionError:
        sys.exit(f"Не могу записать {out} — файл открыт в Excel? Закройте его и повторите.")
    print(f"Сохранено {len(rows)} лидов: {out}")


LEAD_MARK = re.compile(r"(?m)^#(\d+) · ")
BATCH_SEP = "\n\n— — —\n\n"


async def cmd_cleanup(args):
    """Убирает из группы уведомлений заявки, которые больше не в статусе «лид»."""
    con = db()
    notify_id = get_setting(con, "notify_chat")
    if not notify_id:
        sys.exit("Группа уведомлений не настроена")
    client = await connect()
    to_delete, edited, kept = [], 0, 0
    try:
        entity = await client.get_input_entity(PeerChannel(int(notify_id)))
        async for msg in client.iter_messages(entity, limit=args.limit):
            text = msg.message or ""
            ids = [int(i) for i in LEAD_MARK.findall(text)]
            if not ids:
                continue
            statuses = {r["id"]: r["status"] for r in con.execute(
                f"SELECT id, status FROM leads WHERE id IN ({','.join('?' * len(ids))})", ids)}
            good = [i for i in ids if statuses.get(i) == "ok"]
            con.executemany("UPDATE leads SET sent_msg_id=? WHERE id=?", [(msg.id, i) for i in good])
            # снятые заявки больше не в группе: если вернутся в статус «лид», их надо отправить заново
            con.executemany("UPDATE leads SET sent_at=NULL, sent_msg_id=NULL WHERE id=?",
                            [(i,) for i in ids if i not in good])
            if not good:
                to_delete.append(msg.id)
            elif len(good) < len(ids):
                blocks = [b for b in text.split(BATCH_SEP) if int(LEAD_MARK.search(b).group(1)) in good]
                if not args.dry_run:
                    for attempt in range(3):
                        try:
                            await client.edit_message(entity, msg.id, BATCH_SEP.join(blocks),
                                                      parse_mode=None, link_preview=False)
                            break
                        except errors.FloodWaitError as e:
                            print(f"  Telegram просит подождать {e.seconds} с при правке — жду")
                            await asyncio.sleep(e.seconds + 3)
                    await asyncio.sleep(2)
                edited += 1
                kept += len(good)
            else:
                kept += len(good)
        con.commit()
        if to_delete and not args.dry_run:
            for i in range(0, len(to_delete), 90):
                await client.delete_messages(entity, to_delete[i:i + 90])
                await asyncio.sleep(1)
    finally:
        await client.disconnect()
    action = "будет удалено" if args.dry_run else "удалено"
    print(f"{action} сообщений: {len(to_delete)}, подчищено смешанных: {edited}, оставлено заявок: {kept}")


async def cmd_run(args):
    con = db()
    rows = con.execute("SELECT * FROM chats WHERE tracked=1 ORDER BY participants DESC").fetchall()
    if not rows:
        sys.exit("Нет отслеживаемых чатов. Найдите их (search) и добавьте (track).")
    client = await connect()
    try:
        print(f"Собираю сообщения за {args.days} дн. из {len(rows)} чатов…")
        total = await parse_chats(con, client, rows, args.days, args.max, False)
        print(f"Новых сообщений: {total}")
    finally:
        await client.disconnect()
    new_ids = find_leads(con, args.days)
    if new_ids:
        marks = ",".join("?" * len(new_ids))
        print_leads(con.execute(LEADS_SQL + f" WHERE l.id IN ({marks}) ORDER BY m.date DESC", new_ids))


def get_setting(con, key):
    row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def set_setting(con, key, value):
    con.execute("INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)))
    con.commit()


async def cmd_notify_setup(args):
    """Создаёт закрытую группу для уведомлений о новых заявках (или подключает существующую)."""
    con = db()
    client = await connect()
    try:
        if args.invite:
            kind, value = parse_ref(args.invite)
            if kind != "invite":
                sys.exit("Нужна ссылка-приглашение вида https://t.me/+xxxx")
            entity = (await client(ImportChatInviteRequest(value))).chats[0]
            print(f"Вступил в группу «{entity.title}»")
        elif args.chat:
            entity = await resolve_entity(client, args.chat)
        else:
            result = await client(CreateChannelRequest(
                title=args.title, about="Уведомления о новых заявках из чатов Telegram", megagroup=True))
            entity = result.chats[0]
            print(f"Создана закрытая группа «{entity.title}» — она видна только вам")
        set_setting(con, "notify_chat", entity.id)
        await client.send_message(entity, "Сюда буду присылать новые заявки. Проверка связи.", parse_mode=None)
        print(f"Уведомления настроены: {entity.title} (id {entity.id})")
    finally:
        await client.disconnect()


def format_lead(r, limit=900):
    author = " ".join(filter(None, [r["sender_name"], f"@{r['sender_username']}" if r["sender_username"] else None]))
    link = msg_link(r)
    parts = [f"#{r['id']} · {r['niche']}", r["chat_title"], f"Автор: {author or 'без имени'}"]
    if link:
        parts.append(link)
    try:
        offers = (json.loads(r["llm"]) if r["llm"] else {}).get("k")
    except ValueError:
        offers = None
    if offers:  # данные Kwork на момент находки: бюджет и сколько человек уже откликнулось
        parts.append(f"Kwork: {offers}")
    parts += [f"Слова: {r['matched']}", "", r["text"][:limit]]
    return "\n".join(parts)


_sending = set()  # заявки, которые прямо сейчас отправляются (циклы Telegram и бирж работают параллельно)


async def send_lead(client, notify_entity, con, lead_id):
    if lead_id in _sending:
        return False
    _sending.add(lead_id)
    try:
        row = con.execute(LEADS_SQL + " WHERE l.id=?", (lead_id,)).fetchone()
        if not row or row["sent_at"]:
            return False
        sent = await client.send_message(notify_entity, format_lead(row), parse_mode=None, link_preview=False)
        con.execute("UPDATE leads SET sent_at=?, sent_msg_id=? WHERE id=?",
                    (now_utc().isoformat(), sent.id, lead_id))
        con.commit()
        return True
    finally:
        _sending.discard(lead_id)


async def watch_backfill(client, con, notify_entity, hours):
    """Опрос чатов: ловит пропущенное и работает для чатов, где аккаунт не состоит (живых обновлений там нет).

    Первый проход сразу при старте, дальше раз в `hours` часов (можно дробное: 0.25 = 15 минут).
    """
    while True:
        rows = con.execute("SELECT * FROM chats WHERE tracked=1 ORDER BY participants DESC").fetchall()
        print(f"\n[{datetime.now():%H:%M}] опрос {len(rows)} чатов…")
        try:
            fresh = await parse_chats(con, client, rows, days=2, max_count=800, refetch=False)
            for lead_id in dict.fromkeys(find_leads(con, days=2) + unsent_leads(con)):
                await send_lead(client, notify_entity, con, lead_id)
                await asyncio.sleep(random.uniform(1.5, 3))
            sheet_sync(con)  # заодно подбирает то, что не дошло до таблицы раньше
            await find_pains(con)
            pains_sync(con)
            await find_jobs(client, con, notify_entity)
            await check_health(client, notify_entity, fresh, len(rows))
        except errors.FloodWaitError as e:
            print(f"Telegram просит подождать {e.seconds} с — пропускаю цикл")
            if e.seconds > 600:
                await alert(client, notify_entity, "flood",
                            f"Telegram просит подождать {e.seconds // 60} мин (ограничение частоты запросов). Бот сам "
                            "пропускает циклы. Если такое повторяется каждый день, уменьшите число чатов на пробе.", 12)
            await asyncio.sleep(min(e.seconds, 900))
        except Exception as e:  # сетевой сбой не должен убивать опрос
            print(f"сбой цикла опроса: {e.__class__.__name__}: {e}")
        await asyncio.sleep(hours * 3600)


async def watch_boards(client, con, notify_entity, minutes):
    """Раз в `minutes` минут читает биржи (Kwork, FL.ru) и пускает заказы через тот же конвейер."""
    while True:
        try:
            items = await asyncio.to_thread(boards.fetch_all)  # только сеть, без доступа к базе
            new = boards.store(con, items)
            leads = list(dict.fromkeys(find_leads(con, days=2) + unsent_leads(con)))
            for lead_id in leads:
                await send_lead(client, notify_entity, con, lead_id)
                await asyncio.sleep(random.uniform(1.5, 3))
            sheet_sync(con)
            print(f"[{datetime.now():%H:%M}] биржи: получено {len(items)}, новых {new}, заявок {len(leads)}")
            HEALTH["boards_fail"] = 0 if items else HEALTH.get("boards_fail", 0) + 1
            if HEALTH["boards_fail"] >= 8:  # цикл раз в 3 минуты: около 25 минут подряд
                await alert(client, notify_entity, "boards",
                            "Биржи (Kwork, FL.ru) не читаются уже около получаса: сайт недоступен или заблокировал IP сервера. "
                            "Заявки из Telegram-чатов продолжают приходить. Подождите: обычно проходит само.", 12)
            else:
                await recovered(client, notify_entity, "boards", "Биржи снова читаются.")
        except errors.FloodWaitError as e:
            print(f"Telegram просит подождать {e.seconds} с — пропускаю цикл бирж")
            await asyncio.sleep(min(e.seconds, 900))
        except Exception as e:  # сбой одной биржи не должен останавливать остальное
            print(f"сбой чтения бирж: {e.__class__.__name__}: {e}")
        await asyncio.sleep(minutes * 60)


# ---------- автопоиск чатов с пробным периодом ----------

TRIAL_DAYS = 7           # новый чат читаем неделю
KEEP_DAYS = 30           # оставленный чат проверяется заново раз в месяц
MAX_TRIALS = 60          # одновременно на пробе: цикл опроса не должен растягиваться (около 8 минут на 100 чатов)
NEW_TRIALS_PER_DAY = 25
DAILY_QUERIES = 40       # запросов поиска в сутки, по кругу из большого списка
MINE_PER_DAY = 25        # ссылок на другие чаты, проверяемых за сутки
MEMBERS_MIN, MEMBERS_MAX = 300, 150000
TG_LINK_RE = re.compile(r"(?:https?://)?(?:t|telegram)\.me/([A-Za-z][A-Za-z0-9_]{4,31})(?![A-Za-z0-9_])")
NOT_CHAT_NAMES = {"joinchat", "addstickers", "addemoji", "share", "proxy", "socks", "login", "setlanguage", "boost"}


def review_trials(con):
    """Подводит итоги проб: чат с хотя бы одной заявкой за период остаётся, остальные отключаются."""
    now = now_utc()
    kept, dropped = [], []
    due = con.execute("SELECT * FROM chats WHERE tracked=1 AND trial_until IS NOT NULL AND trial_until<=?",
                      (now.isoformat(),)).fetchall()
    for c in due:
        period = TRIAL_DAYS if c["trial_state"] == "trial" else KEEP_DAYS
        since = (datetime.fromisoformat(c["trial_until"]) - timedelta(days=period)).isoformat()
        # польза чата: заявки, боли селлеров и вакансии (иначе чаты вакансий и селлеров отключались бы)
        n = con.execute("SELECT (SELECT COUNT(*) FROM leads WHERE chat_id=:c AND status IN ('new','ok','done') AND created_at>=:s)"
                        " + (SELECT COUNT(*) FROM pains WHERE chat_id=:c AND created_at>=:s)"
                        " + (SELECT COUNT(*) FROM jobs WHERE chat_id=:c AND created_at>=:s)",
                        {"c": c["id"], "s": since}).fetchone()[0]
        if n:
            con.execute("UPDATE chats SET trial_state='kept', trial_until=? WHERE id=?",
                        ((now + timedelta(days=KEEP_DAYS)).isoformat(), c["id"]))
            kept.append((c, n))
        else:
            con.execute("UPDATE chats SET tracked=0, trial_state='dropped', trial_until=NULL WHERE id=?", (c["id"],))
            dropped.append(c)
    con.commit()
    return kept, dropped


def chat_allowed(ent):
    if chat_kind(ent) is None or not getattr(ent, "username", None):
        return False
    if BLOCKED_TITLE_RE.search(ent.title or ""):
        return False  # чаты, которые не подключаем (см. BLOCKED_TITLE_RE)
    if getattr(ent, "scam", False) or getattr(ent, "fake", False):
        return False
    return MEMBERS_MIN <= (getattr(ent, "participants_count", None) or 0) <= MEMBERS_MAX


async def discover(client, con):
    """Поиск Telegram по очередной порции запросов. Возвращает число новых чатов."""
    kw = load_keywords()
    queries = list(dict.fromkeys(kw["search_queries"] + kw["discovery_queries"]))
    saved = get_setting(con, "search_cursor")
    # первый круг начинаем с нового словаря: прежние запросы уже гонялись вручную
    cursor = (len(kw["search_queries"]) if saved is None else int(saved)) % len(queries)
    batch = [queries[(cursor + i) % len(queries)] for i in range(min(DAILY_QUERIES, len(queries)))]
    set_setting(con, "search_cursor", (cursor + len(batch)) % len(queries))
    new = 0
    for i, q in enumerate(batch):
        if i:
            await asyncio.sleep(random.uniform(3, 6))
        try:
            res = await client(SearchRequest(q=q, limit=50))
        except errors.FloodWaitError as e:
            print(f"поиск остановлен, Telegram просит подождать {e.seconds} с")
            break
        for ent in res.chats:
            if chat_allowed(ent) and save_chat(con, ent, found_by=q):
                new += 1
        con.commit()
    print(f"поиск: запросов {len(batch)}, новых чатов {new}")
    return new


async def mine_links(client, con, limit):
    """Ссылки на другие чаты из сообщений отслеживаемых чатов: люди сами делятся хорошими площадками."""
    since = (now_utc() - timedelta(days=7)).isoformat()
    tried = set(json.loads(get_setting(con, "mined_tried") or "[]"))
    known = {r[0].lower() for r in con.execute("SELECT username FROM chats WHERE username IS NOT NULL")}
    counts = Counter()
    for (text,) in con.execute("SELECT m.text FROM messages m JOIN chats c ON c.id=m.chat_id"
                               " WHERE c.tracked=1 AND m.date>=?", (since,)):
        for name in TG_LINK_RE.findall(text):
            n = name.lower()
            if n.endswith("bot") or n in known or n in tried or n in NOT_CHAT_NAMES:
                continue
            counts[n] += 1
    new = 0
    for name, _ in counts.most_common(limit):
        tried.add(name)
        await asyncio.sleep(random.uniform(3, 6))
        try:
            ent = await client.get_entity(name)
            if chat_kind(ent) is None:
                continue
            if isinstance(ent, Channel):
                ent.participants_count = (await client(GetFullChannelRequest(ent))).full_chat.participants_count
        except errors.FloodWaitError as e:
            print(f"проверка ссылок остановлена, Telegram просит подождать {e.seconds} с")
            break
        except (ValueError, TypeError, errors.RPCError):
            continue
        if chat_allowed(ent) and save_chat(con, ent, found_by="ссылка из чата"):
            new += 1
        con.commit()
    set_setting(con, "mined_tried", json.dumps(sorted(tried)[-3000:]))
    print(f"ссылки из чатов: новых чатов {new}")
    return new


def start_trials(con):
    """Ставит на пробу лучших кандидатов, добавленных автопоиском. Предпочтение чатам среднего размера."""
    since = get_setting(con, "autopilot_since")
    active = con.execute("SELECT COUNT(*) FROM chats WHERE tracked=1 AND trial_state='trial'").fetchone()[0]
    slots = max(0, min(NEW_TRIALS_PER_DAY, MAX_TRIALS - active))
    rows = con.execute("SELECT * FROM chats WHERE tracked=0 AND trial_state IS NULL AND username IS NOT NULL"
                       " AND added_at>=? AND participants BETWEEN ? AND ?", (since, MEMBERS_MIN, MEMBERS_MAX)).fetchall()
    # огромные чаты почти всегда шум, совсем маленькие молчат: целимся примерно в 8 тысяч участников
    rows.sort(key=lambda r: abs(math.log(max(r["participants"], 1)) - math.log(8000)))
    until = (now_utc() + timedelta(days=TRIAL_DAYS)).isoformat()
    chosen = rows[:slots]
    for r in chosen:
        con.execute("UPDATE chats SET tracked=1, trial_state='trial', trial_until=? WHERE id=?", (until, r["id"]))
    con.commit()
    return chosen, len(rows) - len(chosen)


async def run_discovery(client, con, notify_entity):
    if not get_setting(con, "autopilot_since"):
        set_setting(con, "autopilot_since", now_utc().isoformat())
    kept, dropped = review_trials(con)
    found = await discover(client, con)
    found += await mine_links(client, con, MINE_PER_DAY)
    chosen, queue = start_trials(con)
    lines = [f"Автопоиск чатов. Найдено новых: {found}, на пробу ({TRIAL_DAYS} дн.) взято: {len(chosen)}, в очереди: {queue}."]
    if kept:
        lines += ["", "Остаются после пробы:"] + [f"✔ {c['title'][:40]} · заявок {n}" for c, n in kept]
    if dropped:
        lines += ["", "Отключены, заявок не дали:"] + [f"✖ {c['title'][:40]}" for c in dropped[:15]]
    if chosen:
        lines += ["", "Новые на пробе:"] + [f"· {r['title'][:40]} · {r['participants']}" for r in chosen]
    await client.send_message(notify_entity, "\n".join(lines), parse_mode=None, link_preview=False)


async def watch_search(client, con, notify_entity, hours):
    """Автопоиск раз в `hours` часов; первый запуск через 10 минут после старта, если пора."""
    while True:
        last = get_setting(con, "last_discovery")
        wait = 600
        if last:
            wait = max(wait, hours * 3600 - (now_utc() - datetime.fromisoformat(last)).total_seconds())
        await asyncio.sleep(wait)
        print(f"\n[{datetime.now():%H:%M}] автопоиск чатов…")
        try:
            await run_discovery(client, con, notify_entity)
        except errors.FloodWaitError as e:
            print(f"автопоиск: Telegram просит подождать {e.seconds} с")
        except Exception as e:  # сбой не должен останавливать слежение
            print(f"сбой автопоиска: {e.__class__.__name__}: {e}")
        set_setting(con, "last_discovery", now_utc().isoformat())


# ---------- поиск по тексту сообщений во всех публичных группах ----------

GLOBAL_EVERY_MIN = 30    # цикл раз в полчаса
GLOBAL_PER_CYCLE = 8     # запросов за цикл (около 380 в сутки): Telegram режет частые поиски, поэтому скромно
GLOBAL_LIMIT = 30        # сообщений на запрос
GLOBAL_WINDOW_H = 6      # смотрим сообщения за последние часы; повторы отсекает ключ (chat_id, msg_id)


async def global_search(client, con, notify_entity):
    """Ищет заявки по фразам во ВСЕХ публичных группах, в том числе там, где мы не следим. Возвращает число новых сообщений."""
    queries = load_keywords()["global_queries"]
    if not queries:
        return 0
    cursor = int(get_setting(con, "global_cursor") or 0) % len(queries)
    batch = [queries[(cursor + i) % len(queries)] for i in range(min(GLOBAL_PER_CYCLE, len(queries)))]
    set_setting(con, "global_cursor", (cursor + len(batch)) % len(queries))
    notify_id = int(get_setting(con, "notify_chat") or 0)
    since = now_utc() - timedelta(hours=GLOBAL_WINDOW_H)
    seen = new = 0
    for i, q in enumerate(batch):
        if i:
            await asyncio.sleep(random.uniform(4, 8))
        try:
            res = await client(SearchGlobalRequest(
                q=q, filter=InputMessagesFilterEmpty(), min_date=since, max_date=None, offset_rate=0,
                offset_peer=InputPeerEmpty(), offset_id=0, limit=GLOBAL_LIMIT))
        except errors.FloodWaitError as e:
            print(f"глобальный поиск остановлен, Telegram просит подождать {e.seconds} с")
            await asyncio.sleep(min(e.seconds, 900))
            break
        chats = {c.id: c for c in res.chats}
        users = {u.id: u for u in res.users}
        skipped = Counter()
        raw_msgs = list(res.messages)
        for m in raw_msgs:
            if not isinstance(m, Message) or not (m.message or "").strip() or m.out:
                skipped["пустое"] += 1
                continue
            peer = m.peer_id
            cid = peer.channel_id if isinstance(peer, PeerChannel) else peer.chat_id if isinstance(peer, PeerChat) else None
            ent = chats.get(cid)
            if ent is None or cid == notify_id or chat_kind(ent) is None:
                skipped["не группа/наша"] += 1
                continue  # личные переписки, наша группа заявок
            if getattr(ent, "scam", False) or getattr(ent, "fake", False) \
                    or BLOCKED_TITLE_RE.search(ent.title or ""):
                skipped["украинский/скам"] += 1
                continue
            if (getattr(ent, "participants_count", None) or 0) > MEMBERS_MAX:
                skipped["огромный чат"] += 1
                continue
            seen += 1
            sender = users.get(m.from_id.user_id) if isinstance(m.from_id, PeerUser) else None
            text = m.message.strip()
            links = kwork_links(m)
            if links:
                text += "\nОтклик: " + links[0]
            save_chat(con, ent, found_by="поиск по сообщениям")
            cur = con.execute(
                "INSERT OR IGNORE INTO messages (chat_id, msg_id, date, sender_id, sender_username, sender_name, text)"
                " VALUES (?,?,?,?,?,?,?)",
                (ent.id, m.id, m.date.isoformat(), sender.id if sender else None,
                 (getattr(sender, "username", None) or "").lower() or None, display_name(sender) if sender else None, text))
            new += cur.rowcount
        con.commit()
        print(f"  «{q}»: {type(res).__name__}, сообщений {len(raw_msgs)}, всего у Telegram {getattr(res, 'count', '?')},"
              f" отброшено {dict(skipped) or 0}")
    print(f"[{datetime.now():%H:%M}] поиск по сообщениям: запросов {len(batch)}, найдено {seen}, новых {new}")
    return new


async def watch_global(client, con, notify_entity):
    while True:
        last = get_setting(con, "last_global")
        wait = 120  # после перезапуска не стреляем сразу: частые рестарты не должны бить по лимитам поиска
        if last:
            wait = max(wait, GLOBAL_EVERY_MIN * 60 - (now_utc() - datetime.fromisoformat(last)).total_seconds())
        await asyncio.sleep(wait)
        try:
            if await global_search(client, con, notify_entity):
                for lead_id in dict.fromkeys(find_leads(con, days=2) + unsent_leads(con)):
                    await send_lead(client, notify_entity, con, lead_id)
                    await asyncio.sleep(random.uniform(1.5, 3))
                sheet_sync(con)
                await find_pains(con)
                pains_sync(con)
                await find_jobs(client, con, notify_entity)
        except errors.FloodWaitError as e:
            print(f"глобальный поиск: Telegram просит подождать {e.seconds} с")
            await asyncio.sleep(min(e.seconds, 900))
        except Exception as e:
            print(f"сбой поиска по сообщениям: {e.__class__.__name__}: {e}")
        set_setting(con, "last_global", now_utc().isoformat())


async def cmd_invite_link(args):
    """Одноразовая ссылка в группу уведомлений: по ней вступает второй аккаунт на сервере."""
    con = db()
    notify_id = get_setting(con, "notify_chat")
    if not notify_id:
        sys.exit("Группа уведомлений не настроена")
    client = await connect()
    try:
        entity = await client.get_input_entity(PeerChannel(int(notify_id)))
        res = await client(ExportChatInviteRequest(
            entity, usage_limit=1, expire_date=now_utc() + timedelta(hours=args.hours)))
        print(res.link)
        print(f"Ссылка одноразовая и действует {args.hours} ч. Никому не пересылайте.")
    finally:
        await client.disconnect()


async def cmd_watch(args):
    """Следит за чатами онлайн, добирает историю и раз в сутки ищет новые группы."""
    kw = load_keywords()
    con = db()
    notify_id = get_setting(con, "notify_chat")
    if not notify_id:
        sys.exit("Сначала создайте группу для уведомлений: .\\tg notify-setup")
    rows = con.execute("SELECT * FROM chats WHERE tracked=1").fetchall()
    by_id = {r["id"]: r for r in rows}
    client = await connect()
    targets = []
    for row in rows:
        try:
            targets.append(await input_entity_for_row(client, row))
        except (ValueError, TypeError, errors.RPCError) as e:
            print(f"✖ пропуск {row['title']}: {e}")
    notify_entity = await client.get_input_entity(PeerChannel(int(notify_id)))

    @client.on(events.NewMessage(chats=targets))
    async def handler(event):
        if event.out:
            return
        text = (event.raw_text or "").strip()
        if not text:
            return
        links = kwork_links(event.message)
        if links:
            text += "\nОтклик: " + links[0]
        chat_id = resolve_id(event.chat_id)[0]
        row = by_id.get(chat_id)
        sender = await event.get_sender()
        username = (getattr(sender, "username", None) or "").lower()
        allowed = kw["whitelist"].get((row["username"] or "").lower() if row else "")
        if allowed and username not in allowed:
            return  # в шумных чатах слушаем только проверенных заказчиков
        if sender_blocked(con, event.sender_id, username):
            return  # бот или повтор того же автора
        result = evaluate(text, kw, soft=is_soft_chat(kw, chat_id, row["username"] if row else None))
        if not result:
            return
        niche, words, score = result
        h = text_hash(text)
        same = con.execute("SELECT id FROM leads WHERE text_hash=?", (h,)).fetchone()
        if same:
            con.execute("UPDATE leads SET dup_count=dup_count+1 WHERE id=?", (same["id"],))
            con.commit()
            return  # тот же заказ уже прилетал из другого чата
        rejected, note, info = await asyncio.to_thread(judge, text)  # сеть: Kwork и ИИ, не блокируем приём
        name = display_name(sender) if sender else None
        con.execute(
            "INSERT OR IGNORE INTO messages (chat_id, msg_id, date, sender_id, sender_username, sender_name, text,"
            " checked) VALUES (?,?,?,?,?,?,?,1)",
            (chat_id, event.id, event.message.date.isoformat(), event.sender_id, username or None, name, text))
        cur = con.execute(
            "INSERT OR IGNORE INTO leads (chat_id, msg_id, niche, matched, score, text_hash, created_at, status,"
            " note, llm) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (chat_id, event.id, niche, ", ".join(words), score, h, now_utc().isoformat(),
             "no" if rejected else "new", note, info))
        con.commit()
        if rejected:
            print(f"[{datetime.now():%H:%M}] отсеяно #{cur.lastrowid}: {(note or '')[:60]}")
            return
        await send_lead(client, notify_entity, con, cur.lastrowid)
        sheet_sync(con)
        print(f"[{datetime.now():%H:%M}] #{cur.lastrowid} {row['title'][:30] if row else chat_id}: {text[:60]}")

    last_notice = get_setting(con, "last_start_notice")
    if not last_notice or now_utc() - datetime.fromisoformat(last_notice) > timedelta(minutes=30):
        await client.send_message(notify_entity, f"Слежение запущено. Чатов в работе: {len(rows)}.", parse_mode=None)
    set_setting(con, "last_start_notice", now_utc().isoformat())  # частые перезапуски не засоряют группу
    print(f"Слежу за {len(targets)} чатами. Заявки уходят в группу «Лиды из Telegram».")
    print(f"Добор истории — каждые {args.backfill} ч, поиск новых групп — каждые {args.search} ч.")
    print("Остановить — Ctrl+C.")
    asyncio.create_task(watch_backfill(client, con, notify_entity, args.backfill))
    if kw["boards_enabled"]:
        asyncio.create_task(watch_boards(client, con, notify_entity, 3))  # свежий заказ виден за минуты, а не за 10
    else:
        print("Биржи (Kwork, FL.ru, Freelance.ru) отключены: boards_enabled=false в keywords.json")
    asyncio.create_task(watch_search(client, con, notify_entity, args.search))
    asyncio.create_task(watch_global(client, con, notify_entity))
    await client.run_until_disconnected()


async def cmd_push(args):
    """Отправляет в группу уведомлений лиды, которые туда ещё не отправлялись."""
    con = db()
    notify_id = get_setting(con, "notify_chat")
    if not notify_id:
        sys.exit("Сначала создайте группу для уведомлений: .\\tg notify-setup")
    statuses = args.status.split(",")
    rows = con.execute(
        f"SELECT l.id FROM leads l JOIN messages m ON m.chat_id=l.chat_id AND m.msg_id=l.msg_id"
        f" WHERE l.sent_at IS NULL AND l.status IN ({','.join('?' * len(statuses))})"
        f" ORDER BY m.date LIMIT ?", statuses + [args.limit]).fetchall()
    if not rows:
        print("Все лиды уже отправлены.")
        return
    ids = [r["id"] for r in rows]
    batches = [ids[i:i + args.batch] for i in range(0, len(ids), args.batch)]
    client = await connect()
    sent = 0
    try:
        notify_entity = await client.get_input_entity(PeerChannel(int(notify_id)))
        for batch in batches:
            leads = [con.execute(LEADS_SQL + " WHERE l.id=?", (i,)).fetchone() for i in batch]
            text = "\n\n— — —\n\n".join(format_lead(r, limit=550) for r in leads if r)
            try:
                msg = await client.send_message(notify_entity, text, parse_mode=None, link_preview=False)
            except errors.FloodWaitError as e:
                if e.seconds > 900:
                    print(f"Telegram просит подождать {e.seconds} с — остановился на {sent} из {len(ids)}")
                    break
                print(f"  пауза {e.seconds} с по просьбе Telegram…")
                await asyncio.sleep(e.seconds + 5)
                msg = await client.send_message(notify_entity, text, parse_mode=None, link_preview=False)
            con.executemany("UPDATE leads SET sent_at=?, sent_msg_id=? WHERE id=?",
                            [(now_utc().isoformat(), msg.id, i) for i in batch])
            con.commit()
            sent += len(batch)
            print(f"  отправлено {sent} из {len(ids)}")
            await asyncio.sleep(random.uniform(5, 9))
    finally:
        await client.disconnect()
    print(f"Отправлено лидов: {sent}")


def cmd_activity(args):
    """Насколько живые отслеживаемые чаты: сообщений в день, сколько людей пишет."""
    con = db()
    since = (now_utc() - timedelta(days=args.days)).isoformat()
    rows = con.execute(
        "SELECT c.title, c.username, c.participants, COUNT(m.msg_id) AS msgs,"
        " COUNT(DISTINCT m.sender_id) AS authors, MAX(m.date) AS last_date"
        " FROM chats c LEFT JOIN messages m ON m.chat_id=c.id AND m.date>=?"
        " WHERE c.tracked=1 GROUP BY c.id ORDER BY msgs DESC", (since,)).fetchall()
    print(f"{'сообщ.':>7}{'в день':>8}{'людей':>7}{'участн.':>9}  {'последнее':<17} чат")
    for r in rows:
        last = fmt_local(r["last_date"])[:16] if r["last_date"] else "—"
        handle = f" @{r['username']}" if r["username"] else ""
        print(f"{r['msgs']:>7}{r['msgs'] / args.days:>8.0f}{r['authors']:>7}"
              f"{r['participants'] or 0:>9}  {last:<17} {r['title'][:45]}{handle}")
    alive = sum(1 for r in rows if r["msgs"] >= args.days)  # хотя бы сообщение в день
    print(f"\nЖивых чатов (от 1 сообщения в день): {alive} из {len(rows)}")


def cmd_rejects(args):
    """Что ИИ-фильтр отсеял за последние N дней: чтобы вовремя заметить потерянные заявки."""
    con = db()
    since = (now_utc() - timedelta(days=args.days)).isoformat()
    rows = con.execute(LEADS_SQL + " WHERE l.note LIKE 'ИИ:%' AND l.created_at>=? ORDER BY l.id DESC LIMIT ?",
                       (since, args.limit)).fetchall()
    for r in rows:
        text = " ".join(r["text"].split())
        print(f"#{r['id']} {r['note']}\n    {r['chat_title'][:30]} · {text[:200]}\n    {msg_link(r)}")
    print(f"\nОтсеяно ИИ за {args.days} дн.: {len(rows)}. Вернуть заявку: mark new <номер>")


def cmd_sheet(args):
    """Ручная отправка в таблицу тех заявок, что туда ещё не попали."""
    print("добавлено строк:", sheet_sync(db()))


def cmd_stats(args):
    con = db()
    q = lambda sql: con.execute(sql).fetchone()[0]
    print(f"Чатов в базе: {q('SELECT COUNT(*) FROM chats')}, отслеживается: {q('SELECT COUNT(*) FROM chats WHERE tracked=1')}")
    print(f"Сообщений: {q('SELECT COUNT(*) FROM messages')}")
    for status, n in con.execute("SELECT status, COUNT(*) FROM leads GROUP BY status"):
        print(f"Лидов «{STATUS_RU.get(status, status)}»: {n}")


# ---------- CLI ----------

def main():
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")

    p = argparse.ArgumentParser(prog="tg", description="Поиск лидов в группах Telegram")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("login", help="войти в аккаунт (один раз, в терминале)").set_defaults(func=cmd_login)
    sub.add_parser("me", help="проверить подключение").set_defaults(func=cmd_me)

    s = sub.add_parser("search", help="найти открытые группы и каналы")
    s.add_argument("queries", nargs="*", help="запросы (по умолчанию — search_queries из keywords.json)")
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--min-members", type=int, default=0)
    s.set_defaults(func=cmd_search)

    s = sub.add_parser("dialogs", help="ваши группы и каналы, включая закрытые и платные")
    s.add_argument("--groups", action="store_true", help="только группы, без каналов")
    s.set_defaults(func=cmd_dialogs)

    s = sub.add_parser("chats", help="список чатов в базе")
    s.add_argument("--tracked", action="store_true", help="только отслеживаемые")
    s.add_argument("--filter", help="фильтр по названию / username / запросу")
    s.add_argument("--limit", type=int, default=200)
    s.set_defaults(func=cmd_chats)

    s = sub.add_parser("track", help="отслеживать чаты (@name, ссылка или id)")
    s.add_argument("refs", nargs="+")
    s.set_defaults(func=cmd_track)

    s = sub.add_parser("untrack", help="перестать отслеживать")
    s.add_argument("refs", nargs="+")
    s.set_defaults(func=cmd_untrack)

    s = sub.add_parser("join", help=f"вступить в чаты (не больше {JOINS_PER_DAY} в сутки, с паузами)")
    s.add_argument("refs", nargs="+")
    s.set_defaults(func=cmd_join)

    s = sub.add_parser("parse", help="собрать сообщения из отслеживаемых (или указанных) чатов")
    s.add_argument("refs", nargs="*")
    s.add_argument("--days", type=int, default=7)
    s.add_argument("--max", type=int, default=3000, help="максимум сообщений с одного чата")
    s.add_argument("--refetch", action="store_true", help="игнорировать уже собранное и пройти период заново")
    s.set_defaults(func=cmd_parse)

    s = sub.add_parser("leads", help="отобрать кандидатов в лиды из собранных сообщений")
    s.add_argument("--days", type=int, default=7)
    s.add_argument("--recheck", action="store_true", help="перепроверить все сообщения (после правки keywords.json)")
    s.add_argument("--full", action="store_true")
    s.set_defaults(func=cmd_leads)

    s = sub.add_parser("show", help="показать лиды")
    s.add_argument("ids", nargs="*", type=int)
    s.add_argument("--status", default="new", help="new, ok, no, done — через запятую")
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--full", action="store_true")
    s.set_defaults(func=cmd_show)

    s = sub.add_parser("mark", help="пометить лиды: ok — лид, no — не лид, done — в работе, new — сбросить")
    s.add_argument("status", choices=list(STATUS_RU))
    s.add_argument("ids", nargs="+", type=int)
    s.add_argument("--note")
    s.set_defaults(func=cmd_mark)

    s = sub.add_parser("export", help="выгрузить лиды в Excel")
    s.add_argument("--status", default="ok,new,done")
    s.add_argument("--out")
    s.set_defaults(func=cmd_export)

    s = sub.add_parser("run", help="parse + leads по отслеживаемым чатам (для ежедневного запуска)")
    s.add_argument("--days", type=int, default=2)
    s.add_argument("--max", type=int, default=3000)
    s.set_defaults(func=cmd_run)

    s = sub.add_parser("notify-setup", help="создать закрытую группу для уведомлений о заявках")
    s.add_argument("--title", default="Лиды из Telegram")
    s.add_argument("--chat", help="использовать существующий чат (@name, ссылка или id) вместо новой группы")
    s.add_argument("--invite", help="вступить в существующую группу по одноразовой ссылке https://t.me/+xxxx")
    s.set_defaults(func=cmd_notify_setup)

    s = sub.add_parser("invite-link", help="одноразовая ссылка в группу уведомлений (для второго аккаунта)")
    s.add_argument("--hours", type=int, default=24)
    s.set_defaults(func=cmd_invite_link)

    s = sub.add_parser("watch", help="следить онлайн, добирать историю и искать новые группы")
    s.add_argument("--backfill", type=float, default=6, help="как часто опрашивать чаты, часов (0.25 = раз в 15 минут)")
    s.add_argument("--search", type=int, default=24, help="через сколько часов искать новые группы")
    s.set_defaults(func=cmd_watch)

    s = sub.add_parser("push", help="отправить лиды в группу уведомлений (что ещё не отправлено)")
    s.add_argument("--status", default="ok")
    s.add_argument("--limit", type=int, default=500)
    s.add_argument("--batch", type=int, default=4, help="сколько заявок в одном сообщении")
    s.set_defaults(func=cmd_push)

    s = sub.add_parser("cleanup", help="убрать из группы уведомлений заявки, снятые со статуса «лид»")
    s.add_argument("--limit", type=int, default=1000, help="сколько последних сообщений просмотреть")
    s.add_argument("--dry-run", action="store_true", help="только показать, ничего не удалять")
    s.set_defaults(func=cmd_cleanup)

    s = sub.add_parser("activity", help="какие чаты живые: сообщений в день, сколько людей пишет")
    s.add_argument("--days", type=int, default=14)
    s.set_defaults(func=cmd_activity)

    s = sub.add_parser("rejects", help="что отсеял ИИ-фильтр")
    s.add_argument("--days", type=int, default=2)
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(func=cmd_rejects)

    sub.add_parser("sheet", help="отправить новые заявки в Google-таблицу").set_defaults(func=cmd_sheet)

    sub.add_parser("stats", help="статистика базы").set_defaults(func=cmd_stats)

    args = p.parse_args()
    if inspect.iscoroutinefunction(args.func):
        try:
            asyncio.run(args.func(args))
        except KeyboardInterrupt:
            print("\nОстановлено.")
    else:
        args.func(args)


if __name__ == "__main__":
    main()
