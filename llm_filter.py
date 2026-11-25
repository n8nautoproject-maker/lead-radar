"""LLM-фильтр заявок на GigaChat (Сбер).

classify(text) -> {"v": "Y"|"N", "c": категория, "r": причина, "tokens": N} или None при сбое.
Ключ авторизации берётся из GIGACHAT_CREDENTIALS (окружение или .env). В журнал ключ не пишется.
Сертификат Минцифры лежит в certs/root.pem и применяется только к запросам этого модуля.
"""
import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

BASE = Path(__file__).resolve().parent
CA_FILE = BASE / "certs" / "root.pem"
OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
API_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
SCOPE = "GIGACHAT_API_PERS"
MAX_TEXT = 900

# Промпты здесь — упрощённые примеры. Под свою нишу их нужно дописать и проверить на своих сообщениях.
SYSTEM_PROMPT = """Ты отбираешь заявки из сообщений Telegram-чатов. Заявка (Y): автор сам хочет заказать услугу или найти исполнителя по теме: боты, парсинг, автоматизация, сайты. Не заявка (N): самореклама, вакансия в штат, вопрос без заказа, реклама, другая тема. Если сомневаешься, выбирай N.

Ответь ТОЛЬКО одним JSON: {"v":"Y" или "N","c":"категория","r":"причина до 8 слов","t":"тип заявки","q":"суть одной строкой"}."""


PAIN_PROMPT = """Ты ищешь в сообщениях из профессиональных чатов реальные проблемы людей, которые можно автоматизировать (ручной учёт, отчёты, таблицы, интеграции). Y: автор описывает свою рутинную задачу или ищет инструмент. N: реклама, вакансия, новость, болтовня, советы другим.

Ответь ТОЛЬКО одним JSON: {"v":"Y" или "N","k":"тип","p":"суть проблемы","h":0 или 1 (прямо ищет инструмент),"m":"вежливое первое сообщение с вопросом о проблеме, без продажи"}."""

JOB_PROMPT = """Ты отбираешь вакансии в сфере ИИ и автоматизации. Y: предложение найма (вакансия, частичная занятость, подработка). N: резюме, самореклама, разовый заказ, реклама курсов, сомнительные схемы.

Ответь ТОЛЬКО одним JSON: {"v":"Y" или "N","t":"позиция","s":"зарплата","f":"удалённо, гибрид, офис или неясно","k":"вакансия или подработка","e":"тип работодателя","w":"почему подходит"}."""


def parse_job(answer):
    d = None
    m = re.search(r"\{.*\}", answer, re.S)
    if m:
        try:
            d = json.loads(m.group(0), strict=False)
        except ValueError:
            d = None
    if d is None:
        v = re.search(r'"v"\s*:\s*"([YyNn])', answer)
        if not v:
            return {"v": "N"}
        def field(name):
            f = re.search(r'"%s"\s*:\s*"(.*?)"\s*(?:,\s*"|\}|$)' % name, answer, re.S)
            return f.group(1) if f else ""
        d = {"v": v.group(1), **{k: field(k) for k in ("t", "s", "f", "k", "e", "w")}}
    v = str(d.get("v", "")).strip().upper()[:1]
    if v != "Y":
        return {"v": "N"}
    clean = lambda k, n: " ".join(str(d.get(k, "")).split())[:n]
    return {"v": "Y", "t": clean("t", 90), "s": clean("s", 60), "f": clean("f", 20), "k": clean("k", 20),
            "e": clean("e", 40), "w": clean("w", 140)}


PAIN_SCREEN = """Сообщение из профессионального чата. Ответь Y, если автор описывает свою ручную или трудоёмкую задачу или ищет инструмент для неё, иначе N. Только JSON: {"v":"Y"} или {"v":"N"}."""


DIRECT_PROMPT = """Ты отбираешь клиентов на настройку контекстной рекламы из сообщений чатов. Y: бизнес сам ищет специалиста или подрядчика. N: самореклама, вакансия в штат, обучение, вопрос без заказа, другая тема.

Ответь ТОЛЬКО одним JSON: {"v":"Y" или "N","c":"категория","r":"причина","t":"тип","q":"суть"}."""


class GigaChatError(RuntimeError):
    pass


def _read_key():
    key = os.environ.get("GIGACHAT_CREDENTIALS", "")
    if not key:
        env = BASE / ".env"
        if env.exists():
            for line in env.read_text(encoding="utf-8").splitlines():
                if line.startswith("GIGACHAT_CREDENTIALS="):
                    key = line.split("=", 1)[1]
    key = key.strip().strip("\"'")
    if not key:
        raise GigaChatError("GIGACHAT_CREDENTIALS не задан")
    return key


class Client:
    def __init__(self, model="GigaChat"):
        self.model = model
        self.key = _read_key()
        cafile = str(CA_FILE) if CA_FILE.exists() else None
        self.ctx = ssl.create_default_context(cafile=cafile)
        self.token = None
        self.expires = 0.0

    def _post(self, url, data, headers, timeout=40):
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, context=self.ctx, timeout=timeout) as r:
            return json.loads(r.read())

    def _auth(self):
        if self.token and time.time() < self.expires - 90:
            return
        js = self._post(
            OAUTH_URL,
            urllib.parse.urlencode({"scope": SCOPE}).encode(),
            {"Authorization": "Basic " + self.key, "RqUID": str(uuid.uuid4()),
             "Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        )
        self.token = js["access_token"]
        # expires_at приходит в миллисекундах
        self.expires = js.get("expires_at", 0) / 1000.0 or time.time() + 25 * 60

    def classify(self, text, model=None, mode="dev"):
        text = (text or "").strip()[:MAX_TEXT]
        body = json.dumps({
            "model": model or self.model, "temperature": 0.1, "max_tokens": 220,
            "messages": [{"role": "system", "content": DIRECT_PROMPT if mode == "direct" else SYSTEM_PROMPT},
                         {"role": "user", "content": "Сообщение:\n<<<\n" + text + "\n>>>"}],
        }).encode()
        last = None
        for attempt in range(3):
            try:
                self._auth()
                js = self._post(API_URL, body, {"Authorization": "Bearer " + self.token,
                                                "Content-Type": "application/json", "Accept": "application/json"})
                answer = js["choices"][0]["message"]["content"]
                res = parse_answer(answer)
                res["tokens"] = js.get("usage", {}).get("total_tokens", 0)
                return res
            except urllib.error.HTTPError as e:
                last = "HTTP %s" % e.code
                if e.code == 401:
                    self.token = None
                elif e.code == 429:
                    time.sleep(3 + attempt * 5)
                elif e.code >= 500:
                    time.sleep(2)
                else:
                    break
            except (urllib.error.URLError, TimeoutError, ConnectionError, KeyError, ValueError) as e:
                last = type(e).__name__
                time.sleep(2)
        return {"v": None, "c": "сбой", "r": str(last), "tokens": 0}


def parse_pain(answer):
    """Разбор ответа Pro. Модель иногда ломает JSON (переводы строк, кавычки в тексте), поэтому есть запасной разбор."""
    d = None
    m = re.search(r"\{.*\}", answer, re.S)
    if m:
        try:
            d = json.loads(m.group(0), strict=False)
        except ValueError:
            d = None
    if d is None:  # запасной путь: достаём поля регулярками
        v = re.search(r'"v"\s*:\s*"([YyNn])', answer)
        if not v:
            return {"v": "N", "k": "", "p": "", "h": 0, "m": ""}
        def field(name):
            f = re.search(r'"%s"\s*:\s*"(.*?)"\s*(?:,\s*"|\}|$)' % name, answer, re.S)
            return f.group(1) if f else ""
        mm = re.search(r'"m"\s*:\s*"(.*)', answer, re.S)
        d = {"v": v.group(1), "k": field("k"), "p": field("p"),
             "h": 1 if re.search(r'"h"\s*:\s*"?1', answer) else 0,
             "m": re.sub(r'["\s}]+$', "", mm.group(1)) if mm else ""}
    v = str(d.get("v", "")).strip().upper()[:1]
    if v not in ("Y", "N"):
        return {"v": "N", "k": "", "p": "", "h": 0, "m": ""}
    return {"v": v, "k": str(d.get("k", ""))[:40], "p": str(d.get("p", ""))[:140],
            "h": 1 if str(d.get("h", "0")) in ("1", "true", "True") else 0,
            "m": " ".join(str(d.get("m", "")).split())[:500]}


def parse_screen(answer):
    m = re.search(r"\{.*?\}", answer, re.S)
    v = ""
    if m:
        try:
            v = str(json.loads(m.group(0)).get("v", "")).strip().upper()[:1]
        except ValueError:
            pass
    if not v and "Y" in answer.upper()[:6]:
        v = "Y"
    return {"v": v if v in ("Y", "N") else "N"}


def _pain(self, text, model=None, system=None, max_tokens=420, parser=None):
    """Классификация сообщения селлера как «боль, которую можно автоматизировать» + черновик первого сообщения."""
    text = (text or "").strip()[:MAX_TEXT]
    parser = parser or parse_pain
    body = json.dumps({
        "model": model or self.model, "temperature": 0.05, "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system or PAIN_PROMPT},
                     {"role": "user", "content": "Сообщение:\n<<<\n" + text + "\n>>>"}],
    }).encode()
    last = None
    for attempt in range(3):
        try:
            self._auth()
            js = self._post(API_URL, body, {"Authorization": "Bearer " + self.token,
                                            "Content-Type": "application/json", "Accept": "application/json"})
            res = parser(js["choices"][0]["message"]["content"])
            res["tokens"] = js.get("usage", {}).get("total_tokens", 0)
            return res
        except urllib.error.HTTPError as e:
            last = "HTTP %s" % e.code
            if e.code == 401:
                self.token = None
            elif e.code == 429:
                time.sleep(3 + attempt * 5)
            elif e.code >= 500:
                time.sleep(2)
            else:
                break
        except (urllib.error.URLError, TimeoutError, ConnectionError, KeyError, ValueError) as e:
            last = type(e).__name__
            time.sleep(2)
    return {"v": None, "k": "сбой", "p": str(last), "h": 0, "m": "", "tokens": 0}


Client.pain = _pain


def _pain_screen(self, text):
    """Дешёвый первый проход на лёгкой модели: Y/N. Сбой даёт v=None."""
    return _pain(self, text, model="GigaChat", system=PAIN_SCREEN, max_tokens=40, parser=parse_screen)


Client.pain_screen = _pain_screen


def _job(self, text):
    """Вакансия или подработка под профиль пользователя. v=None при сбое ИИ."""
    return _pain(self, text, system=JOB_PROMPT, max_tokens=260, parser=parse_job)


Client.job = _job


def parse_answer(answer):
    m = re.search(r"\{.*?\}", answer, re.S)
    if m:
        try:
            d = json.loads(m.group(0))
            v = str(d.get("v", "")).strip().upper()[:1]
            if v in ("Y", "N"):
                return {"v": v, "c": str(d.get("c", ""))[:30], "r": str(d.get("r", ""))[:80],
                        "t": str(d.get("t", ""))[:60], "q": str(d.get("q", ""))[:200]}
        except ValueError:
            pass
    low = answer.lower()
    if "заявка" in low and "не заявка" not in low:
        return {"v": "Y", "c": "заявка", "r": answer[:60]}
    return {"v": "N", "c": "непонятно", "r": answer[:60]}
