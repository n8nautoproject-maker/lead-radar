/**
 * Приём заявок от tg-leads в Google-таблицу.
 *
 * Установка (один раз, в своей таблице):
 *  1. Расширения → Apps Script. Удалить всё в редакторе и вставить этот файл.
 *  2. Слева «Настройки проекта» → «Свойства скрипта» → «Добавить свойство»:
 *       имя TOKEN, значение — любая длинная случайная строка (придумайте сами, 30+ символов).
 *  3. Справа вверху «Развернуть» → «Новое развёртывание» → тип «Веб-приложение»:
 *       Выполнять от имени: «Я», У кого есть доступ: «Все». Нажать «Развернуть», подтвердить доступ.
 *  4. Скопировать выданный адрес (оканчивается на /exec). Адрес и TOKEN вписать в файл .env проекта:
 *       SHEETS_WEBHOOK_URL=адрес
 *       SHEETS_WEBHOOK_TOKEN=тот же токен
 *
 * Скрипт отвечает только тому, кто знает TOKEN. Новые заявки вставляются под шапку (самые свежие сверху),
 * старые опускаются вниз. Поддерживается скрытие строк по статусу (столбец G).
 */

const SHEET_NAME = 'Фриланс';
const STATUS_COL = 7; // G

function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents);
    if (body.token !== PropertiesService.getScriptProperties().getProperty('TOKEN')) {
      return out({ ok: false, error: 'bad token' });
    }
    const sheet = SpreadsheetApp.getActive().getSheetByName(SHEET_NAME);
    if (!sheet) return out({ ok: false, error: 'no sheet' });

    if (body.action === 'add') return out(addRows(sheet, body.rows || []));
    if (body.action === 'status') return out(setStatus(sheet, body.updates || []));
    if (body.action === 'add_sheet') {            // запись строк на любой существующий лист по имени
      const other = SpreadsheetApp.getActive().getSheetByName(String(body.sheet || ''));
      if (!other) return out({ ok: false, error: 'no sheet ' + body.sheet });
      return out(addRows(other, body.rows || []));
    }
    return out({ ok: false, error: 'unknown action' });
  } catch (err) {
    return out({ ok: false, error: String(err) });
  }
}

// rows: массив строк [дата, площадка, автор, ссылка, тип, запрос, статус, комментарий, первое сообщение]
function addRows(sheet, rows) {
  const links = existingLinks(sheet);
  const fresh = rows.filter(r => r[3] && !links.has(String(r[3])));
  if (!fresh.length) return { ok: true, added: 0 };
  sheet.insertRowsAfter(1, fresh.length);                         // под шапку
  sheet.getRange(2, 1, fresh.length, fresh[0].length).setValues(fresh);
  return { ok: true, added: fresh.length };
}

// updates: массив {link, status, note} — например, пометить заявку закрытой или «в работе»
function setStatus(sheet, updates) {
  const last = sheet.getLastRow();
  if (last < 2) return { ok: true, updated: 0 };
  const links = sheet.getRange(2, 4, last - 1, 1).getValues();
  let n = 0;
  updates.forEach(u => {
    for (let i = 0; i < links.length; i++) {
      if (String(links[i][0]) === String(u.link)) {
        sheet.getRange(i + 2, STATUS_COL).setValue(u.status);
        if (u.note) sheet.getRange(i + 2, 8).setValue(u.note);
        n++;
      }
    }
  });
  return { ok: true, updated: n };
}

function existingLinks(sheet) {
  const last = sheet.getLastRow();
  const set = new Set();
  if (last < 2) return set;
  sheet.getRange(2, 4, last - 1, 1).getValues().forEach(r => { if (r[0]) set.add(String(r[0])); });
  return set;
}

function out(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}
