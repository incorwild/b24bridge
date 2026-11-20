# b24bridge

Прототип моста к мессенджеру облачного Bitrix24 по образцу
[go-whatsapp-web-multidevice](https://github.com/aldinokemal/go-whatsapp-web-multidevice):
сервис работает от имени вошедшего пользователя, получает push-события
мессенджера, шлёт их на ваши вебхуки и даёт HTTP API, чтобы отвечать,
читать историю и проверять прочтение.

Платный REST (вебхуки/приложения) не нужен: используется то же, что и веб-мессенджер —
cookie сессии + `sessid` к `/rest/*.json` и WebSocket Push&Pull.
Протокол описан в [docs/protocol.md](docs/protocol.md).

> ⚠️ Это эмуляция веб-клиента. Внутренний протокол может меняться без
> предупреждения, а автоматизация от имени пользователя может противоречить
> условиям Bitrix24. Используйте отдельную учётную запись.

## Запуск

1. Войдите в портал в Chrome → DevTools → Network → любой запрос к
   `https://<портал>/rest/...` → Request Headers → скопируйте значение
   `Cookie` целиком в файл `cookie.txt` (рядом с бинарником, права `600`).
   `document.cookie` не подойдёт: сессионные cookie помечены HttpOnly.
2. Соберите и запустите:

```bash
go build -o bin/b24bridge ./cmd/b24bridge
```

```bash
B24_PORTAL=https://example.bitrix24.ru B24_COOKIE_FILE=cookie.txt WEBHOOK_URLS=https://my.app/hook WEBHOOK_SECRET=change-me ./bin/b24bridge
```

При старте сервис открывает `/online/`, берёт оттуда `sessid` и ваш `USER_ID`.
Входите с галочкой «Запомнить меня»: тогда в cookie есть `BITRIX_SM_UIDH`/`UIDL`.

### Обновление сессии

- **Keepalive.** Раз в `SESSION_KEEPALIVE` (10m) — лёгкий `pull.config.get`,
  чтобы PHP-сессия не умерла от простоя (WebSocket идёт на другой сервер и не считается).
- **Самовосстановление.** На любой ошибке авторизации сервис выбрасывает
  `PHPSESSID` и заходит заново через «Запомнить меня» — портал выдаёт новую сессию
  (проверено на облачном портале). Запрос, на котором это случилось, повторяется.
- **Сохранение.** Обновлённые cookie раз в 30 с пишутся обратно в `B24_COOKIE_FILE`
  (атомарно, права 600); отключить — `B24_COOKIE_READONLY=true`.
- **Полный выход** («выйти на всех устройствах», смена пароля, истёк срок
  «Запомнить меня»): вебхук `session.logged_out`, `/healthz` → `session.authorized=false`,
  push-подписка ждёт. Войдите в браузере и отправьте новый Cookie:
  `curl -X POST localhost:3077/session/cookies --data-binary @cookies.txt` —
  сервис продолжит без перезапуска и пришлёт `session.restored`.
- Если cookie не годятся уже при старте — сервис завершается с ошибкой.

### Переменные окружения

| Переменная | Назначение |
|---|---|
| `B24_PORTAL` | адрес портала |
| `B24_COOKIE` / `B24_COOKIE_FILE` | заголовок Cookie из браузера |
| `B24_SESSID` | необязательно, иначе берётся со страницы |
| `WEBHOOK_URLS` | через запятую |
| `WEBHOOK_SECRET` | ключ HMAC для `X-Hub-Signature-256` |
| `WEBHOOK_EVENTS` | фильтр, например `message,message.edited`; пусто = все |
| `WEBHOOK_INCLUDE_OWN` | `true` — слать и события, вызванные самим аккаунтом (по умолчанию нет, чтобы бот не отвечал сам себе) |
| `SESSION_KEEPALIVE` | период keepalive, по умолчанию `10m` |
| `B24_COOKIE_READONLY` | `true` — не перезаписывать файл cookie |
| `INBOX_DB` | файл SQLite, по умолчанию `inbox.db` |
| `INBOX_BACKFILL` | глубина догрузки при старте, `24h`; `0` — выключить |
| `INBOX_INCLUDE_SYSTEM` | `true` — брать служебные сообщения чатов задач (по умолчанию нет) |
| `INBOX_BATCH_WINDOW` | пауза, после которой пачка сообщений диалога уходит агенту, `3m`; `0` — без группировки |
| `INBOX_BATCH_MAX` | максимальное ожидание пачки от первого сообщения, `10m` |
| `INBOX_IGNORE_BOTS` | `false` — не отсеивать сообщения чат-ботов (по умолчанию отсеиваются) |
| `INBOX_IGNORE_SENDERS` | id пользователей, чьи сообщения не нужны агенту: `12,34` |
| `INBOX_IGNORE_TEXT` | регулярное выражение (без учёта регистра) для текстов, которые не нужны агенту |
| `WEBHOOK_INCLUDE_SYSTEM` | `true` — слать на вебхук и служебные `message` (по умолчанию нет) |
| `AGENT_SEND_MODE` | `direct` — дать агенту `send_message` без подтверждения |
| `LISTEN_ADDR` | по умолчанию `127.0.0.1:3000` |
| `API_TOKEN` | если задан, API требует `Authorization: Bearer <token>` |
| `LOG_LEVEL` | `debug` — логировать и пропущенные служебные команды |

## Вебхук

`POST` JSON, заголовок `X-Hub-Signature-256: sha256=<hmac>`, до 5 попыток (1,2,4,8 с).

```json
{
  "event": "message",
  "timestamp": "2026-11-19T10:52:41Z",
  "portal": "example.bitrix24.ru",
  "user_id": 54,
  "payload": {
    "dialog_id": "88", "chat_id": 11136, "message_id": 338282,
    "sender_id": 88, "is_from_me": false, "text": "привет",
    "date": "2026-11-19T13:52:41+03:00",
    "sender": {"id": 88, "name": "Коллега"}
  },
  "raw": { "...исходные params push-события..." }
}
```

События (проверены вживую): `message`, `message.edited`, `message.reaction`
(снятие — реакция с `-`), `chat.read` (вы прочитали), `message.ack`
(собеседник прочитал: `user_id`, `user_name`, `message_id` = до какого
сообщения дочитал, `viewed_messages`), `chat_presence` (печатает).
Пока не проверено: `message.deleted`.
Ответить можно сразу из обработчика: `POST /send/message` с `dialog_id` из события.

## Inbox и агент

Сервис сам отбирает сообщения, которые касаются вас, и складывает их в SQLite (`INBOX_DB`, по умолчанию `inbox.db`):

| Метка | Когда |
|---|---|
| `direct` | вам написали в личку |
| `mention` | в тексте есть `[USER=<ваш id>]` |
| `task_responsible` / `task_accomplice` | сообщение в чате задачи (`entity_type = TASKS_TASK`), где вы исполнитель / соисполнитель |

Не попадают: свои сообщения, чат «с собой», общие чаты без упоминаний, сообщения чат-ботов
и служебные сообщения задач — «создал задачу», «изменил исполнителя на…», «изменил стадию»,
«задача просрочена» (у них автор `0`; даже если в них упомянуты вы). Дополнительно можно
отсечь отправителей (`INBOX_IGNORE_SENDERS`) и тексты по шаблону (`INBOX_IGNORE_TEXT`),
например `INBOX_IGNORE_TEXT='^(спасибо|ок|\+)$'`.

Для агента достаточно `WEBHOOK_EVENTS=inbox.batch,session.logged_out`: без фильтра на вебхук
уходят все события мессенджера (кроме служебных сообщений — см. `WEBHOOK_INCLUDE_SYSTEM`). Если вы ответили в диалоге сами,
его элементы закрываются. При старте сервис добирает пропущенное за `INBOX_BACKFILL` (24h):
непрочитанные диалоги и чаты ваших задач с активностью; сообщения старше вашего последнего ответа
в том же чате считаются обработанными. Каждый новый элемент — вебхук `inbox.new`.

### Пачки сообщений

Многие пишут одну мысль несколькими сообщениями («привет» → «есть минута?» → сам вопрос).
Поэтому диалог «дозревает»: пока собеседник пишет, сервис ждёт паузу `INBOX_BATCH_WINDOW`
(3m) и только потом шлёт один вебхук `inbox.batch` со всеми новыми сообщениями диалога.
Если человек пишет без пауз, пачка всё равно уходит через `INBOX_BATCH_MAX` (10m) после первого
сообщения. Если вы сами ответили в диалоге за это время — пачка не отправляется.
`inbox_list` (MCP и `/inbox`) тоже не показывает диалоги, которые ещё «дозревают»
(`include_settling=true` — показать). `INBOX_BATCH_WINDOW=0` выключает группировку.

```json
{"event": "inbox.batch", "payload": {
  "dialog_id": "88", "labels": ["direct"],
  "text": "Директор: привет\nесть минута?\nпо отчёту вопрос",
  "items": [{"id": 12, "message_id": 338401, "...": "..."}, "..."]
}}
```

### MCP

`http://127.0.0.1:3077/mcp` (streamable HTTP, тот же `API_TOKEN`). Инструменты:
`inbox_list`, `inbox_done`, `chat_history`, `task_get`, `propose_reply`, `drafts_list`, `mark_read`;
`send_message` — только при `AGENT_SEND_MODE=direct`.

### Подтверждение ответов

`propose_reply` ничего не отправляет адресату: черновик приходит вам в чат «с собой»:

```
🤖 Черновик #7 → задача «Отчёт» (Коллега)
На сообщение: «когда будет готово?»

Завтра до 12:00 пришлю.

Ответьте: «ок 7» — отправить, «нет 7» — отменить, «7: другой текст» — отправить с правкой.
```

Ответ «ок 7» (или «да 7», «ok #7») отправляет, «нет 7» отменяет, «7: текст» отправляет ваш вариант.
Повторное «ок» ничего не шлёт. Не начинайте свои заметки в чате с собой с «N:», если черновик N ждёт ответа.

### Запуск агента (Claude Code)

В `agent/` — промпт, конфиг MCP и скрипт одного прохода:

```bash
./agent/run.sh
```

Скрипт вызывает `claude -p` с `--mcp-config agent/mcp.json` и разрешает только инструменты inbox,
истории, задач и черновиков. Запускайте по расписанию (cron/launchd, например раз в 10 минут)
или по вебхуку `inbox.batch`. Тексты переписки при этом уходят в Claude API.

## HTTP API

| Метод | Путь | Что делает |
|---|---|---|
| GET | `/healthz` | портал, user_id, состояние сессии, подключён ли push |
| POST | `/session/cookies` | новый заголовок Cookie (текстом или `{"cookie":"…"}`) после ручного входа |
| POST | `/send/message` | `{"dialog_id":"88","message":"текст"}` → `{"message_id":…}`; группа — `"chat123"` |
| POST | `/message/{id}/update` | `{"message":"новый текст"}` |
| POST | `/message/{id}/delete` | удалить |
| GET | `/chats?limit=50` | список чатов (`im.recent.list`) |
| GET | `/chat/{dialog}/messages?limit=50&last_id=` | история |
| POST | `/chat/{dialog}/read` | отметить прочитанным |
| GET | `/chat/{dialog}/read-status?message_id=` | прочитали ли собеседники (`read`, `read_by`, `pending`) |
| GET | `/inbox?status=new\|done\|all&limit=` | элементы inbox |
| POST | `/inbox/done` | `{"ids":[1,2]}` |
| GET / POST | `/drafts` | список / `{"dialog_id","text","inbox_id"}` — черновик на подтверждение |
| — | `/mcp` | MCP для агентов |

## Устройство

```
cmd/b24bridge        конфиг из env, сборка компонентов
internal/bitrix      client.go  — cookie-jar, sessid, /rest/, ретрай при протухшем sessid, ~2 rps
                     pull.go    — pull.config.get → WebSocket, переподключение, обновление каналов (~12 ч), дедуп
                     events.go  — разбор #!NGINXNMS!#…#!NGINXNME!#, нормализация команд im
                     methods.go — send/update/delete/history/read/read-status
internal/webhook     очередь, HMAC, ретраи
internal/api         HTTP API
```

## Что дальше

- Проверить вживую удаление сообщения (`messageDelete`) и поправить поля.
- Файлы: `im.disk.folder.get` → загрузка → `im.disk.file.commit`.
- Несколько аккаунтов.
- Ответы на ваши сообщения (reply) как ещё один повод попасть в inbox.
- Поддержка push-сервера v5 (JSON-RPC), если портал на него переедет.
