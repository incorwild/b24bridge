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
- Хранилище (SQLite) для истории и offset, как `chatstorage` в оригинале.
- Файлы: `im.disk.folder.get` → загрузка → `im.disk.file.commit`.
- Несколько аккаунтов, MCP-сервер для агентов.
- Поддержка push-сервера v5 (JSON-RPC), если портал на него переедет.
