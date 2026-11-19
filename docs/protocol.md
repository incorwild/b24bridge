# Bitrix24 (облако): протокол мессенджера без платного REST

Снято 2026-11-19 на облачном портале (тестовый пользователь id 54).

## Авторизация

- Cookie сессии браузера + `sessid` (CSRF-токен; в странице: `BX.bitrix_sessid()`).
- Все вызовы: `POST https://<portal>/rest/<method>.json`, form-urlencoded, поле `sessid=<...>`, с cookie.
- На этом тарифе работает без вебхука/приложения (проверено: ответы 200).
- Не проверено: работает ли тот же вызов не из браузера (Go-клиент с перенесёнными cookie) — первый шаг прототипа.

## Получение событий (Push&Pull, сервер v4)

1. `POST /rest/pull.config.get.json` (`CACHE=N`) →
   - `server.websocket_secure` = `wss://rtc-v2-ms6.bitrix24.com/subws2/`
   - `server.version` = 4 (не JSON-RPC), `server.mode` = `personal`
   - `channels.private.id`, `channels.shared.id` (≈100 символов, с подписью), срок жизни ≈12 ч (`start`/`end`)
   - `api.revision_web` = 19
2. WebSocket: `wss://…/subws2/?CHANNEL_ID=<private>/<shared>&hostname=<portal>&revision=<revision_web>`
   - без `binaryMode=true` сервер шлёт текст; опционально `mid`, `tag`, `time` для докачки после переподключения.
3. Кадр: один или несколько `#!NGINXNMS!#{json}#!NGINXNME!#`.
   `json = {id, mid, channel, tag, time, text:{module_id, command, params, extra}}`.
   Дедуп по `mid`.
4. Закрытие с кодом WRONG_CHANNEL_ID → заново `pull.config.get` (каналы протухли).

### Пойманные команды (`module_id = "im"`)

| command | когда | ключевые поля `params` |
|---|---|---|
| `message` | новое сообщение в личке/чате (в т.ч. своё) | `dialogId`, `chatId`, `message{id, senderId, recipientId, date, text, params, prevId}`, `users`, `files`, `counter` |
| `messageChat` | (ожидается для групповых чатов; не поймано) | — |
| `messageUpdate` | правка | `id`, `chatId`, `dialogId`, `senderId`, `text`, `params.IS_EDITED` |
| `messageParamsUpdate` | изменение параметров (сопровождает правку) | `id`, `chatId`, `params` |
| `messageLike` / `addReaction` | реакция | `id`/`reaction.messageId`, `userId`, `reaction` (напр. `okHand`) |
| `readMessageChat` | вы прочитали чат | — |
| `deleteChatAnchors` | служебное | — |

Входящее = `message.senderId != <свой userId>`. Свои исходящие тоже приходят — фильтровать (иначе автоответчик ответит сам себе).
«Собеседник прочитал» (поймано позже, через сервис): `params = {dialogId, chatId, userId, userName, lastId, date, viewedMessages[], chatMessageStatus:"delivered"}`.
«Печатает»: `params = {dialogId, userId, userName, userFirstName}`.
Своё прочтение (`readMessageChat`): `{dialogId, chatId, lastId, counter, viewedMessages[], unread, muted}`.

Обновлённый веб-клиент ходит через `/bitrix/services/main/ajax.php?action=im.v2.Chat.load | im.v2.Chat.Message.list | im.v2.Chat.Message.read | im.v2.Folder.Recent.tail`; `/rest/im.*` при этом продолжает работать.

## Методы (проверено)

| Действие | Метод | Результат |
|---|---|---|
| Отправить | `im.message.add` `DIALOG_ID=54` (user) / `chatNNN` (группа), `MESSAGE=…` | id сообщения; эхо `message` в WS |
| История | `im.dialog.messages.get` `DIALOG_ID`, `LIMIT` | `messages[{id, author_id, date, text, unread, params}]`, `users`, `files` |
| Прочитать | `im.dialog.read` `DIALOG_ID` | `{chatId, dialogId, lastId, counter}`; в WS `readMessageChat` |
| Список чатов | `im.recent.list` | `items[{id, chat_id, type, title, message, unread, counter, …}]` |
| Прочитал ли собеседник | `im.dialog.get` `DIALOG_ID` → `readed_list[{user_id, message_id}]` | сравнить `message_id` с id своего сообщения |
| Настройки push | `pull.config.get` | см. выше |
