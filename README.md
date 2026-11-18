# ПР7. Реактивный HTTP-сервис доставок на Spring WebFlux

Продолжение ПР4 (RSocket) и ПР5 (сервис доставки). Модели данных взяты без изменений:
доставки 11–17, товары, даркстор и статусы (Принят → В сборке → Доставка → Завершен) — из RSocket-сервера ПР4,
заказы и клиенты — из Order Service ПР5. Поэтому разница между технологиями видна только на уровне взаимодействия.

```
Client
   │
   ├── HTTP GET /api/deliveries/{id}  ──→  Mono<Delivery>
   │
   ├── HTTP GET /api/deliveries       ──→  Flux<Delivery>
   │
   └── HTTP GET /api/deliveries/{id}/stream   (SSE, text/event-stream)
           ↓
        Flux<StatusUpdate>: Принят → В сборке → Доставка → Завершен ...

Delivery Service :8080 ──WebClient──→ Order Service :8081  (GET /api/orders/{id})
```

## Сборка и запуск (JDK 21, Maven)

```powershell
mvn package
# окно 1
java -jar order-service\target\order-service.jar
# окно 2
java -jar delivery-service\target\delivery-service.jar
# окно 3 — запросы
curl.exe -s http://localhost:8080/api/deliveries/14
curl.exe -s -N http://localhost:8080/api/deliveries/16/stream
java -jar http-client\target\http-client.jar all
```

Если кириллица в консоли Windows отображается неверно, выполните `chcp 65001`.
Тела запросов POST/PUT лежат в папке `requests`.

## API Delivery Service

| Метод | Путь | Тип | Описание |
|---|---|---|---|
| GET | /api/deliveries?status=&darkstoreId= | Flux&lt;Delivery&gt; | список, фильтр по статусу и даркстору |
| GET | /api/deliveries/{id} | Mono&lt;Delivery&gt; | одна доставка, 404 если нет |
| POST | /api/deliveries | Mono&lt;Delivery&gt; | создать (201 + Location), 400 при неверном составе |
| PUT | /api/deliveries/{id} | Mono&lt;Delivery&gt; | заменить; смена статуса уходит в SSE-поток |
| DELETE | /api/deliveries/{id} | Mono&lt;Void&gt; | удалить (204), 404 если нет |
| GET | /api/deliveries/{id}/stream?simulate=true | Flux&lt;ServerSentEvent&gt; | поток изменений статуса (SSE) |
| GET | /api/deliveries/{id}/order | Mono&lt;DeliveryDetails&gt; | доставка + заказ из Order Service (WebClient) |

`simulate=true` (по умолчанию) — сервер сам продвигает доставку по этапам каждые 700 мс, как `delivery.track` в ПР4;
`simulate=false` — в поток попадают только изменения, сделанные запросами PUT.

Операторы Reactor: `map`, `filter`, `flatMap`, `switchIfEmpty`, `onErrorResume`, `concatWith`, `mergeWith`,
`takeUntil`, `collectMap`, `timeout`, `delayElement`, `Sinks.Many` (шина событий для SSE). `block()` в сервисах не используется.

Ошибки: отсутствие объекта — пустой `Mono` → `switchIfEmpty(Mono.error(NotFoundException))` → 404;
неверные данные — `ValidationException` → 400; в SSE ошибка передаётся событием `event:error`;
404 от Order Service → пустой `Mono` → `switchIfEmpty`; Order Service недоступен → `onErrorResume`, доставка отдаётся без заказа.

## Те же сценарии, что в ПР4

| ПР4, RSocket | ПР7, WebFlux |
|---|---|
| Request-Response `delivery.get` {id:14} → `Mono<DeliveryView>` | `GET /api/deliveries/14` → `Mono<Delivery>` |
| Request-Stream `delivery.track` {id:16} → `Flux<StatusUpdate>` | `GET /api/deliveries/16/stream` (SSE) → `Flux<StatusUpdate>` |
| Fire-and-Forget `delivery.event` | прямого аналога нет: ближайшее — `PUT`, но HTTP всегда возвращает ответ |
| Channel `goodslist.check` | прямого аналога нет: HTTP-запрос — один запрос и один ответ (поток только от сервера) |

`http-client` — консольный клиент на WebClient, повторяющий режимы `rr`/`stream` RSocket-клиента ПР4 (`get`, `stream`, `all`).

## Чем WebFlux отличается от RSocket

1. **Протокол.** WebFlux — веб-фреймворк поверх HTTP/1.1: адрес операции — URL и метод, данные — тело запроса и заголовки.
   RSocket — отдельный бинарный протокол прикладного уровня поверх TCP/WebSocket: маршрут передаётся в metadata кадра.
2. **Модели взаимодействия.** В HTTP есть только «запрос → ответ». Поток от сервера делается поверх него (SSE, NDJSON),
   а Fire-and-Forget и Channel (двунаправленный поток) частью HTTP API не являются. В RSocket все четыре модели встроены в протокол.
3. **Соединение.** HTTP-запрос самостоятелен (без состояния), каждый SSE-поток — отдельный HTTP-ответ.
   RSocket держит одно постоянное соединение, по которому мультиплексируются все запросы и потоки, и обе стороны могут быть инициатором.
4. **Обратное давление.** В WebFlux `request(n)` действует внутри приложения, а между клиентом и сервером его
   ограничивают только TCP-буферы; клиент может лишь закрыть соединение. В RSocket `request(n)` и `cancel` передаются по сети кадрами,
   поэтому сервер в ПР4 прекратил отправку сразу после `take(2)`.
5. **Совместимость.** Ответ WebFlux понимает любой HTTP-клиент — curl, браузер (`EventSource`), прокси и балансировщик (Traefik из ПР5);
   для RSocket нужна клиентская библиотека RSocket. Общее у них только Project Reactor: и там и там результат — `Mono`/`Flux`.
