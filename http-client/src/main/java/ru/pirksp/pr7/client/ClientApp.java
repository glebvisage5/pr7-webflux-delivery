package ru.pirksp.pr7.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.List;

/**
 * Консольный клиент Delivery Service. Повторяет два сценария RSocket-клиента ПР4, но по HTTP:
 * get    — объект по id (Request-Response ↔ HTTP GET + Mono);
 * stream — поток обновлений статуса (Request-Stream ↔ HTTP/SSE + Flux);
 * all    — оба сценария (по умолчанию).
 * block() используется только здесь, в main консольного клиента, чтобы дождаться результата перед выходом.
 */
public class ClientApp {

    static final String BASE_URL = System.getProperty("baseUrl", "http://localhost:8080");
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new JavaTimeModule());

    public static void main(String[] args) {
        System.setProperty("reactor.netty.native", "false");
        String mode = args.length > 0 ? args[0] : "all";
        DeliveryHttpClient client = new DeliveryHttpClient(BASE_URL);
        Log.out("HTTP-клиент WebClient, сервер " + BASE_URL + " (отдельного соединения/сессии нет)");
        switch (mode) {
            case "get" -> get(client);
            case "stream" -> stream(client);
            case "all" -> {
                get(client);
                stream(client);
            }
            default -> Log.plain("Режимы: get | stream | all");
        }
    }

    /** HTTP GET + Mono: одна доставка; запрос несуществующей доставки. */
    static void get(DeliveryHttpClient client) {
        Log.title("HTTP GET + Mono: доставка по идентификатору (GET /api/deliveries/14)");
        long start = System.nanoTime();
        client.delivery(14)
                .doOnNext(r -> Log.out("ответ HTTP " + r.getStatusCode().value() + ", Content-Type: "
                        + r.getHeaders().getContentType() + ", " + ms(start) + " мс"))
                .mapNotNull(r -> r.getBody())
                .doOnNext(ClientApp::printDelivery)
                .block();

        Log.title("HTTP GET + Mono: несуществующая доставка (GET /api/deliveries/77)");
        Model.Delivery fallback = client.delivery(77)
                .mapNotNull(r -> r.getBody())
                .onErrorResume(WebClientResponseException.class, e -> {
                    Model.ErrorInfo error = e.getResponseBodyAs(Model.ErrorInfo.class);
                    Log.out("ответ HTTP " + e.getStatusCode().value() + ": "
                            + (error != null ? error.message() : e.getMessage()) + " → onErrorResume");
                    return Mono.just(emptyDelivery(77));
                })
                .onErrorResume(WebClientRequestException.class, e -> {
                    Log.out("сервер недоступен: " + e.getMessage());
                    return Mono.just(emptyDelivery(77));
                })
                .block();
        Log.out("показана пустая карточка: " + fallback.statusRus() + ", позиций: " + fallback.goods().size());
    }

    /** HTTP/SSE + Flux: обновления статуса приходят по одному, по мере появления на сервере. */
    static void stream(DeliveryHttpClient client) {
        Log.title("HTTP/SSE + Flux: отслеживание статуса доставки #16 (GET /api/deliveries/16/stream)");
        Long count = client.track(16)
                .doOnSubscribe(s -> Log.out("подписка на text/event-stream, ждём события..."))
                .elapsed()
                .doOnNext(t -> printEvent(t))
                .count()
                .block();
        Log.out("onComplete: сервер закрыл поток, получено событий: " + count);

        Log.title("HTTP/SSE + Flux: отслеживание несуществующей доставки #77");
        client.track(77)
                .filter(e -> "error".equals(e.event()))
                .map(e -> read(e.data(), Model.ErrorInfo.class))
                .doOnNext(e -> Log.out("event: error → " + e.status() + " " + e.message()))
                .onErrorResume(e -> {
                    Log.out("onError: " + e.getMessage());
                    return Flux.empty();
                })
                .blockLast();
        Log.out("поток завершён без падения клиента");
    }

    private static void printEvent(Tuple2<Long, ServerSentEvent<String>> t) {
        ServerSentEvent<String> e = t.getT2();
        Model.StatusUpdate u = read(e.data(), Model.StatusUpdate.class);
        Log.out(String.format("id:%-3s event:%s  доставка #%d (%s): %-9s (+%d мс)",
                e.id(), e.event(), u.deliveryId(), u.address(), u.statusRus(), t.getT1()));
    }

    private static <T> T read(String json, Class<T> type) {
        try {
            return JSON.readValue(json, type);
        } catch (Exception ex) {
            throw new IllegalStateException("некорректный JSON: " + json, ex);
        }
    }

    private static void printDelivery(Model.Delivery d) {
        Log.out("доставка #" + d.id() + ": " + d.address() + ", " + d.deliveryDatetime().toLocalDate()
                + " " + d.deliveryDatetime().toLocalTime() + ", статус «" + d.statusRus() + "»");
        d.goods().forEach(g -> Log.plain(String.format("                - %-20s %4d шт x %9s = %9s",
                g.goodName(), g.count(), Log.rub(g.goodPrice()), Log.rub(g.sum()))));
        Log.plain(String.format("                  Итого: %s", Log.rub(d.total())));
    }

    private static Model.Delivery emptyDelivery(int id) {
        return new Model.Delivery(id, null, null, "—", 0, null, "нет данных", List.of(), 0);
    }

    private static long ms(long start) {
        return (System.nanoTime() - start) / 1_000_000;
    }
}
