package ru.pirksp.pr7.delivery.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import ru.pirksp.pr7.delivery.model.OrderInfo;

import java.time.Duration;

/** Реактивный HTTP-вызов внешнего сервиса: Delivery Service → Order Service через WebClient. */
@Component
public class OrderClient {

    private static final Logger log = LoggerFactory.getLogger(OrderClient.class);

    private final WebClient client;
    private final Duration timeout;

    public OrderClient(WebClient.Builder builder,
                       @Value("${app.order-service.url}") String baseUrl,
                       @Value("${app.order-service.timeout:2s}") Duration timeout) {
        this.client = builder.baseUrl(baseUrl).build();
        this.timeout = timeout;
    }

    /**
     * Заказ по id. 404 от Order Service превращается в пустой Mono (решение принимает вызывающий код
     * через switchIfEmpty), остальные ошибки — сигнал onError (их обрабатывает onErrorResume).
     */
    public Mono<OrderInfo> findOrder(long orderId) {
        return client.get()
                .uri("/api/orders/{id}", orderId)
                .retrieve()
                .bodyToMono(OrderInfo.class)
                .timeout(timeout)
                .doOnSubscribe(s -> log.info("  → order-service GET /api/orders/{}", orderId))
                .doOnNext(o -> log.info("  ← order-service: заказ {} ({}), клиент {}", o.id(), o.status(),
                        o.customer().name()))
                .onErrorResume(WebClientResponseException.NotFound.class, e -> {
                    log.info("  ← order-service: 404, заказ {} не найден → пустой Mono", orderId);
                    return Mono.empty();
                });
    }
}
