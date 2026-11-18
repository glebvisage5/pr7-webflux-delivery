package ru.pirksp.pr7.client;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Типизированный HTTP-клиент на WebClient — замена SupplierClient из ПР4.
 * Было: socket.requestResponse(payload) / socket.requestStream(payload) с маршрутом в metadata.
 * Стало: обычный HTTP GET по URL; одиночный ответ — Mono, поток SSE — Flux.
 */
public class DeliveryHttpClient {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE =
            new ParameterizedTypeReference<>() {
            };

    private final WebClient client;

    public DeliveryHttpClient(String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    /** RSocket Request-Response delivery.get ↔ GET /api/deliveries/{id} → Mono. */
    public Mono<ResponseEntity<Model.Delivery>> delivery(int id) {
        return client.get()
                .uri("/api/deliveries/{id}", id)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toEntity(Model.Delivery.class)
                .timeout(TIMEOUT);
    }

    /** RSocket Request-Stream delivery.track ↔ GET /api/deliveries/{id}/stream (text/event-stream) → Flux. */
    public Flux<ServerSentEvent<String>> track(int id) {
        return client.get()
                .uri("/api/deliveries/{id}/stream", id)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(SSE);
    }
}
