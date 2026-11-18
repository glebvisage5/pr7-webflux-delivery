package ru.pirksp.pr7.delivery.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.pirksp.pr7.delivery.model.Delivery;
import ru.pirksp.pr7.delivery.model.DeliveryDetails;
import ru.pirksp.pr7.delivery.model.DeliveryRequest;
import ru.pirksp.pr7.delivery.model.ErrorInfo;
import ru.pirksp.pr7.delivery.model.Status;
import ru.pirksp.pr7.delivery.service.DeliveryService;
import ru.pirksp.pr7.delivery.service.NotFoundException;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Реактивный HTTP API доставок.
 * Один объект — Mono&lt;Delivery&gt;, несколько — Flux&lt;Delivery&gt;, поток изменений — SSE (Flux&lt;ServerSentEvent&gt;).
 */
@RestController
@RequestMapping("/api/deliveries")
public class DeliveryController {

    private static final Logger log = LoggerFactory.getLogger(DeliveryController.class);

    private final DeliveryService service;
    private final AtomicLong eventIds = new AtomicLong();

    public DeliveryController(DeliveryService service) {
        this.service = service;
    }

    @GetMapping
    public Flux<Delivery> all(@RequestParam(required = false) Status status,
                              @RequestParam(required = false) Integer darkstoreId) {
        log.info("GET /api/deliveries status={} darkstoreId={}", status, darkstoreId);
        return service.findAll(status, darkstoreId)
                .doOnComplete(() -> log.info("  → список отправлен"));
    }

    /** RSocket Request-Response (delivery.get) ↔ HTTP GET + Mono. */
    @GetMapping("/{id}")
    public Mono<Delivery> one(@PathVariable int id) {
        log.info("GET /api/deliveries/{}", id);
        return service.findById(id)
                .doOnNext(d -> log.info("  → доставка #{} ({}, {}, позиций: {})", d.id(), d.address(),
                        d.statusRus(), d.goods().size()));
    }

    @PostMapping
    public Mono<ResponseEntity<Delivery>> create(@RequestBody DeliveryRequest request) {
        log.info("POST /api/deliveries");
        return service.create(request)
                .map(d -> ResponseEntity.created(URI.create("/api/deliveries/" + d.id())).body(d));
    }

    @PutMapping("/{id}")
    public Mono<Delivery> update(@PathVariable int id, @RequestBody DeliveryRequest request) {
        log.info("PUT /api/deliveries/{}", id);
        return service.update(id, request);
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> delete(@PathVariable int id) {
        log.info("DELETE /api/deliveries/{}", id);
        return service.delete(id).then(Mono.just(ResponseEntity.noContent().build()));
    }

    /** Доставка + сведения о заказе из Order Service (WebClient). */
    @GetMapping("/{id}/order")
    public Mono<DeliveryDetails> details(@PathVariable int id) {
        log.info("GET /api/deliveries/{}/order", id);
        return service.details(id)
                .doOnNext(d -> log.info("  → доставка #{} + заказ: {}", id, d.orderSource()));
    }

    /**
     * RSocket Request-Stream (delivery.track) ↔ HTTP + Server-Sent Events + Flux.
     * Каждое изменение статуса уходит клиенту отдельным SSE-событием сразу, как только появилось.
     * Ошибка (нет доставки) передаётся последним событием event: error — соединение закрывается штатно.
     */
    @GetMapping(value = "/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> stream(@PathVariable int id,
                                                @RequestParam(defaultValue = "true") boolean simulate) {
        log.info("GET /api/deliveries/{}/stream (SSE)", id);
        return service.track(id, simulate)
                .map(u -> {
                    String eventId = String.valueOf(eventIds.incrementAndGet());
                    log.info("  SSE → id:{} event:status доставка #{}: {}", eventId, u.deliveryId(), u.statusRus());
                    return ServerSentEvent.builder()
                            .id(eventId)
                            .event("status")
                            .data((Object) u)
                            .build();
                })
                .onErrorResume(NotFoundException.class, e -> {
                    log.warn("  SSE → event:error {}", e.getMessage());
                    return Mono.just(ServerSentEvent.builder()
                            .event("error")
                            .data((Object) new ErrorInfo(404, "Not Found", e.getMessage(),
                                    "/api/deliveries/" + id + "/stream"))
                            .build());
                })
                .doOnComplete(() -> log.info("  SSE: поток доставки #{} завершён", id))
                .doOnCancel(() -> log.info("  SSE: клиент закрыл соединение (cancel)"));
    }
}
