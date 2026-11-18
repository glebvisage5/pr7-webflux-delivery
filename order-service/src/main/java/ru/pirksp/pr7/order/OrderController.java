package ru.pirksp.pr7.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** REST API заказов (реактивный). */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    /** Имитация обращения к БД: ответ приходит не сразу, поток при этом не блокируется. */
    private static final Duration DB_LATENCY = Duration.ofMillis(150);

    private final OrderRepository repository;

    public OrderController(OrderRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public Flux<Dto.Order> all() {
        log.info("GET /api/orders");
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Object>> one(@PathVariable long id) {
        log.info("GET /api/orders/{}", id);
        return repository.findById(id)
                .delayElement(DB_LATENCY)
                .doOnNext(o -> log.info("  → заказ {} ({}), клиент {}", o.id(), o.status(), o.customer().name()))
                .<ResponseEntity<Object>>map(ResponseEntity::ok)
                .switchIfEmpty(Mono.fromSupplier(() -> {
                    log.warn("  → заказ {} не найден, 404", id);
                    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new Dto.ErrorInfo(404, "Not Found",
                            "Заказ с id=" + id + " не найден", "/api/orders/" + id));
                }));
    }
}
