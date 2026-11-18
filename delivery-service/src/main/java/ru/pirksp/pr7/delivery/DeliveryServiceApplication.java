package ru.pirksp.pr7.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/** Delivery Service: реактивный HTTP API доставок АС Поставщиков на Spring WebFlux. */
@SpringBootApplication
public class DeliveryServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(DeliveryServiceApplication.class);

    @Value("${server.port}")
    private int port;

    @Value("${app.order-service.url}")
    private String orderServiceUrl;

    public static void main(String[] args) {
        // транспорт Java NIO, как на Windows (на Linux Reactor Netty иначе выбирает epoll)
        System.setProperty("reactor.netty.native", "false");
        SpringApplication.run(DeliveryServiceApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void ready() {
        log.info("Delivery Service (Spring WebFlux) запущен: http://localhost:{}", port);
        log.info("  Mono/Flux : GET|POST /api/deliveries, GET|PUT|DELETE /api/deliveries/{id}");
        log.info("  SSE       : GET /api/deliveries/{id}/stream   (text/event-stream)");
        log.info("  WebClient : GET /api/deliveries/{id}/order → {}", orderServiceUrl);
    }
}
