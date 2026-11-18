package ru.pirksp.pr7.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

/** Order Service: хранит заказы АС сервиса доставки (данные ПР5), к нему обращается Delivery Service через WebClient. */
@SpringBootApplication
public class OrderServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceApplication.class);

    public static void main(String[] args) {
        // транспорт Java NIO, как на Windows (на Linux Reactor Netty иначе выбирает epoll)
        System.setProperty("reactor.netty.native", "false");
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void ready() {
        log.info("Order Service (Spring WebFlux) запущен: GET /api/orders, GET /api/orders/{id}");
    }

    @EventListener(ContextClosedEvent.class)
    public void stopped() {
        log.info("Order Service останавливается (POST /actuator/shutdown)");
    }
}
