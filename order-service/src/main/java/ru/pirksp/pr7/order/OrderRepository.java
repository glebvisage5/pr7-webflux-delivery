package ru.pirksp.pr7.order;

import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Заказы в памяти сервиса (данные data.sql из ПР5 + два заказа для доставок 12 и 13). */
@Repository
public class OrderRepository {

    private record Product(long id, String name, BigDecimal price) {
    }

    private static final Map<Long, Product> PRODUCTS = Map.of(
            1L, new Product(1, "Яблоки", new BigDecimal("229.90")),
            2L, new Product(2, "Молоко 3,2%", new BigDecimal("94.50")),
            3L, new Product(3, "Хлеб бородинский", new BigDecimal("62.00")),
            4L, new Product(4, "Сыр российский", new BigDecimal("389.00")),
            5L, new Product(5, "Кофе молотый", new BigDecimal("549.00")));

    private static final Map<Long, Dto.Customer> CUSTOMERS = Map.of(
            1L, new Dto.Customer(1, "Иванов Пётр", "+7 900 111-22-33", "ул. Ленина, 12, кв. 5"),
            2L, new Dto.Customer(2, "Смирнова Анна", "+7 900 444-55-66", "пр. Мира, 48, кв. 101"),
            3L, new Dto.Customer(3, "Кузнецов Олег", "+7 900 777-88-99", "ул. Садовая, 3, кв. 17"));

    private final Map<Long, Dto.Order> orders = new TreeMap<>();

    public OrderRepository() {
        add(1, 1, 1, "COMPLETED", "2026-11-18T10:15", 1, 2, 3, 1);
        add(2, 2, 2, "ASSEMBLY", "2026-11-18T12:40", 2, 3, 4, 1, 5, 1);
        add(3, 3, 1, "FIXED", "2026-11-18T13:05", 1, 1, 2, 2, 3, 2);
        add(4, 1, 3, "DRAFT", "2026-11-18T13:30", 5, 2);
        add(5, 2, 2, "CANCELED", "2026-11-18T09:50", 4, 2);
        add(6, 3, 2, "COMPLETED", "2026-11-18T10:20", 2, 4, 3, 1);
    }

    private void add(long id, long customerId, long darkstoreId, String status, String createdAt, long... lines) {
        List<Dto.OrderLine> list = new ArrayList<>();
        for (int i = 0; i < lines.length; i += 2) {
            Product p = PRODUCTS.get(lines[i]);
            int count = (int) lines[i + 1];
            list.add(new Dto.OrderLine(p.id(), p.name(), p.price(), count, p.price().multiply(BigDecimal.valueOf(count))));
        }
        BigDecimal total = list.stream().map(Dto.OrderLine::sum).reduce(BigDecimal.ZERO, BigDecimal::add);
        orders.put(id, new Dto.Order(id, CUSTOMERS.get(customerId), darkstoreId, status,
                LocalDateTime.parse(createdAt), List.copyOf(list), total));
    }

    public Flux<Dto.Order> findAll() {
        return Flux.fromIterable(orders.values());
    }

    /** Пустой Mono, если заказа нет: решение о 404 принимает контроллер. */
    public Mono<Dto.Order> findById(long id) {
        return Mono.justOrEmpty(orders.get(id));
    }
}
