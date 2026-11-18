package ru.pirksp.pr7.order;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Объекты Order Service — те же, что в ПР5 (Customer, OrderList, Order). */
public final class Dto {
    private Dto() {
    }

    public record Customer(long id, String name, String phone, String address) {
    }

    public record OrderLine(long productId, String productName, BigDecimal price, int count, BigDecimal sum) {
    }

    /** Статус заказа: DRAFT, FIXED, CANCELED, ASSEMBLY, DELIVERY, COMPLETED. */
    public record Order(long id, Customer customer, long darkstoreId, String status, LocalDateTime createdAt,
                        List<OrderLine> orderList, BigDecimal total) {
    }

    public record ErrorInfo(int status, String error, String message, String path) {
    }
}
