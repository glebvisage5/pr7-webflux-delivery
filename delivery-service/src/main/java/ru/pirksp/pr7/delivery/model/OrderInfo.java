package ru.pirksp.pr7.delivery.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** Заказ в том виде, в котором его отдаёт Order Service (поля ПР5). */
public record OrderInfo(long id, Customer customer, long darkstoreId, String status, LocalDateTime createdAt,
                        List<Line> orderList, BigDecimal total) {

    public record Customer(long id, String name, String phone, String address) {
    }

    public record Line(long productId, String productName, BigDecimal price, int count, BigDecimal sum) {
    }
}
