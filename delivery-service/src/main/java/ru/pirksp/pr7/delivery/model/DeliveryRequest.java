package ru.pirksp.pr7.delivery.model;

import java.time.LocalDateTime;
import java.util.List;

/** Тело запросов POST /api/deliveries и PUT /api/deliveries/{id}. */
public record DeliveryRequest(
        Long orderId,
        LocalDateTime deliveryDatetime,
        Integer darkstoreId,
        Status status,
        List<Line> goods) {

    /** Строка состава: товар и количество (как GoodsListLine в ПР4). */
    public record Line(int goodId, int count) {
    }
}
