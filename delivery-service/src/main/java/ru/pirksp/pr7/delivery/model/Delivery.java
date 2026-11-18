package ru.pirksp.pr7.delivery.model;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Доставка. Поля совпадают с DeliveryView из RSocket-сервера ПР4 (строка Delivery.tsx + состав GoodsList.tsx),
 * добавлен только orderId — ссылка на заказ в Order Service.
 */
public record Delivery(
        int id,
        Long orderId,
        LocalDateTime deliveryDatetime,
        String address,
        int darkstoreId,
        Status status,
        String statusRus,
        List<GoodsListRecord> goods,
        double total) {
}
