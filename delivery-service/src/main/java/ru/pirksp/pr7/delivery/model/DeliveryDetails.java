package ru.pirksp.pr7.delivery.model;

import java.math.BigDecimal;

/**
 * Карточка доставки для курьера: данные доставки (Delivery Service) + клиент и адрес из заказа,
 * полученного у Order Service через WebClient. Если заказ получить не удалось, поля заказа не заполняются.
 */
public record DeliveryDetails(
        int deliveryId,
        String statusRus,
        String from,
        double total,
        Long orderId,
        String orderStatus,
        String customer,
        String phone,
        String to,
        BigDecimal orderTotal,
        String orderSource) {

    public static DeliveryDetails of(Delivery d, OrderInfo o) {
        return new DeliveryDetails(d.id(), d.statusRus(), d.address(), d.total(), o.id(), o.status(),
                o.customer().name(), o.customer().phone(), o.customer().address(), o.total(), "Order Service");
    }

    public static DeliveryDetails withoutOrder(Delivery d, String reason) {
        return new DeliveryDetails(d.id(), d.statusRus(), d.address(), d.total(), d.orderId(), null,
                null, null, null, null, reason);
    }
}
