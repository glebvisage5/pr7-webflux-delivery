package ru.pirksp.pr7.client;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

/** Объекты, которые клиент получает от Delivery Service (те же поля, что у RSocket-клиента ПР4). */
public final class Model {
    private Model() {
    }

    public record GoodsListRecord(int goodId, String goodName, double goodPrice, int count) {
        public double sum() {
            return goodPrice * count;
        }
    }

    public record Delivery(int id, Long orderId, LocalDateTime deliveryDatetime, String address, int darkstoreId,
                           String status, String statusRus, List<GoodsListRecord> goods, double total) {
    }

    public record StatusUpdate(int deliveryId, String address, String status, String statusRus, LocalTime time) {
    }

    public record ErrorInfo(int status, String error, String message, String path) {
    }
}
