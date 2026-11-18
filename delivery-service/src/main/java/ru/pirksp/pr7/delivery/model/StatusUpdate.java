package ru.pirksp.pr7.delivery.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalTime;

/** Событие изменения статуса доставки — тот же объект, что передавал поток delivery.track в ПР4. */
public record StatusUpdate(int deliveryId, String address, Status status, String statusRus,
                           @JsonFormat(pattern = "HH:mm:ss.SSS") LocalTime time) {

    public static StatusUpdate of(Delivery d) {
        return new StatusUpdate(d.id(), d.address(), d.status(), d.statusRus(), LocalTime.now());
    }
}
