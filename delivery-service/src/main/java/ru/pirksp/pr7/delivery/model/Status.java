package ru.pirksp.pr7.delivery.model;

/** Статус доставки — тот же enum, что в RSocket-сервере ПР4 (Принят → В сборке → Доставка → Завершен). */
public enum Status {
    SUBMITTED("Принят"),
    ASSEMBLY("В сборке"),
    DELIVERY("Доставка"),
    COMPLETED("Завершен"),
    CANCELLED("Отменен");

    private final String rus;

    Status(String rus) {
        this.rus = rus;
    }

    public String rus() {
        return rus;
    }

    /** Следующий этап доставки (для имитации движения курьера в потоке SSE). */
    public Status next() {
        return switch (this) {
            case SUBMITTED -> ASSEMBLY;
            case ASSEMBLY -> DELIVERY;
            case DELIVERY, COMPLETED -> COMPLETED;
            case CANCELLED -> CANCELLED;
        };
    }

    public boolean isFinal() {
        return this == COMPLETED || this == CANCELLED;
    }
}
