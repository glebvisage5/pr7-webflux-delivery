package ru.pirksp.pr7.delivery.service;

/** Объект не найден → HTTP 404 (аналог NotFoundException RSocket-сервера ПР4). */
public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) {
        super(message);
    }
}
