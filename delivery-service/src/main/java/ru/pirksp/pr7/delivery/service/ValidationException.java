package ru.pirksp.pr7.delivery.service;

/** Некорректные данные запроса → HTTP 400. */
public class ValidationException extends RuntimeException {
    public ValidationException(String message) {
        super(message);
    }
}
