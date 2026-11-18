package ru.pirksp.pr7.delivery.model;

/** Ответ об ошибке. */
public record ErrorInfo(int status, String error, String message, String path) {
}
