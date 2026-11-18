package ru.pirksp.pr7.delivery.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.codec.DecodingException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;
import ru.pirksp.pr7.delivery.model.ErrorInfo;
import ru.pirksp.pr7.delivery.service.NotFoundException;
import ru.pirksp.pr7.delivery.service.ValidationException;

/** Сигнал onError из реактивной цепочки превращается в HTTP-ответ с телом ErrorInfo. */
@RestControllerAdvice
public class ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    @ExceptionHandler(NotFoundException.class)
    public Mono<ResponseEntity<ErrorInfo>> notFound(NotFoundException e, ServerWebExchange exchange) {
        return reply(HttpStatus.NOT_FOUND, e.getMessage(), exchange);
    }

    @ExceptionHandler(ValidationException.class)
    public Mono<ResponseEntity<ErrorInfo>> badRequest(ValidationException e, ServerWebExchange exchange) {
        return reply(HttpStatus.BAD_REQUEST, e.getMessage(), exchange);
    }

    @ExceptionHandler({ServerWebInputException.class, DecodingException.class})
    public Mono<ResponseEntity<ErrorInfo>> badInput(Exception e, ServerWebExchange exchange) {
        return reply(HttpStatus.BAD_REQUEST, "некорректный запрос: " + e.getMessage(), exchange);
    }

    private Mono<ResponseEntity<ErrorInfo>> reply(HttpStatus status, String message, ServerWebExchange exchange) {
        String path = exchange.getRequest().getPath().value();
        log.warn("  ✗ {} {}: {}", status.value(), status.getReasonPhrase(), message);
        return Mono.just(ResponseEntity.status(status)
                .body(new ErrorInfo(status.value(), status.getReasonPhrase(), message, path)));
    }
}
