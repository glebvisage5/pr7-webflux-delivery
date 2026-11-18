package ru.pirksp.pr7.delivery.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Обычные JSON-ответы выводятся с отступами (удобно читать в curl),
 * а данные SSE-событий — одной строкой: одно событие = одна строка data:.
 */
@Configuration
public class CodecConfig implements WebFluxConfigurer {

    private final ObjectMapper mapper;

    public CodecConfig(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void configureHttpMessageCodecs(ServerCodecConfigurer configurer) {
        ObjectMapper compact = mapper.copy().disable(SerializationFeature.INDENT_OUTPUT);
        configurer.defaultCodecs().serverSentEventEncoder(new Jackson2JsonEncoder(compact));
    }
}
