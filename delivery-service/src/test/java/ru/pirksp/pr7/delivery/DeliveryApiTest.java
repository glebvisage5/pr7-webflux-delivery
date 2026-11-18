package ru.pirksp.pr7.delivery;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;
import ru.pirksp.pr7.delivery.model.Delivery;

import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"app.courier-step=50ms", "app.order-service.url=http://localhost:1"})
@AutoConfigureWebTestClient
class DeliveryApiTest {

    @Autowired
    WebTestClient web;

    @Test
    void getOneReturnsMonoAndMissingReturns404() {
        web.get().uri("/api/deliveries/14").exchange()
                .expectStatus().isOk()
                .expectBody(Delivery.class).value(d -> {
                    assert d.id() == 14;
                    assert d.goods().size() == 2;
                });
        web.get().uri("/api/deliveries/77").exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.message").isEqualTo("Доставка с id=77 не найдена");
    }

    @Test
    void filterByDarkstore() {
        web.get().uri("/api/deliveries?darkstoreId=3").exchange()
                .expectStatus().isOk()
                .expectBodyList(Delivery.class).hasSize(2);
    }

    @Test
    void createWithUnknownGoodIs400() {
        web.post().uri("/api/deliveries").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("darkstoreId", 1, "goods", java.util.List.of(Map.of("goodId", 42, "count", 1))))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void sseStreamEmitsStatusesUntilFinal() {
        var events = web.get().uri("/api/deliveries/16/stream")
                .accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<Map<String, Object>>>() {
                })
                .getResponseBody()
                .map(e -> e.data().get("status"));
        StepVerifier.create(events)
                .expectNext("SUBMITTED", "ASSEMBLY", "DELIVERY", "COMPLETED")
                .verifyComplete();
    }

    @Test
    void orderServiceDownFallsBackWithoutError() {
        web.get().uri("/api/deliveries/14/order").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.deliveryId").isEqualTo(14)
                .jsonPath("$.customer").doesNotExist();
    }
}
