package ru.pirksp.pr7.delivery.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import ru.pirksp.pr7.delivery.client.OrderClient;
import ru.pirksp.pr7.delivery.model.Delivery;
import ru.pirksp.pr7.delivery.model.DeliveryDetails;
import ru.pirksp.pr7.delivery.model.DeliveryRequest;
import ru.pirksp.pr7.delivery.model.Status;
import ru.pirksp.pr7.delivery.model.StatusUpdate;
import ru.pirksp.pr7.delivery.repository.DeliveryRepository;
import ru.pirksp.pr7.delivery.repository.DeliveryRepository.DeliveryRow;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Бизнес-логика доставок. Каждый метод строит реактивную цепочку и возвращает Mono/Flux;
 * block() нигде не вызывается — подписывается на цепочку сам WebFlux при обработке запроса.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryRepository repository;
    private final OrderClient orders;
    private final Duration courierStep;

    /** Шина событий об изменении статуса: в неё пишут PUT и «курьер», из неё читают SSE-подписчики. */
    private final Sinks.Many<StatusUpdate> events = Sinks.many().multicast().directBestEffort();

    public DeliveryService(DeliveryRepository repository, OrderClient orders,
                           @Value("${app.courier-step:700ms}") Duration courierStep) {
        this.repository = repository;
        this.orders = orders;
        this.courierStep = courierStep;
    }

    // ---------- чтение: Flux для списка, Mono для одного объекта ----------

    /** GET /api/deliveries — filter по статусу и даркстору. */
    public Flux<Delivery> findAll(Status status, Integer darkstoreId) {
        return repository.findAll()
                .filter(d -> status == null || d.status() == status)
                .filter(d -> darkstoreId == null || d.darkstoreId() == darkstoreId);
    }

    /** GET /api/deliveries/{id} — аналог RSocket Request-Response delivery.get. */
    public Mono<Delivery> findById(int id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(() -> notFound(id)));
    }

    // ---------- изменение ----------

    /** POST /api/deliveries. */
    public Mono<Delivery> create(DeliveryRequest request) {
        return validate(request)
                .flatMap(goods -> repository.nextId()
                        .map(id -> new DeliveryRow(id, request.orderId(),
                                request.deliveryDatetime() != null ? request.deliveryDatetime() : LocalDateTime.now().withNano(0),
                                request.darkstoreId(),
                                request.status() != null ? request.status() : Status.SUBMITTED,
                                goods)))
                .flatMap(repository::save)
                .doOnNext(d -> log.info("  создана доставка #{} ({}, позиций: {}, сумма {})",
                        d.id(), d.address(), d.goods().size(), d.total()));
    }

    /** PUT /api/deliveries/{id}: полная замена; если сменился статус — событие для SSE-подписчиков. */
    public Mono<Delivery> update(int id, DeliveryRequest request) {
        return repository.findRow(id)
                .switchIfEmpty(Mono.error(() -> notFound(id)))
                .flatMap(old -> validate(request)
                        .map(goods -> new DeliveryRow(id, request.orderId(),
                                request.deliveryDatetime() != null ? request.deliveryDatetime() : old.datetime(),
                                request.darkstoreId(),
                                request.status() != null ? request.status() : old.status(),
                                goods))
                        .flatMap(repository::save)
                        .doOnNext(d -> {
                            log.info("  доставка #{} обновлена: {} → {}", id, old.status().rus(), d.statusRus());
                            if (d.status() != old.status()) {
                                publish(StatusUpdate.of(d));
                            }
                        }));
    }

    /** DELETE /api/deliveries/{id}. */
    public Mono<Void> delete(int id) {
        return repository.deleteById(id)
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(() -> notFound(id)))
                .doOnNext(ok -> log.info("  доставка #{} удалена", id))
                .then();
    }

    /** Проверка тела запроса средствами Reactor: каждый товар ищется в хранилище через flatMap. */
    private Mono<Map<Integer, Integer>> validate(DeliveryRequest request) {
        if (request.darkstoreId() == null) {
            return Mono.error(new ValidationException("не указан darkstoreId"));
        }
        if (request.goods() == null || request.goods().isEmpty()) {
            return Mono.error(new ValidationException("состав доставки пуст"));
        }
        return repository.findDarkstore(request.darkstoreId())
                .switchIfEmpty(Mono.error(() -> new ValidationException(
                        "даркстор с id=" + request.darkstoreId() + " не существует")))
                .thenMany(Flux.fromIterable(request.goods()))
                .flatMap(line -> line.count() <= 0
                        ? Mono.error(new ValidationException("количество товара " + line.goodId() + " должно быть больше 0"))
                        : repository.findGood(line.goodId())
                        .switchIfEmpty(Mono.error(() -> new ValidationException(
                                "товар с id=" + line.goodId() + " не существует")))
                        .map(good -> line))
                .collectMap(DeliveryRequest.Line::goodId, DeliveryRequest.Line::count, LinkedHashMap::new);
    }

    // ---------- внешний вызов: Delivery Service → Order Service ----------

    /** GET /api/deliveries/{id}/order — доставка + заказ из Order Service (WebClient). */
    public Mono<DeliveryDetails> details(int id) {
        return findById(id)
                .flatMap(delivery -> Mono.justOrEmpty(delivery.orderId())
                        .flatMap(orders::findOrder)
                        .map(order -> DeliveryDetails.of(delivery, order))
                        // заказа нет (404 или orderId не указан) — отдаём доставку без заказа
                        .switchIfEmpty(Mono.fromSupplier(() -> DeliveryDetails.withoutOrder(delivery,
                                "заказ " + delivery.orderId() + " не найден в Order Service")))
                        // Order Service недоступен или не ответил вовремя — доставка всё равно возвращается
                        .onErrorResume(e -> {
                            log.warn("  Order Service: {} → onErrorResume, доставка #{} без заказа", e.getMessage(), id);
                            return Mono.just(DeliveryDetails.withoutOrder(delivery,
                                    "Order Service недоступен: " + e.getMessage()));
                        }));
    }

    // ---------- поток изменений статуса (SSE) ----------

    /**
     * Поток событий по доставке — аналог RSocket Request-Stream delivery.track.
     * Первым приходит текущий статус, затем каждое изменение по мере его появления (из шины событий),
     * поток завершается, когда доставка доходит до финального статуса.
     * simulate=true — сервер сам продвигает доставку по этапам с шагом courierStep (как в ПР4).
     */
    public Flux<StatusUpdate> track(int id, boolean simulate) {
        return findById(id).flatMapMany(start -> {
            log.info("  SSE: подписка на доставку #{} (текущий статус «{}», simulate={})", id, start.statusRus(), simulate);
            Flux<StatusUpdate> updates = Flux.just(StatusUpdate.of(start))
                    .concatWith(events.asFlux().filter(u -> u.deliveryId() == id))
                    .takeUntil(u -> u.status().isFinal());
            if (!simulate || start.status().isFinal()) {
                return updates;
            }
            Flux<StatusUpdate> courier = Flux.interval(courierStep, courierStep)
                    .concatMap(tick -> advance(id))
                    .takeUntil(u -> u.status().isFinal())
                    .ignoreElements()
                    .flux();
            return updates.mergeWith(courier);
        });
    }

    /** Один шаг «курьера»: статус переходит на следующий этап и публикуется в шину. */
    private Mono<StatusUpdate> advance(int id) {
        return repository.findRow(id)
                .switchIfEmpty(Mono.error(() -> notFound(id)))
                .map(row -> row.withStatus(row.status().next()))
                .flatMap(repository::save)
                .map(StatusUpdate::of)
                .doOnNext(this::publish);
    }

    private void publish(StatusUpdate update) {
        events.emitNext(update, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
    }

    private static NotFoundException notFound(int id) {
        return new NotFoundException("Доставка с id=" + id + " не найдена");
    }
}
