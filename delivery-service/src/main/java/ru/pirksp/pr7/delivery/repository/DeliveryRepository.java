package ru.pirksp.pr7.delivery.repository;

import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.pirksp.pr7.delivery.model.Darkstore;
import ru.pirksp.pr7.delivery.model.Delivery;
import ru.pirksp.pr7.delivery.model.Good;
import ru.pirksp.pr7.delivery.model.GoodsListRecord;
import ru.pirksp.pr7.delivery.model.Status;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Реактивное хранилище в памяти: те же даркстор, товары и доставки 11–17, что и в RSocket-сервере ПР4.
 * Все методы возвращают Mono/Flux; отсутствие объекта — пустой Mono, а не null и не исключение.
 */
@Repository
public class DeliveryRepository {

    /** Строка таблицы delivery вместе с составом goods_list. */
    public record DeliveryRow(int id, Long orderId, LocalDateTime datetime, int darkstoreId, Status status,
                              Map<Integer, Integer> goods) {

        public DeliveryRow withStatus(Status newStatus) {
            return new DeliveryRow(id, orderId, datetime, darkstoreId, newStatus, goods);
        }
    }

    private final Map<Integer, Darkstore> darkstores = new LinkedHashMap<>();
    private final Map<Integer, Good> goods = new LinkedHashMap<>();
    private final Map<Integer, DeliveryRow> deliveries = new ConcurrentSkipListMap<>();
    private final AtomicInteger sequence = new AtomicInteger(17);

    public DeliveryRepository() {
        darkstores.put(1, new Darkstore(1, "Даркстор Ленина, 10"));
        darkstores.put(2, new Darkstore(2, "Даркстор Мира, 25"));
        darkstores.put(3, new Darkstore(3, "Даркстор Садовая, 7"));

        addGood(1, "Яблочный пирог", 350.00);
        addGood(2, "Суп \"Минестроне\"", 280.50);
        addGood(3, "Салат \"Цезарь\"", 420.00);
        addGood(4, "Морс клюквенный", 150.00);
        addGood(5, "Блины с творогом", 310.00);
        addGood(6, "Котлета по-киевски", 390.00);
        addGood(7, "Сырники со сметаной", 260.00);

        // id, заказ в Order Service, даркстор, время, статус, пары «товар, количество» — данные ПР4
        addDelivery(11, 1L, 1, "2026-11-18T09:30", Status.COMPLETED, 1, 20, 4, 40);
        addDelivery(12, 5L, 2, "2026-11-18T10:15", Status.CANCELLED, 3, 5);
        addDelivery(13, 6L, 2, "2026-11-18T11:00", Status.COMPLETED, 5, 25);
        addDelivery(14, 2L, 3, "2026-11-18T12:40", Status.ASSEMBLY, 6, 30, 2, 12);
        addDelivery(15, 3L, 1, "2026-11-18T14:20", Status.SUBMITTED, 3, 10, 7, 15);
        addDelivery(16, 4L, 2, "2026-11-18T15:05", Status.SUBMITTED, 4, 60, 1, 8);
        addDelivery(17, 9L, 3, "2026-11-18T16:30", Status.DELIVERY, 1, 8, 5, 12);
    }

    private void addGood(int id, String name, double price) {
        goods.put(id, new Good(id, name, price));
    }

    private void addDelivery(int id, Long orderId, int darkstoreId, String datetime, Status status, int... pairs) {
        Map<Integer, Integer> list = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            list.put(pairs[i], pairs[i + 1]);
        }
        deliveries.put(id, new DeliveryRow(id, orderId, LocalDateTime.parse(datetime), darkstoreId, status, list));
    }

    // ---------- чтение ----------

    public Flux<Delivery> findAll() {
        return Flux.fromIterable(deliveries.values()).map(this::toDelivery);
    }

    public Mono<Delivery> findById(int id) {
        return Mono.fromSupplier(() -> deliveries.get(id)).map(this::toDelivery);
    }

    public Mono<DeliveryRow> findRow(int id) {
        return Mono.fromSupplier(() -> deliveries.get(id));
    }

    public Mono<Good> findGood(int id) {
        return Mono.justOrEmpty(goods.get(id));
    }

    public Mono<Darkstore> findDarkstore(int id) {
        return Mono.justOrEmpty(darkstores.get(id));
    }

    // ---------- запись ----------

    public Mono<Integer> nextId() {
        return Mono.fromSupplier(sequence::incrementAndGet);
    }

    public Mono<Delivery> save(DeliveryRow row) {
        return Mono.fromSupplier(() -> {
            deliveries.put(row.id(), row);
            return toDelivery(row);
        });
    }

    /** true — доставка была и удалена, false — удалять было нечего. */
    public Mono<Boolean> deleteById(int id) {
        return Mono.fromSupplier(() -> deliveries.remove(id) != null);
    }

    // ---------- сборка объекта Delivery из строк таблиц ----------

    private Delivery toDelivery(DeliveryRow row) {
        Darkstore darkstore = darkstores.get(row.darkstoreId());
        List<GoodsListRecord> records = new ArrayList<>();
        row.goods().forEach((goodId, count) -> {
            Good good = goods.get(goodId);
            records.add(new GoodsListRecord(good.id(), good.name(), good.price(), count));
        });
        double total = records.stream().mapToDouble(GoodsListRecord::sum).sum();
        return new Delivery(row.id(), row.orderId(), row.datetime(), darkstore.address(), darkstore.id(),
                row.status(), row.status().rus(), List.copyOf(records), total);
    }
}
