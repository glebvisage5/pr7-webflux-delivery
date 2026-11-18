package ru.pirksp.pr7.delivery.model;

/** Строка состава доставки (как в ПР4). */
public record GoodsListRecord(int goodId, String goodName, double goodPrice, int count) {

    public double sum() {
        return goodPrice * count;
    }
}
