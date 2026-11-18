package ru.pirksp.pr7.client;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Вывод в консоль с временем (как в ПР4). */
public final class Log {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Log() {
    }

    public static synchronized void out(String message) {
        System.out.println(LocalTime.now().format(TIME) + "  " + message);
    }

    public static synchronized void plain(String message) {
        System.out.println(message);
    }

    public static synchronized void title(String title) {
        System.out.println();
        System.out.println("=== " + title + " ===");
    }

    public static String rub(double value) {
        String pattern = value == Math.rint(value) ? "%,.0f ₽" : "%,.2f ₽";
        return String.format(Locale.ROOT, pattern, value).replace(',', ' ');
    }
}
