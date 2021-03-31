package com.sg.gov.coviddata.util;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.concurrent.ThreadLocalRandom;

public class Util {

    public static LocalDate between(LocalDate startInclusive, LocalDate endExclusive) {
        long startEpochDay = startInclusive.toEpochDay();
        long endEpochDay = endExclusive.toEpochDay();
        long randomDay = ThreadLocalRandom
                .current()
                .nextLong(startEpochDay, endEpochDay);

        return LocalDate.ofEpochDay(randomDay);
    }

    public static LocalTime between(LocalTime startTime, LocalTime endTime) {
        int startSeconds = startTime.toSecondOfDay();
        int endSeconds = endTime.toSecondOfDay();
        int randomTime = ThreadLocalRandom
                .current()
                .nextInt(startSeconds, endSeconds);

        return LocalTime.ofSecondOfDay(randomTime);
    }

    public static LocalTime randomTime() {
        return between(LocalTime.MIN, LocalTime.MAX);
    }

    public static LocalDate getRandomBirthDt(){
        LocalDate start = LocalDate.of(1955, Month.JANUARY, 1);
        LocalDate end = LocalDate.of(2020, Month.JANUARY, 1);
        return between(start,end);
    }

    public static LocalDate getRandomDiagnosedDt(){
        LocalDate start = LocalDate.of(2021, Month.JANUARY, 1);
        LocalDate end = LocalDate.now();
        return between(start,end);
    }

}
