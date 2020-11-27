package pers.pudgebd.flink.java.func;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.TemporalField;

public class BigintToTimestamp extends ScalarFunction {


    public Timestamp eval(Long num) {
        if (num == null) {
            return new Timestamp(System.currentTimeMillis());
        }
        int len = String.valueOf(num)
                .length();
        if (len == 10) {

        } else {

        }
        return null;
    }

    public static Long getmicTime() {
        Long cutime = System.currentTimeMillis() * 1000; // 微秒
        Long nanoTime = System.nanoTime(); // 纳秒
        return cutime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
    }

}
