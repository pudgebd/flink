package pers.pudgebd.flink.java.func;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;


public class BigintToTimestamp extends ScalarFunction {

    public Timestamp eval(String num) {
        return eval(
                Long.parseLong(num)
        );
    }

    public Timestamp eval(Long num) {
        if (num == null) {
            return new Timestamp(System.currentTimeMillis());
        }
        String str = String.valueOf(num);
        int len = str.length();
        if (len < 10) {
            return new Timestamp(System.currentTimeMillis());

        } else if (len == 10) {
            return new Timestamp(num * 1000);

        } else if (len == 13) {
            return new Timestamp(num);

        } else {
            return new Timestamp(
                    Long.parseLong(str.substring(0, 13))
            );
        }
    }

    public static Long getmicTime() {
        Long cutime = System.currentTimeMillis() * 1000; // 微秒
        Long nanoTime = System.nanoTime(); // 纳秒
        return cutime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
    }

}
