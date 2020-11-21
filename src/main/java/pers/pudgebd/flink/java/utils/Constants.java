package pers.pudgebd.flink.java.utils;

import java.util.Properties;

public class Constants {

    public static Properties kafkaProp = new Properties();
    static {
        kafkaProp.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        kafkaProp.setProperty("zookeeper.connect", "localhost:2181");
        kafkaProp.setProperty("group.id", "test-consumer-group");
    }

}
