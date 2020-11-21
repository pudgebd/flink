package pers.pudgebd.flink.java.watermark;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.Random;

/**
 * 不好用
 * 参考
 * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_timestamps_watermarks.html#timestamps-per-kafka-partition
 */
public class KafkaWatermark extends AscendingTimestampExtractor<String> {

    private Random random = new Random();

    @Override
    public long extractAscendingTimestamp(String element) {
        return System.currentTimeMillis() - random.nextInt(300);
    }

}
