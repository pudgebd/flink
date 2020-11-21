package pers.pudgebd.flink.java.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import pers.pudgebd.flink.java.pojo.GrPo;

import java.util.Random;

/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<GrPo> {

    private final long maxOutOfOrderness = 3500L; // 3.5 seconds

    private long currentMaxTimestamp = 0L;

    private Random random = new Random();

    @Override
    public long extractTimestamp(GrPo element, long previousElementTimestamp) {
        //long timestamp = element.getTs();
        long timestamp = System.currentTimeMillis() - random.nextInt(300);
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        //TODO 下面的不行，要这样：return new Watermark(System.currentTimeMillis() - maxOutOfOrderness);
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}