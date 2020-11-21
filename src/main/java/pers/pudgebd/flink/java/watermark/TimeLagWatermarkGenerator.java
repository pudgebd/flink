package pers.pudgebd.flink.java.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import pers.pudgebd.flink.java.pojo.GrPo;

/**
 * This generator generates watermarks that are lagging behind processing time by a fixed amount.
 * It assumes that elements arrive in Flink after a bounded delay.
 */
public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<GrPo> {

    private final long maxTimeLag = 2000; // 5 seconds

    @Override
    public long extractTimestamp(GrPo element, long previousElementTimestamp) {
        return System.currentTimeMillis();
//        return element.getTs();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}