package pers.pudgebd.flink.java.watermark;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.pojo.IdNamePo;

public class BatchWindowBoundedWaterMark extends BoundedOutOfOrdernessTimestampExtractor<IdNamePo> {

    private final Logger LOG = LoggerFactory.getLogger(BatchWindowBoundedWaterMark.class);

    //这个参数的意思就是说，接受的数据延迟的时间最多是这个时间间隔，超过就丢弃
    public BatchWindowBoundedWaterMark(long maxOutOfOrder) {
        super(Time.seconds(maxOutOfOrder));
    }

    @Override
    public long extractTimestamp(IdNamePo element) {
        return element.ts;
    }
}
