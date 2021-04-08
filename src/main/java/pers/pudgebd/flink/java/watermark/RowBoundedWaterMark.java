package pers.pudgebd.flink.java.watermark;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.pojo.StockOrderPo;

public class RowBoundedWaterMark extends BoundedOutOfOrdernessTimestampExtractor<StockOrderPo> {

    public RowBoundedWaterMark(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(StockOrderPo element) {
        return element.getTs();
    }

}
