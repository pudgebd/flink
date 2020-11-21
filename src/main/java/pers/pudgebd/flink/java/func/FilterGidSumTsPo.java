package pers.pudgebd.flink.java.func;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import pers.pudgebd.flink.java.pojo.GidSumTsPo;

public class FilterGidSumTsPo implements FilterFunction<Tuple2<Boolean, GidSumTsPo>> {

    @Override
    public boolean filter(Tuple2<Boolean, GidSumTsPo> value) throws Exception {
        return value.f0;
    }

}
