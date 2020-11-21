package pers.pudgebd.flink.java.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.constants.KeyScala;
import pers.pudgebd.flink.java.pojo.GoodsDetailPo;

public class MapStrToGoodsDetail implements MapFunction<String, GoodsDetailPo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapStrToGoodsDetail.class);

    @Override
    public GoodsDetailPo map(String strVal) throws Exception {
        String[] partsArr = strVal.split(KeyScala.KAFKA_SEP());
        try {
            if (partsArr.length == 3) {
                return new GoodsDetailPo(
                        Double.parseDouble(partsArr[0]), partsArr[1],
                        Double.parseDouble(partsArr[2])
                );
            } else {
                LOGGER.warn("strVal: $strVal 's partsArr.length:" + partsArr.length + " must be 8, so ignore this ConsumerRecord");
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return new GoodsDetailPo();
    }

}
