package pers.pudgebd.flink.java.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.constants.KeyScala;
import pers.pudgebd.flink.java.pojo.GrPo;

public class MapStrToGrPo implements MapFunction<String, GrPo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapStrToGrPo.class);

    @Override
    public GrPo map(String strVal) throws Exception {
        String[] partsArr = strVal.split(KeyScala.KAFKA_SEP());
        try {
            if (partsArr.length == 8) {
                return new GrPo(
                        Double.parseDouble(partsArr[0]), Double.parseDouble(partsArr[1]),
                        Double.parseDouble(partsArr[2]), Double.parseDouble(partsArr[3]),
                        Double.parseDouble(partsArr[4]), Double.parseDouble(partsArr[5]),
                        Double.parseDouble(partsArr[6]), Long.parseLong(partsArr[7])
                );
            } else {
                LOGGER.warn("strVal: $strVal 's partsArr.length:" + partsArr.length + " must be 8, so ignore this ConsumerRecord");
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return new GrPo();
    }

}
