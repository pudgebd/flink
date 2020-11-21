package pers.pudgebd.flink.java.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.constants.KeyScala;
import pers.pudgebd.flink.java.pojo.GrHivePo;

public class MapStrToGrHivePo implements MapFunction<String, GrHivePo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapStrToGrHivePo.class);

    @Override
    public GrHivePo map(String strVal) throws Exception {
        String[] partsArr = strVal.split(KeyScala.KAFKA_SEP());
        try {
            if (partsArr.length == 8) {
                return new GrHivePo(
                        Long.parseLong(partsArr[0]), partsArr[1],
                        Double.parseDouble(partsArr[2])
                );
            } else {
                LOGGER.warn("strVal: $strVal 's partsArr.length:" + partsArr.length + " must be 8, so ignore this ConsumerRecord");
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return new GrHivePo();
    }

}
