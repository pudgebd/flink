package pers.pudgebd.flink.java.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.pojo.GidSumPricePo;
import pers.pudgebd.flink.java.utils.JedisPoolUtils;
import redis.clients.jedis.Jedis;

public class MyRedisSinkTp2 extends RichSinkFunction<Tuple2<Boolean, GidSumPricePo>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyRedisSinkTp2.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


    @Override
    public void invoke(Tuple2<Boolean, GidSumPricePo> tp2, Context context) throws Exception {
        Jedis jedis = null;
        try {
            GidSumPricePo po = tp2.f1;
            if (po.getGoodsId() == null || po.getSumPrice() == null) {
                return;
            }
            jedis = JedisPoolUtils.getJedis();
            jedis.lpush("test_list", JSON.toJSONString(tp2.f1));
            jedis.expire("test_list", 5000);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            JedisPoolUtils.close(jedis);
        }
    }
}
