package pers.pudgebd.flink.java.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.pojo.GidSumPricePo;
import pers.pudgebd.flink.java.utils.JedisPoolUtils;
import redis.clients.jedis.Jedis;

public class MyRedisSink extends RichSinkFunction<GidSumPricePo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyRedisSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


    @Override
    public void invoke(GidSumPricePo po, Context context) throws Exception {
        Jedis jedis = null;
        try {
            jedis = JedisPoolUtils.getJedis();
            jedis.lpush("test_list", JSON.toJSONString(po));
            jedis.expire("test_list", 5000);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            JedisPoolUtils.close(jedis);
        }
    }
}
