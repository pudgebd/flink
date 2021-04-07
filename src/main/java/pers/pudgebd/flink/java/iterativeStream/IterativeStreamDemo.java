package pers.pudgebd.flink.java.iterativeStream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pers.pudgebd.flink.java.pojo.StockOrderPo;

public class IterativeStreamDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "50100-50200");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStream<Long> someIntegers = streamEnv.generateSequence(0, 10);
        // 创建迭代流
        IterativeStream<Long> iteration = someIntegers.iterate();
        // 增加处理逻辑，对元素执行减一操作。
        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });

        // 获取要进行迭代的流，
        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        // 对需要迭代的流形成一个闭环
        iteration.closeWith(stillGreaterThanZero);

        //TODO 自注：这一步好像没什么用
        //小于等于0的数据继续向前传输。
//        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long value) throws Exception {
//                return (value <= 0);
//            }
//        });

        iteration.print();
        streamEnv.execute("asd");
//        createTable(tableEnv);
//        Table table = tableEnv.from("kafka_stock_order");
//        DataStream<StockOrderPo> ds = tableEnv.toAppendStream(table, StockOrderPo.class);
//        IterativeStream<StockOrderPo> is = ds.iterate();
//        is.
    }

    private static void createTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table kafka_stock_order(\n" +
                "    acct_id string COMMENT '投资者账户代码',\n" +
                "    sec_code string comment '产品代码',\n" +
                "    ts bigint COMMENT '订单接收时间，微妙级时间戳'" +
                ") \n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order',\n" +
                " 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',\n" +
                " 'properties.group.id' = 'stock_order_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL',\n" +
                " 'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                " 'properties.sasl.mechanism' = 'GSSAPI',\n" +
                " 'properties.sasl.kerberos.service.name' = 'kafka',\n" +
                " 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab=\"/home/work/work.keytab\" principal=\"work@HAIZHI.COM\";'\n" +
                ")");
    }


}
