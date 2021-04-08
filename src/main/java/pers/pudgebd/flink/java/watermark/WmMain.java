package pers.pudgebd.flink.java.watermark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pers.pudgebd.flink.java.pojo.StockOrderPo;

public class WmMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "50100-50200");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createTable(tableEnv);
        Table table = tableEnv.from("kafka_stock_order");
        DataStream<StockOrderPo> ds = tableEnv.toAppendStream(table, StockOrderPo.class);

        SingleOutputStreamOperator<StockOrderPo> soso =
                ds.assignTimestampsAndWatermarks(new RowBoundedWaterMark(Time.seconds(2)))
                        .keyBy(po -> po.getSec_code())
                        .timeWindow(Time.seconds(10))
                        .sum("order_vol");

        soso.print();
        streamEnv.execute("asd");
    }

    private static void createTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table kafka_stock_order(\n" +
                "    acct_id string COMMENT '投资者账户代码',\n" +
                "    sec_code string comment '产品代码',\n" +
                "    order_vol bigint,\n" +
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
