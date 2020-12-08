package pers.pudgebd.flink.java.task;

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.constants.FuncName;
import pers.pudgebd.flink.java.sql.CrtTblSqls;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class TmpMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);
        String sql = "select sec_code, min(order_price) as min_order_price, max(order_price) as max_order_price \n" +
                "from kafka_stock_order " +
                "group by TUMBLE(proctime, INTERVAL '10' SECONDS), sec_code";
        System.out.println(sql);
        Table tmp = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(tmp, Row.class)
                .print();
        streamEnv.execute("a");
    }

    public static void main1(String[] args) {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "50100-50200");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);


        tableEnv.executeSql("create table kafka_stock_order(\n" +
                "    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',\n" +
                "    acct_id string COMMENT '投资者账户代码',\n" +
                "    order_no bigint COMMENT '原始订单参考编号',\n" +
                "    sec_code string comment '产品代码',\n" +
                "    trade_dir string COMMENT '交易方向,B 或者 S',\n" +
                "    order_price bigint comment '交易价格，单位为分',\n" +
                "    order_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',\n" +
                "    act_no bigint COMMENT '订单确认顺序号',\n" +
                "    withdraw_order_no bigint COMMENT '撤单订单编号',\n" +
                "    pbu double COMMENT '发出此订单的报盘机编号',\n" +
                "    order_status string COMMENT '订单状态,0=New,1=Cancelled,2=Reject',\n" +
                "    ts_sql timestamp(3) COMMENT '订单接收时间，微妙级时间戳',\n" +
                "    ts_iso timestamp(3) COMMENT '订单接收时间，微妙级时间戳',\n" +
                "    ts_long bigint COMMENT '订单接收时间，微妙级时间戳',\n" +
                "    WATERMARK FOR ts_iso AS ts_iso - INTERVAL '5' SECONDS" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_order_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'ISO-8601'\n" +
                ")");

        tableEnv.executeSql("create table kafka_stock_order_group(\n" +
                "    sec_code string,\n" +
                "    sum_order_price bigint,\n" +
                "    window_start timestamp(3),\n" +
                "    window_end timestamp(3)\n" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order_group',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_order_group_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'ISO-8601'\n" +
                ")");

        tableEnv.executeSql("insert into kafka_stock_order_group " +
                "select sec_code, sum(order_price), " +
                "TUMBLE_START(ts_iso, INTERVAL '10' SECONDS) as window_start, " +
                "TUMBLE_END(ts_iso, INTERVAL '10' SECONDS) as window_end " +
                "from kafka_stock_order " +
                "group by TUMBLE(ts_iso, INTERVAL '10' SECONDS), sec_code");
    }


}
