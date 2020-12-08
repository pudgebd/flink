package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.constants.FuncName;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdtaf;
import pers.pudgebd.flink.java.func.BigintToTimestamp;

import static org.apache.flink.table.api.Expressions.*;

public class JoinAndWindow_success_part2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createSth(tableEnv);
        Table kafka_stock_order = tableEnv.from("kafka_stock_order");

//        tableEnv.toAppendStream(kafka_stock_after_join_read, Row.class)
//                .print();
//        streamEnv.execute("a");

        //1.11.2 udtaf 不能在sql中使用
        Table aggTbl = kafka_stock_order.window(Tumble.over(lit(10).seconds()).on($("ts")).as("w"))
                .groupBy($("w"), $("sec_code"))
                .flatAggregate(
                        call(
                                FuncName.ALERT_SELF_BUY_SELL_UDTAF, $("order_type"), $("acct_id"),
                                $("trade_dir"), $("order_price"), $("order_vol")
                        ).as("alert_percent")
                )
                .select($("sec_code"), $("alert_percent"),
                        $("w").start().cast(DataTypes.TIMESTAMP(3)),
                        $("w").end().cast(DataTypes.DOUBLE()));
                //str TIMESTAMP
        tableEnv.toRetractStream(aggTbl, Row.class)
                .map(tp2 -> tp2.f1)
                .print();
        streamEnv.execute("a");
    }

    private static void createSth(StreamTableEnvironment tableEnv) {
        tableEnv.registerFunction(FuncName.BIGINT_TO_TS, new BigintToTimestamp());
        tableEnv.registerFunction(FuncName.ALERT_SELF_BUY_SELL_UDTAF, new AlertSelfBuySellUdtaf());

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
                "    ts_long bigint COMMENT '订单接收时间，微妙级时间戳',\n" +
                "    ts AS bigint_to_ts(ts_long),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS\n" +
                ") \n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_order_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");
    }

}
