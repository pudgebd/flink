package pers.pudgebd.flink.java.joinAndWindow;

import com.haizhi.streamx.flinkcluster.platformapp.func.ArbitraryAggFunc;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.constants.FuncName;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdaf;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdtaf;
import pers.pudgebd.flink.java.func.BigintToTimestamp;
import pers.pudgebd.flink.java.func.OutputAllUdtaf;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class JoinAndWindow01_1 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);

        Table table = tableEnv.from("kafka_stock_order");
        Table selectTbl = table.window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
                .groupBy($("w"))
                .flatAggregate("output_all_udtaf(order_no, sec_code, order_type) as arr")
                .select($("arr"));

        tableEnv.createTemporaryView("selectTbl", selectTbl);
        Table tbl = tableEnv.sqlQuery("select arr[1] as order_no, arr[2] as sec_code, arr[3] as order_type from selectTbl");

        TypeInformation<?>[] types = new TypeInformation[3];
        String[] fieldNames = new String[3];
        types[0] = TypeInformation.of(new TypeHint<Long>() {});
        types[1] = TypeInformation.of(new TypeHint<String>() {});
        types[2] = TypeInformation.of(new TypeHint<Long>() {});
        fieldNames[0] = "order_no";
        fieldNames[1] = "sec_code";
        fieldNames[2] = "order_type";

        DataStream<Row> ds = tableEnv.toAppendStream(tbl, Row.class);
        SingleOutputStreamOperator<Row> sos = ds
                .map(tp2 -> tp2)
                .returns(new RowTypeInfo(types, fieldNames));
//        sos.print();
//        streamEnv.execute("a");
        tableEnv.createTemporaryView("view_stock_order_window_data", sos);
        Table afterJoin = tableEnv.sqlQuery("select o.sec_code, " +
                "alert_self_buy_sell(o.order_type, c.acct_id, c.trade_dir, c.trade_price, c.trade_vol) as alert_percent \n" +
                "from view_stock_order_window_data o \n" +
                "left join kafka_stock_order_confirm c on o.order_no = c.order_no \n" +
                "group by o.sec_code");
        DataStream<Tuple2<Boolean, Row>> afterJoinToRs = tableEnv.toRetractStream(afterJoin, Row.class);

        TypeInformation<?>[] types2 = new TypeInformation[2];
        String[] fieldNames2 = new String[2];
        types2[0] = TypeInformation.of(new TypeHint<String>() {});
        types2[1] = TypeInformation.of(new TypeHint<Double>() {});
        fieldNames2[0] = "sec_code";
        fieldNames2[1] = "alert_percent";

        SingleOutputStreamOperator<Row> afterJoinSos = afterJoinToRs
                .map(tp2 -> tp2.f1)
                .returns(new RowTypeInfo(types2, fieldNames2));
//        afterJoinSos.print();
//        streamEnv.execute("a");
//
        String view = "to_insert_ds_" + System.currentTimeMillis();
        tableEnv.createTemporaryView(view, afterJoinSos);
        String finalSql = StringUtils.join(
                "insert into kafka_stock_alert_self_buy_sell select * from ", view
        );
        tableEnv.executeSql(finalSql);
    }

    public static void createSth(StreamTableEnvironment tableEnv) {
        tableEnv.registerFunction(FuncName.BIGINT_TO_TS, new BigintToTimestamp());
//        tableEnv.createTemporarySystemFunction(FuncName.OUTPUT_ALL_UDTF, new OutputAllUdtf());
//        tableEnv.registerFunction(FuncName.OUTPUT_ALL_UDTAF, new OutputAllUdtaf());
        tableEnv.registerFunction(FuncName.ALERT_SELF_BUY_SELL_UDTAF, new AlertSelfBuySellUdtaf());
//        tableEnv.executeSql("CREATE FUNCTION alert_self_buy_sell AS 'pers.pudgebd.flink.java.func.AlertSelfBuySellUdaf' LANGUAGE JAVA");

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
                "    ts_sql timestamp(3) COMMENT '订单接收时间，微妙级时间戳'," +
                "    ts_iso timestamp(3) COMMENT '订单接收时间，微妙级时间戳'," +
                "    ts_long bigint COMMENT '订单接收时间，微妙级时间戳'" +
//                "    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS"+
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
        //ISO-8601

        tableEnv.executeSql("create table kafka_stock_order_confirm(\n" +
                "    sec_code string,\n" +
                "    act_no bigint,\n" +
                "    acct_id string,\n" +
                "    order_no bigint,\n" +
                "    trade_dir string,\n" +
                "    trade_price bigint comment '交易价格，单位为分',\n" +
                "    trade_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',\n" +
                "    pbu bigint,\n" +
                "    ts_sql timestamp(3) COMMENT '订单接收时间,timestamp(3)，微妙级时间戳'," +
                "    ts_iso timestamp(3) COMMENT '订单接收时间,timestamp(3)，微妙级时间戳'," +
                "    ts_long bigint COMMENT '订单接收时间，微妙级时间戳'" +
                ")\n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order_confirm',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_order_confirm_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");

        tableEnv.executeSql("create table kafka_stock_after_join_write(\n" +
                "    sec_code string,\n" +
                "    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',\n" +
                "    acct_id string,\n" +
                "    trade_dir string,\n" +
                "    trade_price bigint comment '交易价格，单位为分',\n" +
                "    trade_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',\n" +
                "    ts_long bigint,\n" +
                "    is_acc boolean\n" +
                ")\n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_after_join',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'kafka_stock_after_join_write_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");

        tableEnv.executeSql("create table kafka_stock_after_join_read(\n" +
                "    sec_code string,\n" +
                "    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',\n" +
                "    acct_id string,\n" +
                "    trade_dir string,\n" +
                "    trade_price bigint comment '交易价格，单位为分',\n" +
                "    trade_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',\n" +
                "    ts_long bigint,\n" +
                "    is_acc boolean,\n" +
                "    ts AS bigint_to_ts(ts_long),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS"+
                ")\n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_after_join',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'kafka_stock_after_join_read_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");

        tableEnv.executeSql("create table kafka_stock_alert_self_buy_sell(\n" +
                "    sec_code string,\n" +
                "    alert_percent double\n" +
                ")\n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_alert_self_buy_sell',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_alert_self_buy_sell',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");
    }

}
