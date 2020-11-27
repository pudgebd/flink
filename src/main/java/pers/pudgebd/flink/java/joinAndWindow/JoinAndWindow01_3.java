package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.constants.FuncName;

import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class JoinAndWindow01_3 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);
        Table joinedTbl = tableEnv.sqlQuery(
                "select o.order_type, c.acct_id, o.sec_code, c.trade_dir, " +
                        "c.trade_price, c.trade_vol, o.ts " +
                        "from kafka_stock_order o left join kafka_stock_order_confirm c on o.order_no = c.order_no");

        tableEnv.createTemporaryView("joinedTbl", joinedTbl);

        tableEnv.executeSql("create view view_after_join(\n" +
                "    order_type,\n" +
                "    acct_id,\n" +
                "    sec_code,\n" +
                "    trade_dir,\n" +
                "    trade_price,\n" +
                "    trade_vol," +
                "    ts"+
                ") AS select order_type, acct_id, sec_code, trade_dir, trade_price, trade_vol, ts from joinedTbl");
//        joinedTbl.addOrReplaceColumns($("ts").rowtime());

        Table view_after_join =  tableEnv.from("view_after_join");
        Table aggTbl = view_after_join.window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
                .groupBy($("w"), $("sec_code"))
                .flatAggregate(
                        call(
                                FuncName.ALERT_SELF_BUY_SELL_UDTAF, $("order_type"), $("acct_id"),
                                $("trade_dir"), $("trade_price"), $("trade_vol"),
                                $("is_acc")
                        ).as("alert_percent")
                )
                .select($("sec_code"), $("alert_percent"));

        tableEnv.createTemporaryView("tmp", aggTbl);
        tableEnv.executeSql("insert into kafka_stock_alert_self_buy_sell select sec_code, alert_percent from tmp");
    }

}
