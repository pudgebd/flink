package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.constants.FuncName;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class JoinAndWindow_success_part2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createSth(tableEnv);
        Table kafka_stock_after_join_read = tableEnv.from("kafka_stock_after_join_read");

//        tableEnv.toAppendStream(kafka_stock_after_join_read, Row.class)
//                .print();
//        streamEnv.execute("a");

        if (false) {
            Table aggTbl = kafka_stock_after_join_read.window(Tumble.over(lit(60).seconds()).on($("ts")).as("w"))
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
        } else {
            tableEnv.executeSql("insert into kafka_stock_alert_self_buy_sell " +
                    "select sec_code, " +
                    "alert_self_buy_sell_udaf(order_type, acct_id, trade_dir, trade_price, trade_vol, is_acc) " +
                    "as alert_percent " +
                    "from kafka_stock_after_join_read " +
                    "group by TUMBLE(ts, INTERVAL '10' SECONDS), sec_code");
        }
    }

}
