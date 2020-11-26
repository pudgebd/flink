package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdtaf;
import pers.pudgebd.flink.java.func.OutputAllUdtaf;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01.createSth;

public class JoinAndWindow02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);
        tableEnv.registerFunction("alertSelfBuySellUdtaf", new AlertSelfBuySellUdtaf());

        Table kafka_stock_order = tableEnv.from("kafka_stock_order");
        Table kafka_stock_order_confirm = tableEnv.from("kafka_stock_order_confirm");

        Table joinedTbl = kafka_stock_order.join(kafka_stock_order_confirm,
                $("kafka_stock_order.order_no").isEqual($("kafka_stock_order_confirm.order_no")));

        ApiExpression[] apiExprs = {$("kafka_stock_order.order_type"), $(""), $(""),
                $(""), $(""), $("")};

        joinedTbl.window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
        .groupBy($("w"), $("kafka_stock_order.sec_code"))
                .select($("kafka_stock_order.order_price").sum())
        .printSchema();
//        .flatAggregate(
//                call("alertSelfBuySellUdtaf", $("a"))
//                .as("v", "rank")
//        );
    }

}
