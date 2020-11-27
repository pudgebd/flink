package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.hadoop.yarn.util.Times;
import pers.pudgebd.flink.java.constants.FuncName;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdaf;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdtaf;
import pers.pudgebd.flink.java.func.OutputAllUdtaf;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class JoinAndWindow01_2 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        //Emit strategy has not been supported for Table Aggregate!
//        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
//        tableEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "1000 ms");

        createSth(tableEnv);
        Table joinedTbl = tableEnv.sqlQuery(
                "select o.ts, o.sec_code, o.order_type, c.acct_id, c.trade_dir, " +
                        "c.trade_price, c.trade_vol " +
                        "from kafka_stock_order o left join kafka_stock_order_confirm c on o.order_no = c.order_no");

        TypeInformation<?>[] types1 = new TypeInformation[7];
        String[] fieldNames1 = new String[7];
        types1[0] = TypeInformation.of(new TypeHint<Timestamp>() {
        }); //Timestamp LocalDateTime
        types1[1] = TypeInformation.of(new TypeHint<String>() {
        });
        types1[2] = TypeInformation.of(new TypeHint<Long>() {
        });
        types1[3] = TypeInformation.of(new TypeHint<String>() {
        });
        types1[4] = TypeInformation.of(new TypeHint<String>() {
        });
        types1[5] = TypeInformation.of(new TypeHint<Long>() {
        });
        types1[6] = TypeInformation.of(new TypeHint<Long>() {
        });
        fieldNames1[0] = "ts";
        fieldNames1[1] = "sec_code";
        fieldNames1[2] = "order_type";
        fieldNames1[3] = "acct_id";
        fieldNames1[4] = "trade_dir";
        fieldNames1[5] = "trade_price";
        fieldNames1[6] = "trade_vol";

        DataStream<Tuple2<Boolean, Row>> ds = tableEnv.toRetractStream(joinedTbl, new RowTypeInfo(types1, fieldNames1)); //, new RowTypeInfo(types1, fieldNames1)
//        ds.print();
//        streamEnv.execute("a");


        TypeInformation<?>[] types2 = new TypeInformation[8];
        String[] fieldNames2 = new String[8];
        types2[0] = TypeInformation.of(new TypeHint<Timestamp>() {
        }); //Timestamp LocalDateTime
        types2[1] = TypeInformation.of(new TypeHint<String>() {
        });
        types2[2] = TypeInformation.of(new TypeHint<Long>() {
        });
        types2[3] = TypeInformation.of(new TypeHint<String>() {
        });
        types2[4] = TypeInformation.of(new TypeHint<String>() {
        });
        types2[5] = TypeInformation.of(new TypeHint<Long>() {
        });
        types2[6] = TypeInformation.of(new TypeHint<Long>() {
        });
        types2[7] = TypeInformation.of(new TypeHint<Boolean>() {
        });
        fieldNames2[0] = "ts";
        fieldNames2[1] = "sec_code";
        fieldNames2[2] = "order_type";
        fieldNames2[3] = "acct_id";
        fieldNames2[4] = "trade_dir";
        fieldNames2[5] = "trade_price";
        fieldNames2[6] = "trade_vol";
        fieldNames2[7] = "is_acc";

        SingleOutputStreamOperator<Row> sos = ds
                .map(tp2 -> Row.join(tp2.f1, Row.of(tp2.f0)))
                .returns(new RowTypeInfo(types2, fieldNames2));
//        sos.print();
//        streamEnv.execute("a");

        Table sosTbl = tableEnv.fromDataStream(sos, $("ts").rowtime(), $("sec_code"),
                $("order_type"), $("acct_id"), $("trade_dir"),
                $("trade_price"), $("trade_vol"), $("is_acc"));

//        sosTbl.printSchema();
        tableEnv.toAppendStream(sosTbl, Row.class)
        .print();
        streamEnv.execute("a");
        if (false) {
        Table aggTbl = sosTbl.window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
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

}
