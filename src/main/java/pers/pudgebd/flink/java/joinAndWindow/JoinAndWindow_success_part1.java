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
import org.apache.flink.types.RowKind;
import pers.pudgebd.flink.java.constants.FuncName;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class JoinAndWindow_success_part1 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        //Emit strategy has not been supported for Table Aggregate!
//        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
//        tableEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "1000 ms");

        createSth(tableEnv);
        Table joinedTbl = tableEnv.sqlQuery(
                "select o.sec_code, o.order_type, c.acct_id, c.trade_dir, " +
                        "c.trade_price, c.trade_vol, o.ts_long " +
                        "from kafka_stock_order o left join kafka_stock_order_confirm c on o.order_no = c.order_no");

        DataStream<Tuple2<Boolean, Row>> ds = tableEnv.toRetractStream(joinedTbl, Row.class); //, new RowTypeInfo(types1, fieldNames1)
        TypeInformation<?>[] types2 = new TypeInformation[8];
        String[] fieldNames2 = new String[8];
        types2[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types2[1] = TypeInformation.of(new TypeHint<Long>() {
        });
        types2[2] = TypeInformation.of(new TypeHint<String>() {
        });
        types2[3] = TypeInformation.of(new TypeHint<String>() {
        });
        types2[4] = TypeInformation.of(new TypeHint<Long>() {
        });
        types2[5] = TypeInformation.of(new TypeHint<Long>() {
        });
        types2[6] = TypeInformation.of(new TypeHint<Long>() {
        }); //Timestamp LocalDateTime
        types2[7] = TypeInformation.of(new TypeHint<Boolean>() {
        });
        fieldNames2[0] = "sec_code";
        fieldNames2[1] = "order_type";
        fieldNames2[2] = "acct_id";
        fieldNames2[3] = "trade_dir";
        fieldNames2[4] = "trade_price";
        fieldNames2[5] = "trade_vol";
        fieldNames2[6] = "ts_long";
        fieldNames2[7] = "is_acc";
        RowTypeInfo rowTypeInfo2 = new RowTypeInfo(types2, fieldNames2);

        SingleOutputStreamOperator<Row> sos = ds
                .map(tp2 -> Row.join(tp2.f1, Row.of(tp2.f0)))
                .returns(rowTypeInfo2);
//        sos.print();
//        streamEnv.execute("a");

        Table sosTbl = tableEnv.fromDataStream(sos);
        tableEnv.createTemporaryView("sosTbl", sosTbl);
//        tableEnv.executeSql("insert into kafka_stock_after_join select sec_code, " +
//                "order_type, acct_id, trade_dir, " +
//                "trade_price, trade_vol, ts_long, is_acc from sosTbl");
//        tableEnv.from("kafka_stock_after_join_write").printSchema();
        tableEnv.executeSql("insert into kafka_stock_after_join_write select * from sosTbl");
    }

}
