package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdtaf;

import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class JoinAndWindow04 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);
        tableEnv.registerFunction("alertSelfBuySellUdtaf", new AlertSelfBuySellUdtaf());

        Table kafka_stock_order = tableEnv.from("kafka_stock_order");
        DataStream<Row> orderDs = tableEnv.toAppendStream(kafka_stock_order, Row.class);
        Table kafka_stock_order_confirm = tableEnv.from("kafka_stock_order_confirm");
        DataStream<Row> confirmDs = tableEnv.toAppendStream(kafka_stock_order_confirm, Row.class);

        KeyedStream<Row, Object> orderKs = orderDs.keyBy(new KeySelector<Row, Object>() {
            @Override
            public Object getKey(Row value) throws Exception {
                Object order_no = value.getField(2);
                return order_no;
            }
        });

        KeyedStream<Row, Object> confirmKs = confirmDs.keyBy(new KeySelector<Row, Object>() {
            @Override
            public Object getKey(Row value) throws Exception {
                Object order_no = value.getField(3);
                return order_no;
            }
        });

        orderKs.connect(confirmKs)
        ;
    }

}
