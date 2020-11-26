package pers.pudgebd.flink.java.joinAndWindow;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import pers.pudgebd.flink.java.func.AlertSelfBuySellUdtaf;

import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class JoinAndWindow05 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);
        tableEnv.registerFunction("alertSelfBuySellUdtaf", new AlertSelfBuySellUdtaf());

        Table kafka_stock_order = tableEnv.from("kafka_stock_order");
        DataStream<Row> ksoDs = tableEnv.toAppendStream(kafka_stock_order, Row.class);
        Table kafka_stock_order_confirm = tableEnv.from("kafka_stock_order_confirm");
        DataStream<Row> ksocDs = tableEnv.toAppendStream(kafka_stock_order_confirm, Row.class);

        ksoDs.coGroup(ksocDs)
                .where(new KeySelector<Row, Object>() {
                    @Override
                    public Object getKey(Row value) throws Exception {
                        Object order_no = value.getField(2);
                        return order_no;
                    }
                })
                .equalTo(new KeySelector<Row, Object>() {
                    @Override
                    public Object getKey(Row value) throws Exception {
                        Object order_no = value.getField(3);
                        return order_no;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .apply(new CoGroupFunction<Row, Row, Object>() {
                    @Override
                    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<Object> out) throws Exception {
//                        out.collect();
                    }
                })
        .print();
        streamEnv.execute("a");
    }

}
