package pers.pudgebd.flink.java.windowAndState;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Map;

public class KeyedWindFuncState {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "50100-50200");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createSth(tableEnv);
        Table tbl = tableEnv.sqlQuery("select sec_code, acct_id from kafka_stock_order_confirm");
        TypeInformation<?>[] types = new TypeInformation[2];
        String[] fieldNames = new String[2];
        types[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types[1] = TypeInformation.of(new TypeHint<String>() {
        });
        types[2] = TypeInformation.of(new TypeHint<Boolean>() {
        });
        fieldNames[0] = "sec_code";
        fieldNames[1] = "acct_id";
        fieldNames[2] = "is_acc";
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

        DataStream<Tuple2<Boolean, Row>> ds = tableEnv.toRetractStream(tbl, Row.class);
        SingleOutputStreamOperator<Row> sos = ds
                .map(tp2 -> Row.join(tp2.f1, Row.of(tp2.f0)))
                .returns(rowTypeInfo);

        KeyedStream<Row, String> ks = sos.keyBy(row -> String.valueOf(row.getField(0)));
        WindowedStream<Row, String, TimeWindow> ws = ks.timeWindow(Time.seconds(20));
        processOrApply(ws);
//        reduce(ws);
//        agg(ws);
        streamEnv.execute("a");
    }

    private static void agg(WindowedStream<Row, String, TimeWindow> ws) {
        ws.aggregate(new AggregateFunction<Row, Object, Map<String, String>>() {
            @Override
            public Object createAccumulator() {
                return null;
            }

            @Override
            public Object add(Row value, Object accumulator) {
                return null;
            }

            @Override
            public Map<String, String> getResult(Object accumulator) {
                return null;
            }

            @Override
            public Object merge(Object a, Object b) {
                return null;
            }
        });
    }

    private static void reduce(WindowedStream<Row, String, TimeWindow> ws) {
        ws.reduce(new RichReduceFunction<Row>() {
                      @Override
                      public Row reduce(Row value1, Row value2) throws Exception {
                          return null;
                      }
                  },
                new WindowFunction<Row, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Row> input, Collector<String> out) throws Exception {

                    }
                });
    }

    private static void processOrApply(WindowedStream<Row, String, TimeWindow> ws) {
        //和.apply是一样的
        ws.process(new ProcessWindowFunction<Row, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Row> elements, Collector<String> out) throws Exception {
                out.collect(key);
            }
        }).print();
    }


    private static void createSth(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table kafka_stock_order_confirm (\n" +
                "    buy map<string,string>,\n" +
                "    sell map<string,string>,\n" +
                "    sec_code as buy['sec_code'],\n" +
                "    acct_id as buy['acct_id'],\n" +
                "    trade_price as cast(buy['trade_price'] as bigint),\n" +
                "    trade_vol as cast(buy['trade_vol'] as bigint),\n" +
                "    ts as bigint_to_ts(buy['ts_long']),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS\n" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order_confirm',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_order_confirm_origin_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");
    }

}
