package pers.pudgebd.flink.java.multiInsert;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.table.api.Expressions.$;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class MultiInert02 {

    static TypeInformation<?>[] types01 = new TypeInformation[4];
    static String[] fieldNames01 = new String[4];
    static RowTypeInfo rowTypeInfo01 = new RowTypeInfo();

    static TypeInformation<?>[] types02 = new TypeInformation[2];
    static String[] fieldNames02 = new String[2];
    static RowTypeInfo rowTypeInfo02 = new RowTypeInfo();

    static TypeInformation<?>[] types03 = new TypeInformation[2];
    static String[] fieldNames03 = new String[2];
    static RowTypeInfo rowTypeInfo03 = new RowTypeInfo();

    static {
        types01[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types01[1] = TypeInformation.of(new TypeHint<Long>() {
        });
        types01[2] = TypeInformation.of(new TypeHint<Long>() {
        });
        types01[3] = TypeInformation.of(new TypeHint<Boolean>() {
        });
        fieldNames01[0] = "sec_code";
        fieldNames01[1] = "order_type";
        fieldNames01[2] = "order_no";
        fieldNames01[3] = "is_acc";
        rowTypeInfo01 = new RowTypeInfo(types01, fieldNames01);


        types02[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types02[1] = TypeInformation.of(new TypeHint<Long>() {
        });
        fieldNames02[0] = "sec_code";
        fieldNames02[1] = "order_type";
        rowTypeInfo02 = new RowTypeInfo(types02, fieldNames02);


        types03[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types03[1] = TypeInformation.of(new TypeHint<Long>() {
        });
        fieldNames03[0] = "sec_code";
        fieldNames03[1] = "order_no";
        rowTypeInfo03 = new RowTypeInfo(types03, fieldNames03);
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
//        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createSth(tableEnv);
        createTmp(tableEnv);
        Table joinedTbl = tableEnv.sqlQuery(
                "select o.sec_code, o.order_type, c.order_no " +
                        "from kafka_stock_order o left join kafka_stock_order_confirm c on o.order_no = c.order_no");

        DataStream<Row> ds = tableEnv.toRetractStream(joinedTbl, Row.class)
                .map(tp2 -> Row.join(tp2.f1, Row.of(tp2.f0)))
                .returns(rowTypeInfo01);

        OutputTag<Row> outputTag01 = new OutputTag<Row>("side_output_01", rowTypeInfo02){};
        OutputTag<Row> outputTag02 = new OutputTag<Row>("side_output_02", rowTypeInfo03){};

        SingleOutputStreamOperator<Row> mainDataStream = ds.process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(Row row, Context ctx, Collector<Row> out) throws Exception {
                out.collect(row);

                String secCode = row.getField(0).toString();
                if ("p1".equalsIgnoreCase(secCode)) {
                    ctx.output(outputTag01, Row.ofKind(row.getKind(), row.getField(0),
                            row.getField(1)));

                } else if ("p2".equalsIgnoreCase(secCode)) {
                    ctx.output(outputTag02, Row.ofKind(row.getKind(), row.getField(0),
                            row.getField(2)));
                }
            }
        }, rowTypeInfo01);
//        mainDataStream.print();
//        streamEnv.execute("a");

        DataStream<Row> ds01 = mainDataStream.getSideOutput(outputTag01);
        tableEnv.fromDataStream(ds01)
                .executeInsert("side_output_01");

        DataStream<Row> ds02 = mainDataStream.getSideOutput(outputTag02);
        tableEnv.fromDataStream(ds02)
                .executeInsert("side_output_02");
    }


    private static void createTmp(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table side_output_01(\n" +
                "    sec_code string,\n" +
                "    order_type bigint\n" +
                ")\n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'side_output_01',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'side_output_01_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");

        tableEnv.executeSql("create table side_output_02(\n" +
                "    sec_code string,\n" +
                "    order_no bigint\n" +
                ")\n" +
                "with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'side_output_02',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'side_output_02_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");
    }

}