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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class MultiInert02 {

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

        TypeInformation<?>[] types = new TypeInformation[3];
        String[] fieldNames = new String[3];
        types[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types[1] = TypeInformation.of(new TypeHint<Long>() {
        });
        types[2] = TypeInformation.of(new TypeHint<Long>() {
        });
        types[3] = TypeInformation.of(new TypeHint<Boolean>() {
        });
        fieldNames[0] = "sec_code";
        fieldNames[1] = "order_type";
        fieldNames[2] = "order_no";
        fieldNames[3] = "is_acc";
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

        DataStream<Row> ds = tableEnv.toRetractStream(joinedTbl, Row.class)
                .map(tp2 -> Row.join(tp2.f1, Row.of(tp2.f0)))
                .returns(rowTypeInfo);


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
