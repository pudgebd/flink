package pers.pudgebd.flink.java.multiInsert;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;


//from ( select src2alia.f2 as src2_f2_alia, ij_all_as.id as ij1_id_alia, ij_all_as.name, ij_all_as.age realAge from src2 src2alia inner join (select ij1_alia.id, ij2_as.name, ij2_as.age from ij1 ij1_alia left join (select id, name, age from ij2 where date > '2020-07-07') ij2_as on ij1_alia.id = ij2_as.id union (select ij3_alia.id, ij4_alia.name, ij4_alia.age from ij3 ij3_alia full join ij4 as ij4_alia on ij3_alia.name = ij4_alia.name)) ij_all_as on src2alia.id = ij_all_as.id) sale_detail
//
//        insert into table sale_detail_multi01 partition (sale_date='2010', region='china' )
//        select src2_f2_alia, ij1_id_alia, name, realAge where a = 1
//
//        insert overwrite table sale_detail_multi02 partition (sale_date='2011', region='china' )
//        select src2_f2_alia, ij1_id_alia, name, realAge where b != 'var'

//from test_source
//        insert into table test_order partition (year='2000', month='05')
//        select order_no, balance where order_no >= '0001' and order_no < '1000'

//        insert into table test_order partition (year='2000', month='06')
//        select order_no, balance where order_no >= '1000' and order_no < '5000'
public class MultiInert01 {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
//        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createSth(tableEnv);
        createTmp(tableEnv);
        Table joinedTbl = tableEnv.sqlQuery(
                "select o.sec_code, o.order_type " +
                        "from kafka_stock_order o left join kafka_stock_order_confirm c on o.order_no = c.order_no");

        TypeInformation<?>[] types = new TypeInformation[2];
        String[] fieldNames = new String[2];
        types[0] = TypeInformation.of(new TypeHint<String>() {
        });
        types[1] = TypeInformation.of(new TypeHint<Long>() {
        });
        fieldNames[0] = "sec_code";
        fieldNames[1] = "order_type";
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

        DataStream<Row> ds = tableEnv.toRetractStream(joinedTbl, Row.class)
                .map(tp2 -> tp2.f1)
                .returns(rowTypeInfo);

        tableEnv.createTemporaryView("view1", ds);
        tableEnv.executeSql("insert into side_output_01 select sec_code, order_type from view1 where sec_code = 'p1'");
//        tableEnv.executeSql("insert into side_output_02 select sec_code, order_type from view1 where sec_code = 'p2'");
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
                "    order_type bigint\n" +
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
