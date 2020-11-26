package pers.pudgebd.flink.java.task;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import pers.pudgebd.flink.java.func.MapStrToGoodsDetail;
import pers.pudgebd.flink.java.func.MapStrToGrPo;
import pers.pudgebd.flink.java.pojo.GidSumPricePo;
import pers.pudgebd.flink.java.pojo.GoodsDetailPo;
import pers.pudgebd.flink.java.pojo.GrPo;
import pers.pudgebd.flink.java.pojo.InsertTestPo;
import pers.pudgebd.flink.java.sql.CrtTblSqls;
import pers.pudgebd.flink.java.utils.Constants;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class MysqlDimTable {

    private static String CSV_BASIC_PATH = "/Users/asd/tmp/csv/t_goods_raiting/";
    private static String APP_NAME = "MysqlDimTable";

    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        TableEnvironment noStreamTableEnv = TableEnvironment.create(bsSettings);

        //设置流的时间特征
//        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

//        FlinkKafkaConsumer source01 = new FlinkKafkaConsumer<String>("mytopic02", new SimpleStringSchema(), properties);
//        DataStream<GrPo> kafkaDs01 = bsEnv.addSource(source01).map(new MapStrToGrPo());
//                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());

//        bsTableEnv.fromDataStream(kafkaDs01);
//        bsTableEnv.createTemporaryView("kafka_source", kafkaDs01);

        tableEnv.executeSql(CrtTblSqls.KAFKA_SOURCE);

        tableEnv.executeSql(CrtTblSqls.DIM_MYSQL);

        tableEnv.executeSql(CrtTblSqls.INSERT_TEST);

//        bsTableEnv.createTemporaryView("dim_mysql", dimTbl);
//        String sql = "insert into insert_test select cast(ks.id as int), ks.first, ks.last, cast(dm.score as double) " +
//                "from kafka_source ks left join dim_mysql dm on cast(ks.user_id as int) = cast(dm.user_id as int)";
        String sql = "insert into insert_test select ks.id, ks.first, ks.last, dm.score " +
                "from kafka_source ks left join dim_mysql dm on ks.user_id = dm.user_id";

//        Table resultTbl = tableEnv.sqlQuery(sql);
        tableEnv.executeSql(sql);
//        System.out.println(tableResult1.getJobClient().get().getJobStatus());
//        DataStream<Tuple2<Boolean, InsertTestPo>> toRetr = bsTableEnv.toRetractStream(resultTbl, InsertTestPo.class);
//
//        toRetr.filter(tp2 -> tp2.f0)
//                .map(tp2 -> tp2.f1)
//                .print();

//        SingleOutputStreamOperator<GidSumPricePo> mapped = toRetr.filter(tp2 -> tp2.f0)
//                .map(tp2 -> tp2.f1);
//        mapped.addSink(new MyRedisSink());

//        tableEnv.execute("");
//        CountDownLatch countDownLatch = new CountDownLatch(1);
//        countDownLatch.await();
    }

}
