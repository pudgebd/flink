package pers.pudgebd.flink.java.task;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.func.MapStrToGrPo;
import pers.pudgebd.flink.java.pojo.GrPo;
import pers.pudgebd.flink.java.utils.Constants;
import pers.pudgebd.flink.java.watermark.TimeLagWatermarkGenerator;

import java.util.Properties;


public class StreamTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamTask.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);
//        设置流的时间特征
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = Constants.kafkaProp;
//        FlinkKafkaConsumer source01 = new FlinkKafkaConsumer<String>("mytopic02", new SimpleStringSchema(), properties);
//        DataStream<GrPo> kafkaDs01 = bsEnv.addSource(source01).map(new MapStrToGrPo())
//                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());
//
//
//        bsTableEnv.fromDataStream(kafkaDs01);
//        bsTableEnv.createTemporaryView("kafka_source", kafkaDs01);
    }

}
