package pers.pudgebd.flink.java.hive;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import pers.pudgebd.flink.java.hive.pojo.HiveDimPojo;

public class HiveDim06 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String hiveDimTblPath = "hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable";

        PojoTypeInfo<HiveDimPojo> typeInfo = (PojoTypeInfo<HiveDimPojo>) PojoTypeInfo.of(HiveDimPojo.class);
        Schema schema = ReflectData.get().getSchema(HiveDimPojo.class);
        MessageType messageType = new AvroSchemaConverter().convert(schema);
        org.apache.flink.core.fs.Path flinkFsPath = new org.apache.flink.core.fs.Path(hiveDimTblPath);

        DataStreamSource<HiveDimPojo> dss = bsEnv.createInput(
                new ParquetPojoInputFormat<HiveDimPojo>(flinkFsPath, messageType, typeInfo)
        );
        dss.print();
    }


}
