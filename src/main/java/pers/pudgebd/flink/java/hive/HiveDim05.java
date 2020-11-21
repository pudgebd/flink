package pers.pudgebd.flink.java.hive;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import pers.pudgebd.flink.java.hive.pojo.HiveDimPojo;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.parquet.avro.AvroSchemaConverter;

public class HiveDim05 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String hiveDimTblPath = "hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable";

        PojoTypeInfo<HiveDimPojo> typeInfo = (PojoTypeInfo<HiveDimPojo>) PojoTypeInfo.of(HiveDimPojo.class);
        Schema schema = ReflectData.get().getSchema(HiveDimPojo.class);
        MessageType messageType = new AvroSchemaConverter().convert(schema);
        org.apache.flink.core.fs.Path p = new org.apache.flink.core.fs.Path(hiveDimTblPath);
        ParquetPojoInputFormat<HiveDimPojo> inputFormat = new ParquetPojoInputFormat<HiveDimPojo>(
                p, messageType, typeInfo);

        DataStreamSource<HiveDimPojo> dss = bsEnv.readFile(inputFormat, hiveDimTblPath);
        dss.print();
    }


}
