package pers.pudgebd.flink.java.hive;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import pers.pudgebd.flink.java.hive.pojo.HiveDimPojo;

public class HiveDim04 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String hiveDimTblPath = "hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable";

        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        Path path = new Path(hiveDimTblPath + "/dt=2020-11-16/000000_0");
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
        MessageType messageType =readFooter.getFileMetaData().getSchema();

        PojoTypeInfo<HiveDimPojo> as = (PojoTypeInfo<HiveDimPojo>) PojoTypeInfo.of(HiveDimPojo.class);
        FileInputFormat<HiveDimPojo> fif = new ParquetPojoInputFormat<HiveDimPojo>(
                new org.apache.flink.core.fs.Path(hiveDimTblPath), messageType, as
        );

        TypeInformation<?>[] types = new TypeInformation<?>[2];
        types[0] = TypeInformation.of(new TypeHint<String>() {});
        types[1] = TypeInformation.of(new TypeHint<String>() {});

        String[] fieldNames = new String[2];
        fieldNames[0] = "channel";
        fieldNames[1] = "name";

        DataStreamSource<HiveDimPojo> dss = bsEnv.readFile(fif, hiveDimTblPath);
        dss.print();
    }


}
