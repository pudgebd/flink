package pers.pudgebd.flink.java.task;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFileTask {

    private static Logger LOG = LoggerFactory.getLogger(ReadFileTask.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String parquetPath = "/path_to_file_dir/parquet_0000";
//        HadoopInputs.readHadoopFile(new MapredParquetInputFormat(), Void.class, ArrayWritable.class, parquetPath);
//        bsEnv.createInput(hadoopInputFormat);

    }

}
