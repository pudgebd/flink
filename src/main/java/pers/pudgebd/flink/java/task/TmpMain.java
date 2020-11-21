package pers.pudgebd.flink.java.task;

import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pers.pudgebd.flink.java.sql.CrtTblSqls;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class TmpMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tableEnv.executeSql(CrtTblSqls.KAFKA_SOURCE);
//        String filePath = "/Users/pudgebd/work_doc/flinksql/official_test_sql_hive_dim.sql";
//        String rawSql = IOUtils.toString(new FileInputStream(filePath));
//
//        for (String sql : rawSql.split(";")) {
//            tableEnv.executeSql(sql);
//        }
    }

}
