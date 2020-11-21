package pers.pudgebd.flink.java.hive;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import pers.pudgebd.flink.java.func.MapStrToGrHivePo;
import pers.pudgebd.flink.java.pojo.GoodsDetailPo;
import pers.pudgebd.flink.java.pojo.GrHivePo;
import pers.pudgebd.flink.java.sql.CrtTblSqls;
import pers.pudgebd.flink.java.utils.Constants;

import java.util.Properties;

public class HiveDemo {

    static HiveConf hiveConf = new HiveConf();
    static {
        hiveConf.set("javax.jdo.option.ConnectionURL", "");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver");
        hiveConf.set("", "");
        hiveConf.set("", "");
        hiveConf.set("", "");
        hiveConf.set("", "");
        hiveConf.set("", "");
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        String name            = "myhive";
        String defaultDatabase = "ods_kafka";
        String hiveConfDir     = "hdfs://localhost:8020/tmp"; // a local path

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hiveCatalog);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

        tableEnv.executeSql(CrtTblSqls.KAFKA_SOURCE);
        String sql = "insert into ods_kafka.from_kafka_source select * from kafka_source";
        tableEnv.executeSql(sql);
    }

}
