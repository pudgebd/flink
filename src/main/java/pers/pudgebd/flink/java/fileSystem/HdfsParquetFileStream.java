package pers.pudgebd.flink.java.fileSystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HdfsParquetFileStream {

    static boolean needCreateTable = false;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tableEnv.executeSql("CREATE TABLE MyKafkaSrc01(\n" +
                "    channel STRING,\n" +
                "    goods_id BIGINT,\n" +
                "    dt STRING,\n" +
                "    proctime as PROCTIME()\n" +
                ")\n" +
                "WITH (\n" +
                " 'connector' = 'kafka-0.11',\n" +
                " 'topic' = 'MyKafkaSrc01',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'MyKafkaSrc01',\n" +
                " 'format' = 'csv',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'csv.ignore-parse-errors' = 'true'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE MyTestInsert(\n" +
                "    id bigint," +
                "    channel STRING,\n" +
                "    pv STRING,\n" +
                "    name STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )WITH(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',\n" +
                "   'table-name' = 'cq_test_insert',\n" +
                "   'username' = 'stream_dev',\n" +
                "   'password' = 'stream_dev'\n" +
                " )");

        tableEnv.executeSql("CREATE TABLE MyHiveDimTable (\n" +
                "    channel STRING,\n" +
                "    name STRING,\n" +
                "    dt STRING) PARTITIONED BY (dt) WITH (\n" +
                "'connector'='filesystem',\n" +
                "'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',\n" +
                "'format'='parquet',\n" +
                "'sink.partition-commit.delay'='1 h',\n" +
                "'sink.partition-commit.policy.kind'='success-file',\n" +
                "'streaming-source.enable'='true',\n" +
                "'streaming-source.monitor-interval'='1 m',\n" +
                "'streaming-source.consume-order'='create-time',\n" +
                "'streaming-source.consume-start-offset'='2020-11-17',\n" +
                "'lookup.join.cache.ttl'='1 min',\n" +
                "'lookup.cache.max-rows' = '5000',\n" +
                "'lookup.cache.ttl' = '1min'\n" +
                ")");
        ////streaming-source.consume-start-offset 不起作用
//        tableEnv.executeSql("ALTER TABLE MyHiveDimTable set ('streaming-source.enable'='true', " +
//                "'streaming-source.consume-start-offset'='2020-11-10', " +
//                "'streaming-source.consume-order'='partition-time')");

//        tableEnv.getConfig().getConfiguration().setBoolean(
//                TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
        //1.11.2不支持
//        tableEnv.executeSql("set table.dynamic-table-options.enabled=true");

        tableEnv.executeSql("insert into MyTestInsert " +
                "select s1.goods_id as id, s1.channel, s1.dt as pv, d.name " +
                "from MyKafkaSrc01 s1 " +
                "left join MyHiveDimTable d " +
                "on s1.channel = d.channel and s1.dt = d.dt");

        //可把维表作为时序表join
        // FOR SYSTEM_TIME AS OF s1.proctime AS

    }


}
