package pers.pudgebd.flink.java.hive;

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.FileInputStream;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HiveDim01_01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

//        for (String ele : tableEnv.listCatalogs()) {
//            System.out.println(ele);
//        }

//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
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

//        String name            = "myhive";
//        String defaultDatabase = "default";
//        String hiveConfDir     = "/Users/pudgebd/work_doc/深圳集群环境/配置"; // a local path
//
//        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
//        tableEnv.registerCatalog("myhive", hiveCatalog);
//        tableEnv.useCatalog("myhive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("CREATE TABLE MyHiveDimTable (\n" +
                "    channel STRING,\n" +
                "    name STRING\n" +
                ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',\n" +
                "  'format'='parquet',\n" +
                "  'streaming-source.enable'='true',\n" +
                "  'streaming-source.monitor-interval'='1 m'," +
                "  'streaming-source.consume-order'='partition-time',\n" +
                "  'streaming-source.consume-start-offset'='2020-11-16',\n" +
                "  'lookup.join.cache.ttl'='1 m'" +
                ")");

//        Configuration config = tableEnv.getConfig().getConfiguration();
//        config.setString("table.dynamic-table-options.enabled", "true");
//        config.setString("lookup.join.cache.ttl", "1 m");

        tableEnv.executeSql("insert into MyTestInsert " +
                "select s1.goods_id as id, s1.channel, s1.dt as pv, d.name " +
                "from MyKafkaSrc01 s1 " +
                "left join MyHiveDimTable d " +
                "on s1.channel = d.channel and s1.dt = d.dt");


        Optional<Catalog> catalogOpt = tableEnv.getCatalog("default_catalog");
        Catalog catalog = catalogOpt.get();
        ObjectPath hiveDim1 = ObjectPath.fromString("default_database.MyHiveDimTable");
        ObjectPath hiveDim2 = ObjectPath.fromString("default_database.MyHiveDimTable2");
//        catalog.dropTable(objectPath, true);

        ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
        pool.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            catalog.dropTable(hiveDim1, false);
//                            catalog.renameTable(hiveDim1, "MyHiveDimTable_to_drop", true);
//                            catalog.renameTable(hiveDim2, "MyHiveDimTable", true);

//                            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//                            tableEnv.executeSql("CREATE TABLE MyHiveDimTable (\n" +
//                                    "    channel STRING,\n" +
//                                    "    name STRING\n" +
//                                    ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
//                                    "  'connector'='filesystem',\n" +
//                                    "  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',\n" +
//                                    "  'format'='parquet',\n" +
//                                    "  'streaming-source.enable'='true',\n" +
//                                    "  'streaming-source.monitor-interval'='1 m'," +
//                                    "  'streaming-source.consume-order'='partition-time',\n" +
//                                    "  'streaming-source.consume-start-offset'='2020-11-17',\n" +
//                                    "  'lookup.join.cache.ttl'='1 m'" +
//                                    ")");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, 10, 10, TimeUnit.SECONDS
        );
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }
//        Thread.sleep(5_000L);
//

//        tableEnv.from("MyHiveDimTable").printSchema();

}
