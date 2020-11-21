package pers.pudgebd.flink.java.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveDim_success {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDim_success.class);
    static boolean needCreateTable = true;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "/Users/pudgebd/work_doc/sz_cluster/conf"; // a local path

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        //可读取目录里的文件
//        File filesDir = new File(hiveConfDir);
//        if (filesDir.isDirectory()) {
//            for (File ele : filesDir.listFiles()) {
//                hadoopConf.addResource(new FileInputStream(ele));
//            }
//        }
        //最少一个配置就行
        hadoopConf.set("hive.metastore.uris", "thrift://cdh601:9083");
//        hadoopConf.set("hive.metastore.warehouse.dir", "/user/hive/warehouse");
//        hadoopConf.set("hive.zookeeper.quorum", "cdh601,cdh603,cdh602");
//        hadoopConf.set("hive.zookeeper.client.port", "2181");

        HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConf);
        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        //1.11.2 不能通过sql创建hive catalog
//        tableEnv.executeSql("CREATE CATALOG myhive WITH ('hive.metastore.uris'='thrift://cdh601:9083')");
//        tableEnv.executeSql("USE CATALOG myhive");
//        System.out.println(StringUtils.join(tableEnv.listCatalogs(), ", "));

        if (needCreateTable) {
            try {
                tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
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
            } catch (Exception e) {
                LOG.error("", e);
            }
            try {
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
            } catch (Exception e) {
                LOG.error("", e);
            }
            try {
                tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
                tableEnv.executeSql("CREATE TABLE MyHiveDimTable (\n" +
                        "    channel STRING,\n" +
                        "    name STRING\n" +
                        ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt',\n" +
                        "  'sink.partition-commit.trigger'='partition-time',\n" +
                        "  'sink.partition-commit.delay'='1 d',\n" +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'," +
                        "  'streaming-source.enable'='true'," +
                        "  'streaming-source.consume-order'='partition-time'," +
                        "  'streaming-source.consume-start-offset'='2020-11-10'" +
                        ")");
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
//        tableEnv.executeSql("ALTER TABLE MyHiveDimTable set ('streaming-source.enable'='true', " +
//                "'streaming-source.consume-start-offset'='2020-11-10', " +
//                "'streaming-source.consume-order'='partition-time')");

        tableEnv.getConfig().getConfiguration().setBoolean(
                TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
        //1.11.2不支持
//        tableEnv.executeSql("set table.dynamic-table-options.enabled=true");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("insert into MyTestInsert " +
                "select s1.goods_id as id, s1.channel, s1.dt as pv, d.name " +
                "from MyKafkaSrc01 s1 " +
                "left join MyHiveDimTable " +
                "/*+ OPTIONS('streaming-source.enable'='true', " +
                "'streaming-source.monitor-interval'='5 s', " +
                "'streaming-source.consume-start-offset'='2020-11-16') */ d " +
                "on s1.channel = d.channel and s1.dt = d.dt");

        //可把维表作为时序表join
        // FOR SYSTEM_TIME AS OF s1.proctime AS

    }


}
