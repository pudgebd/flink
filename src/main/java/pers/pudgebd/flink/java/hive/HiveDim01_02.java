package pers.pudgebd.flink.java.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HiveDim01_02 {

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

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("CREATE TABLE MyHiveDimTable (\n" +
                "    channel STRING,\n" +
                "    name STRING\n" +
                ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable/dt=2020-11-16',\n" +
                "  'format'='parquet',\n" +
                "  'streaming-source.enable'='true',\n" +
                "  'streaming-source.monitor-interval'='1 m'," +
                "  'streaming-source.consume-order'='partition-time',\n" +
                "  'streaming-source.consume-start-offset'='2020-11-16',\n" +
                "  'lookup.join.cache.ttl'='1 m'" +
                ")");

        tableEnv.executeSql("CREATE TABLE MyHiveDimTableMoreParti (\n" +
                "    channel STRING,\n" +
                "    name STRING\n" +
                ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable/dt=2020-11-17',\n" +
                "  'format'='parquet',\n" +
                "  'streaming-source.enable'='true',\n" +
                "  'streaming-source.monitor-interval'='1 m'," +
                "  'streaming-source.consume-order'='partition-time',\n" +
                "  'streaming-source.consume-start-offset'='2020-11-16',\n" +
                "  'lookup.join.cache.ttl'='1 m'" +
                ")");

        Table tbl = tableEnv.from("MyHiveDimTable");
        DataStream<Row> ds = tableEnv.toAppendStream(tbl, Row.class);



        tableEnv.executeSql("insert into MyTestInsert " +
                "select s1.goods_id as id, s1.channel, s1.dt as pv, d.name " +
                "from MyKafkaSrc01 s1 " +
                "left join MyHiveDimTable d " +
                "on s1.channel = d.channel and s1.dt = d.dt");
    }


}
