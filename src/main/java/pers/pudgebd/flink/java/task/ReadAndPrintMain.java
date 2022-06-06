package pers.pudgebd.flink.java.task;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class ReadAndPrintMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        DataStreamSource<Integer> dss = bsEnv.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    ctx.collect(1);
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
            }
        });
        dss.print();
        bsEnv.execute("demo_job1");
    }

    public static void main2(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String[] sqls = rawSqls.split(";");
        int tailIdx = sqls.length - 1;
        for (int i = 0; i <= tailIdx; i++) {
            String subSql = sqls[i];
            TableResult tableResult = tableEnv.executeSql(subSql);
            if (i == tailIdx) {
                tableResult.print();
            }
        }
    }

    static String rawSqls = "CREATE TABLE MyKafkaSrc(\n" +
            "    channel STRING,\n" +
            "    pv STRING\n" +
            ")\n" +
            "WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'mytopic04',\n" +
            " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
            " 'properties.group.id' = 'testGroupAppJar01',\n" +
            " 'format' = 'csv',\n" +
            " 'scan.startup.mode' = 'latest-offset',\n" +
            " 'csv.ignore-parse-errors' = 'true'\n" +
            ");" +
            "\n" +
            "select * from MyKafkaSrc;";

}
