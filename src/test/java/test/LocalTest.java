package test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import pers.pudgebd.flink.java.sql.CrtTblSqls;
import pers.pudgebd.flink.java.task.ReadAndPrintMain;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Calendar;

public class LocalTest {

    @Test
    public void encodeSql() throws Exception {
//        System.out.println(CrtTblSqls.KAFKA_SOURCE);
//        System.out.println(CrtTblSqls.DIM_MYSQL);
//        System.out.println(CrtTblSqls.INSERT_TEST);
//        System.out.println();
//        System.out.println();

//        String filePath = "/Users/pudgebd/work_doc/flink_sql/official_test_sql_2.sql";
//        String filePath = "/Users/pudgebd/work_doc/platform_app_main_config/test_map_state_creat_tbls.sql";
        String filePath = "/Users/pudgebd/work_doc/platform_app_main_config/test_map_state_query_sql.sql";

        String rawSqls = IOUtils.toString(new FileInputStream(filePath));
        rawSqls = URLEncoder.encode(rawSqls, StandardCharsets.UTF_8.name());
        System.out.println(rawSqls);
//        ReadAndPrintMain.main(new String[] {rawSqls});
    }


    @Test
    public void hiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(new Path("hdfs://localhost:8020/tmp/hive-site.xml"));
//        hiveConf.set
        System.out.println(hiveConf.get("javax.jdo.option.ConnectionDriverName"));
    }


    @Test
    public void mathExpression() throws Exception {
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        String foo = "(3+(5*(6-1/2)) > 31.49) || (1 / 2) > 0.7";
        System.out.println(engine.eval(foo));
    }


    @Test
    public void print() throws Exception {
        System.out.println("CREATE TABLE MyHiveDimTable (\n" +
                "    channel STRING,\n" +
                "    name STRING,\n" +
                "    dt STRING" +
                ") PARTITIONED BY (dt) WITH (" +
                "'connector'='filesystem',\n" +
                "'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',\n" +
                "'format'='parquet',\n" +
                "'sink.partition-commit.delay'='1 h',\n"+
                "'sink.partition-commit.policy.kind'='success-file',\n" +
                "'streaming-source.enable'='true',\n" +
                "'streaming-source.monitor-interval'='1 m',\n" +
                "'streaming-source.consume-order'='create-time',\n" +
                "'streaming-source.consume-start-offset'='2020-11-17',\n" +
                "'lookup.join.cache.ttl'='1 min',\n" +
                "'lookup.cache.max-rows' = '5000',\n" +
                "'lookup.cache.ttl' = '1min'\n" +
                ")");
    }

    @Test
    public void localDateTime() throws Exception {
        System.out.println(System.currentTimeMillis());
    }
}
