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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Calendar;

public class LocalTest {


    @Test
    public void tmp() throws Exception {
        String str = String.valueOf(System.currentTimeMillis());
        System.out.println(str.length());
    }

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
        System.out.println("insert into kafka_stock_alert_self_buy_sell " +
                "select sec_code, " +
                "alert_self_buy_sell_udaf(order_type, acct_id, trade_dir, trade_price, trade_vol, is_acc) " +
                "as alert_percent " +
                "from kafka_stock_after_join_read " +
                "group by TUMBLE(ts, INTERVAL '10' SECONDS), sec_code");
    }

    @Test
    public void localDateTime() throws Exception {
//        System.out.println(System.currentTimeMillis());
//        System.out.println(new Timestamp(
//                Long.parseLong("1606536044856123154".substring(0, 13))
//        ));
        System.out.println(new Timestamp(1606566074148L));
    }
}
