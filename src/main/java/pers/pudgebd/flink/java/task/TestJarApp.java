package pers.pudgebd.flink.java.task;

import com.haizhi.streamx.app.common.constant.FuncName;
import com.haizhi.streamx.app.common.constant.LocalDebug;
import com.haizhi.streamx.app.common.func.BigintToTimestamp;
import com.haizhi.streamx.app.func.Scene07ProcWinFunc;
import com.haizhi.streamx.app.func.vo.Scene07OutVo;
import com.haizhi.streamx.app.scene.po.ConfirmPo;
import com.haizhi.streamx.app.scene.util.EnvUtils;
import com.haizhi.streamx.app.scene.watermark.BoundedWaterMark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class TestJarApp {

    private static final Logger LOG = LoggerFactory.getLogger(TestJarApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("----- version: " + 2);
        StreamExecutionEnvironment streamEnv = EnvUtils.getEnv(args);

        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createSth(tableEnv);
        Table tbl = tableEnv.from("kafka_stock_order_confirm")
                .select($("sec_code"), $("acct_id"), $("trade_price"),
                        $("trade_vol"), $("ts_long"));

        DataStream<ConfirmPo> ds = tableEnv.toAppendStream(tbl, ConfirmPo.class)
                .assignTimestampsAndWatermarks(new BoundedWaterMark(5));//最多迟到5秒
        KeyedStream<ConfirmPo, String> ks = ds.keyBy(po -> po.sec_code);
        WindowedStream<ConfirmPo, String, TimeWindow> ws = ks.timeWindow(Time.seconds(10), Time.seconds(2));

        //和.apply是一样的
        SingleOutputStreamOperator<Scene07OutVo> soso = ws.process(new Scene07ProcWinFunc(),
                TypeInformation.of(Scene07OutVo.class));

        tableEnv.fromDataStream(soso)
                .executeInsert("kafka_scene07_output");
    }
//        TypeInformation<?>[] types = new TypeInformation[3];
//        String[] fieldNames = new String[3];
//        types[0] = TypeInformation.of(new TypeHint<String>() {});
//        types[1] = TypeInformation.of(new TypeHint<String>() {});
//        types[2] = TypeInformation.of(new TypeHint<String>() {});
//        fieldNames[0] = "acct_id";
//        fieldNames[1] = "window_start";
//        fieldNames[2] = "window_end";
    //        new RowTypeInfo(types, fieldNames)
    //TypeInformation.of(Scene07OutVo.class)
    //        tableEnv.createTemporaryView("view_to_insert", soso);
//        tableEnv.executeSql("insert into kafka_scene07_output select * from view_to_insert");


    private static void createSth(StreamTableEnvironment tableEnv) {
        tableEnv.registerFunction(FuncName.BIGINT_TO_TS, new BigintToTimestamp());

        tableEnv.executeSql("create table kafka_stock_order_confirm (\n" +
                "    buy map<string,string>,\n" +
                "    sell map<string,string>,\n" +
                "    sec_code as buy['sec_code'],\n" +
                "    acct_id as buy['acct_id'],\n" +
                "    trade_price as cast(buy['trade_price'] as bigint),\n" +
                "    trade_vol as cast(buy['trade_vol'] as bigint),\n" +
                "    ts_long as cast(buy['ts_long'] as bigint),\n" +
                "    ts as bigint_to_ts(buy['ts_long']),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS\n" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'stock_order_confirm',\n" +
                " 'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                " 'properties.group.id' = 'stock_order_confirm_origin_group',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.ignore-parse-errors' = 'true',\n" +
                " 'json.timestamp-format.standard' = 'SQL'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE kafka_scene07_output(\n" +
                "  acct_id STRING,\n" +
                "  window_start string,\n" +
                "  window_end string\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'scene07_output',\n" +
                "  'properties.bootstrap.servers' = '192.168.2.201:9092',\n" +
                "  'properties.group.id' = 'scene07_output_group',\n" +
                " 'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'json.timestamp-format.standard' = 'SQL'\n" +
                ")");
    }

}
