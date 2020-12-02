package pers.pudgebd.flink.java.task;

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pers.pudgebd.flink.java.constants.FuncName;
import pers.pudgebd.flink.java.sql.CrtTblSqls;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import static org.apache.flink.table.api.Expressions.*;
import static pers.pudgebd.flink.java.joinAndWindow.JoinAndWindow01_1.createSth;

public class TmpMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        createSth(tableEnv);
        String sql = "select sec_code, min(order_price) as min_order_price, max(order_price) as max_order_price \n" +
                "from kafka_stock_order " +
                "group by TUMBLE(proctime, INTERVAL '10' SECONDS), sec_code";
        System.out.println(sql);
        Table tmp = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(tmp, Row.class)
                .print();
        streamEnv.execute("a");
    }

}
