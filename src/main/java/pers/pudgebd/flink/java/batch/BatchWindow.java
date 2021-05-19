package pers.pudgebd.flink.java.batch;

import com.haizhi.streamx.app.scene.watermark.BoundedWaterMark;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pers.pudgebd.flink.java.pojo.IdNamePo;
import pers.pudgebd.flink.java.watermark.BatchWindowBoundedWaterMark;

import java.util.Iterator;

public class BatchWindow {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "50100-50200");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dss = streamEnv.readTextFile(
                "/Users/chenqian/work_doc/sqls/flink_sql/debugsql/data/stock_order_input_data.csv");
        SingleOutputStreamOperator<IdNamePo> soso = dss.filter(str -> StringUtils.isNotBlank(str))
                .map(str -> {
                    String[] arr = str.split(",");
                    return new IdNamePo(arr[0], arr[1], Long.parseLong(arr[2]));
                });
        soso.assignTimestampsAndWatermarks(new BatchWindowBoundedWaterMark(2))
                .keyBy(po -> po.id)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<IdNamePo, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<IdNamePo> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        Iterator<IdNamePo> it = elements.iterator();
                        while (it.hasNext()) {
                            out.collect(it.next().toString() + "__start_" + start + "__end_" + end);
                        }
                    }
                })
                .print();

        streamEnv.execute("a");
    }


}
