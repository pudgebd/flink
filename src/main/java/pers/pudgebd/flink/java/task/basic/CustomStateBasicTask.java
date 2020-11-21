package pers.pudgebd.flink.java.task.basic;

import com.haizhi.streamx.sqlparser.common.util.CommonSqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.func.CustomStateProcessFunc;
import pers.pudgebd.flink.java.pojo.custom.state.config.CustomStateConfig;
import pers.pudgebd.flink.java.pojo.custom.state.config.StateAndOutput;
import pers.pudgebd.flink.java.pojo.custom.state.config.StateDefine;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CustomStateBasicTask {

    private static final Logger LOG = LoggerFactory.getLogger(CustomStateBasicTask.class);

    protected static void createTables(
            StreamTableEnvironment bsTableEnv, CustomStateConfig customStateConfig) throws Exception {
        String createTableSqls = URLDecoder.decode(
                customStateConfig.getCreateTableSqls(), StandardCharsets.UTF_8.name()
        );
        List<String> sqls = CommonSqlUtils.getSqlsFromRawStr(createTableSqls);
        for (String sql : sqls) {
            bsTableEnv.executeSql(sql);
        }
    }


    protected static void createMapStateWithMapVal(
            StreamExecutionEnvironment bsEnv, StreamTableEnvironment tableEnv, StateAndOutput so) throws Exception {
        String querySql = URLDecoder.decode(
                so.getQuerySql(), StandardCharsets.UTF_8.name()
        );
        StateDefine stateDefine = so.getStateDefine();

        Table table = tableEnv.sqlQuery(querySql);
        TableSchema tableSchema = table.getSchema();
        List<TableColumn> tcs = tableSchema.getTableColumns();
        TypeInformation<?>[] types = new TypeInformation[tcs.size()];
        String[] fieldNames = new String[tcs.size()];

        for (int i = 0; i < tcs.size(); i++) {
            TableColumn tc = tcs.get(i);
            fieldNames[i] = tc.getName();
            DataType dt = tc.getType();
            types[i] = TypeInformation.of(dt.getConversionClass());
        }
        DataStream<Tuple2<Boolean, Row>> ds = tableEnv.toRetractStream(table, Row.class);

        SingleOutputStreamOperator<Row> sos = ds
                .map(tp2 -> tp2.f1) //getFlatenRowFromRetarctRow(tp2)
                .uid("to_retract_stream_map")
                .returns(new RowTypeInfo(types, fieldNames))
                .uid("to_retract_stream_row_type_info");

        KeyedStream<Row, String> ks = sos.keyBy(row -> String.valueOf(row.getField(0)));
//        WindowedStream<Row, String, TimeWindow> ws = ks.timeWindow(Time.seconds(
//                stateDefine.getWindowSizeSeconds()
//        ));

        TypeInformation<?>[] types2 = new TypeInformation[4];
        types2[0] = TypeInformation.of(new TypeHint<Long>() {});
        types2[1] = TypeInformation.of(new TypeHint<Double>() {});
        types2[2] = TypeInformation.of(new TypeHint<Double>() {});
        types2[3] = TypeInformation.of(new TypeHint<String>() {});
        String[] fieldNames2 = new String[4];
        fieldNames2[0] = "product_id";
        fieldNames2[1] = "rise_percent";
        fieldNames2[2] = "buy_percent";
        fieldNames2[3] = "dim_name";

        SingleOutputStreamOperator<Row> sosApplied = ks.process(new CustomStateProcessFunc(stateDefine))
                .uid("custom_state_process_func_apply")
                .returns(new RowTypeInfo(types2, fieldNames2))
                .uid("custom_state_process_func_returns");
        String view = "after_state_calculate";
        tableEnv.createTemporaryView(view, sosApplied);
        String finalSql = StringUtils.join(
                "insert into ", so.getOutputTableName(), " select * from ", view
        );

        LOG.info(StringUtils.join("convert to retract stream final sql: ", finalSql));
        tableEnv.executeSql(finalSql);
    }


}
