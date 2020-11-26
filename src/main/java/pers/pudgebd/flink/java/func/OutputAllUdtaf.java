package pers.pudgebd.flink.java.func;

import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import pers.pudgebd.flink.java.func.accumulator.OutputAllAcc;


//1.11.2 只能在代码中使用 udtaf，不能在sql中使用
//        Table selectTbl = table.window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
//                .groupBy($("w"), $("order_no"))
//                .flatAggregate("output_all_udtaf(sec_code, pbu) as arr")
//                .select($("order_no"), $("arr"));
//
//        tableEnv.createTemporaryView("selectTbl", selectTbl);
//        Table tbl = tableEnv.sqlQuery("select order_no, arr[1] as sec_code, arr[2] as pbu from selectTbl");
//
//        TypeInformation<?>[] types = new TypeInformation[3];
//        String[] fieldNames = new String[3];
//        types[0] = TypeInformation.of(new TypeHint<Long>() {});
//        types[1] = TypeInformation.of(new TypeHint<String>() {});
//        types[2] = TypeInformation.of(new TypeHint<Double>() {});
//        fieldNames[0] = "order_no";
//        fieldNames[1] = "sec_code";
//        fieldNames[2] = "pbu";
//
//        DataStream<Row> ds = tableEnv.toAppendStream(tbl, Row.class);
//        SingleOutputStreamOperator<Row> sos = ds
//                .map(tp2 -> tp2)
//                .returns(new RowTypeInfo(types, fieldNames));
//
//        tableEnv.createTemporaryView("tmp", sos);
public class OutputAllUdtaf extends TableAggregateFunction<Object[], OutputAllAcc> {


    @Override
    public OutputAllAcc createAccumulator() {
        return new OutputAllAcc();
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

//    public void accumulate(OutputAllAcc acc, String s, Double d) {
//        acc.list.add(Tuple2.of(s, d));
//    }

    public void accumulate(OutputAllAcc rowsAcc, Object... objs) {
        if (objs == null || objs.length == 0) {
            return;
        }
        rowsAcc.list.add(objs);
    }


    public void merge(OutputAllAcc acc, Iterable<OutputAllAcc> iterable) {
        for (OutputAllAcc otherAcc : iterable) {
            acc.list.addAll(otherAcc.list);
        }
    }

    public void emitValue(OutputAllAcc rowsAcc, Collector<Object[]> out) {
        for (Object[] ele : rowsAcc.list) {
            out.collect(ele);
        }
    }

}
