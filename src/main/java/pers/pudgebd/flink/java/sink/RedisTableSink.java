package pers.pudgebd.flink.java.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;

public class RedisTableSink implements TableSink<String> {

    @Override
    public TableSink<String> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }
}
