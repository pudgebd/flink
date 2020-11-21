package pers.pudgebd.flink.java.hive.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class HiveLookupSrc implements LookupableTableSource<Row> {

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return null;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return null;
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public DataType getProducedDataType() {
        return null;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    @Override
    public String explainSource() {
        return null;
    }
}
