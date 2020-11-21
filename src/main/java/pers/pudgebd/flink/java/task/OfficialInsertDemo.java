package pers.pudgebd.flink.java.task;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import pers.pudgebd.flink.java.sql.CrtTblSqls;

import java.util.concurrent.CompletableFuture;

public class OfficialInsertDemo {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(CrtTblSqls.KAFKA_SOURCE);
        tEnv.executeSql(CrtTblSqls.DIM_MYSQL);

//        String sql = "insert into insert_test select id, first, last, cast(user_id as double) as score " +
//                "from kafka_source";
        String sql = "insert into dim_mysql select id, first as name, id as user_id, 1.1 as score " +
                "from kafka_source";
        TableResult tableResult1 = tEnv.executeSql(sql);
        System.out.println(tableResult1.getJobClient().get().getJobStatus());
    }


    public static void main2(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(CrtTblSqls.INSERT_TEST);

        String sql = "insert into insert_test values (1, 'a', 'b', 1.1), (2, 'a', 'b', 1.2)";
        TableResult tableResult1 = tEnv.executeSql(sql);
        CompletableFuture<JobStatus> cf = tableResult1.getJobClient().get().getJobStatus();
        System.out.println(cf.isCancelled());
        System.out.println(cf.isCompletedExceptionally());
        System.out.println(cf.isDone());
    }

}
