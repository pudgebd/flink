package test;


import com.haizhi.streamx.app.common.constant.PropKey;
import com.haizhi.streamx.flinkcluster.client.FlinkOnYarnSubmitClient;
import com.haizhi.streamx.flinkcluster.client.bean.HzFlinkApplicatonModeSubmitPara;
import com.haizhi.streamx.sqlparser.common.util.CommonSqlUtils;
import com.haizhi.streamx.sqlparser.spark.statement.lineage.SparkStmtData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//@RunWith(SpringRunner.class)
//@SpringBootTest(classes = StreamxEngineServerApplication.class)
public class SubmitSqlTest {

//    @Autowired
//    private FlinkJobBasicService flinkJobBasicService;

    static String currVersion = "1.2.1";
    static String appName = null;
    static String encodedRawSqls = null;
    static String rawSqls = null;
    static String jarAppMainClass = null;
    static {
        try {
//            appName = "cq_test_sql_hive_dim";
//            String sqlFilePath = "/Users/chenqian/work_doc/sqls/flink_sql/test_hive_dim.sql";
//            appName = "test_map_state";
//            String sqlFilePath = "/Users/chenqian/work_doc/sqls/flink_sql/test_map_state.sql";
//            appName = "test_two_insert";
//            String sqlFilePath = "/Users/chenqian/work_doc/sqls/flink_sql/test_two_insert.sql";
//            appName = "cq_customer_sql_num_01";
//            String sqlFilePath = "/Users/chenqian/work_doc/sqls/customer_sql/num_01.sql";
            appName = "TestJarApp";
            jarAppMainClass = "pers.pudgebd.flink.java.task.TestJarApp";

            String sqlFilePath = "/Users/chenqian/Downloads/Untitled-9";
            rawSqls = IOUtils.toString(new FileInputStream(sqlFilePath));
            encodedRawSqls = URLEncoder.encode(rawSqls, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void submitSqlApplicationMode() throws Exception {
        File yarnConfDir = new File("/Users/chenqian/work_doc/cluster/sz/conf");
        Map<String,InputStream> yarnConfInputs = new HashMap<>();
        for (File confFile : yarnConfDir.listFiles()) {
            yarnConfInputs.put(confFile.getName(),new FileInputStream(confFile));
        }
        Map<String, Object> flinkConfMap = new HashMap<>();
        flinkConfMap.put(YarnConfigOptions.APPLICATION_NAME.key(), appName);
        flinkConfMap.put(DeploymentOptions.TARGET.key(), YarnDeploymentTarget.APPLICATION.getName());
        flinkConfMap.put(YarnConfigOptions.APPLICATION_QUEUE.key(), "root.default");
        if (StringUtils.isBlank(jarAppMainClass)) {
            flinkConfMap.put(PipelineOptions.JARS.key(), "hdfs://cdh601:8020/streamx-test/platform/flink_2.12-1.11.2/streamx-app-sql-" + currVersion + "-jar-with-dependencies.jar");
        } else {
            flinkConfMap.put(PipelineOptions.JARS.key(), "hdfs://cdh601:8020/streamx-test/platform/flink_2.12-1.11.2/flink.jar");
        }
        flinkConfMap.put(JobManagerOptions.JVM_HEAP_MEMORY.key(), "512mb");
        flinkConfMap.put(TaskManagerOptions.TASK_HEAP_MEMORY.key(), "512mb");
        flinkConfMap.put(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "512mb");
        flinkConfMap.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), 1);
        flinkConfMap.put(CoreOptions.DEFAULT_PARALLELISM.key(), 1);
        //要从 tableConfig.getConfiguration().setInteger("table.exec.resource.default-parallelism", 3);
//        flinkConfMap.put(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 3);

        flinkConfMap.putIfAbsent(CheckpointingOptions.STATE_BACKEND.key(), "rocksdb");
        flinkConfMap.putIfAbsent(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER.key(), "true");

        String ckPath = StringUtils.join("hdfs://cdh601:8020/user/chenqian/checkpoints/cq_local_submit_app_mode/" + appName);
        flinkConfMap.putIfAbsent(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), ckPath);
        flinkConfMap.putIfAbsent(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT.key(), "RETAIN_ON_CANCELLATION");
        flinkConfMap.putIfAbsent(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key(), "30000");
        flinkConfMap.putIfAbsent(ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key(), "EXACTLY_ONCE");
        flinkConfMap.putIfAbsent(RestartStrategyOptions.RESTART_STRATEGY.key(), "fixed-delay");
        flinkConfMap.putIfAbsent(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS.key(), "3");
        flinkConfMap.putIfAbsent(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY.key(), "10 s");
        if (StringUtils.isBlank(jarAppMainClass)) {
            flinkConfMap.put(YarnConfigOptions.PROVIDED_LIB_DIRS.key(),
                    "hdfs://cdh601:8020/streamx-test/flink-common/flink_2.12-1.11.2;" +
                            "hdfs://cdh601:8020/streamx-test/flink-datasource/flink_2.12-1.11.2/jdbc;" +
                            "hdfs://cdh601:8020/streamx-test/flink-datasource/flink_2.12-1.11.2/kafka;" +
//                            "hdfs://cdh601:8020/flink-dist/platform-jars/flink-1.11-scala-2.12/hbase-1.4.13;" +
//                            "hdfs://cdh601:8020/flink-dist/platform-jars/flink-1.11-scala-2.12/hadoop-2.7.4;" +
//                            "hdfs://cdh601:8020/flink-dist/platform-jars/flink-1.11-scala-2.12/hive-2.1.1" +
                           "");
        } else {
            flinkConfMap.put(YarnConfigOptions.PROVIDED_LIB_DIRS.key(),
                    "hdfs://cdh601:8020/streamx-test/flink-common/flink_2.12-1.11.2");
        }

        String[] progArgs = {PropKey.RAW_SQLS, encodedRawSqls,
                "-minIdleStateRetentionTime", "46400000",
                "-maxIdleStateRetentionTime", "86400000"};

        HzFlinkApplicatonModeSubmitPara appModeSubmitPara = new HzFlinkApplicatonModeSubmitPara();

        if (StringUtils.isBlank(jarAppMainClass)) {
            appModeSubmitPara.setApplicationClassName("com.haizhi.streamx.app.sql.PlatformAppMain");
        } else {
            appModeSubmitPara.setApplicationClassName(jarAppMainClass);
        }
        appModeSubmitPara.setYarnResourceinputStreams(yarnConfInputs);
        appModeSubmitPara.setProgramArguments(progArgs);
        appModeSubmitPara.setFlinkConfiguration(flinkConfMap);
        appModeSubmitPara.setYarnSubmitUser("work");

        String yarnAppId = FlinkOnYarnSubmitClient.createFlinkJobByYarnClientApplication(appModeSubmitPara);
        System.out.println(yarnAppId);
    }


    @Test
    public void checkSqlValid() throws Exception {
        CommonSqlUtils.checkSqlValid(rawSqls);
        System.out.println();
    }


    @Test
    public void getSqlsFromRawStr() throws Exception {
        List<String> sqlArr = CommonSqlUtils.getSqlsFromRawStr(rawSqls);
        for (String sql : sqlArr) {
            try {
                SparkStmtData ssd = CommonSqlUtils.parseSql(sql);
                System.out.println();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void localMode() throws Exception {
        String[] args = new String[2];
        args[0] = PropKey.RAW_SQLS;
        args[1] = encodedRawSqls;
    }

}
