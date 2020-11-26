package pers.pudgebd.flink.java.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.constants.enums.StateTypeEnum;
import pers.pudgebd.flink.java.constants.enums.StateValTypeEnum;
import pers.pudgebd.flink.java.pojo.custom.state.config.CustomStateConfig;
import pers.pudgebd.flink.java.pojo.custom.state.config.StateAndOutput;
import pers.pudgebd.flink.java.pojo.custom.state.config.StateDefine;
import pers.pudgebd.flink.java.task.basic.CustomStateBasicTask;

import java.io.FileInputStream;
import java.net.URLDecoder;
import java.util.List;

public class CustomStateTask extends CustomStateBasicTask {

    private static final Logger LOG = LoggerFactory.getLogger(CustomStateTask.class);

    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String filePath = "/Users/pudgebd/work_doc/platform_app_main_config/test_map_state.json";
        String json = IOUtils.toString(new FileInputStream(filePath));
//        String json = args[0];

        CustomStateConfig customStateConfig = mapper.readValue(json, CustomStateConfig.class);

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        createTables(tableEnv, customStateConfig);

        for (StateAndOutput so : customStateConfig.getStateAndOutputs()) {
            StateDefine stateDefine = so.getStateDefine();
            if (StateTypeEnum.MAP_STATE.toString().equalsIgnoreCase(stateDefine.getStateType())
                && StateValTypeEnum.MAP.toString().equalsIgnoreCase(stateDefine.getStateValType())) {
                createMapStateWithMapVal(streamEnv, tableEnv, so);

            } else {
                LOG.warn(StringUtils.join(
                        "------ unknown stateType: ", stateDefine.getStateType(),
                        " and stateValType: ", stateDefine.getStateValType()
                ));
            }
        }

    }



}
