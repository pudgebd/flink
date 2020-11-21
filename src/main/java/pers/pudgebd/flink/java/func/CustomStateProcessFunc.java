package pers.pudgebd.flink.java.func;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.constants.enums.ActionTypeEnum;
import pers.pudgebd.flink.java.constants.enums.TimeRangeEnum;
import pers.pudgebd.flink.java.pojo.custom.state.config.*;
import pers.pudgebd.flink.java.utils.FieldUtils;
import pers.pudgebd.flink.java.utils.MapUtils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class CustomStateProcessFunc extends KeyedProcessFunction<String, Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomStateProcessFunc.class);
    private static final String TIME_SEPARATE = "_ts_";
    private static final int TIME_SEPARATE_LEN = TIME_SEPARATE.length();

    private static final ScriptEngineManager MGR = new ScriptEngineManager();
    private static final ScriptEngine ENGINE = MGR.getEngineByName("JavaScript");

    private transient MapState<String, Map<String, String>> customMapState;

    private StateDefine stateDefine;

    public CustomStateProcessFunc(StateDefine stateDefine) {
        this.stateDefine = stateDefine;
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Row> outputs) throws Exception {
        try {
            Integer keyByFieldPos = stateDefine.getKeyByFieldPos();
            String mapStateKey = String.valueOf(
                    row.getField(keyByFieldPos)
            );
            if ("null".equalsIgnoreCase(mapStateKey)) {
                LOG.warn(StringUtils.join("find keyVal is 'null' string"));
                return;
            }
            if (!customMapState.contains(mapStateKey)) {
                customMapState.put(mapStateKey, new HashMap<>());
            }
            Map<String, String> customMap = customMapState.get(mapStateKey);
            customMap = updateMapState(customMap, row);
            customMapState.put(mapStateKey, customMap);

            List<Row> outputRows = calculateOuput(customMap, mapStateKey);
            for (Row ele : outputRows) {
                outputs.collect(ele);
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private Map<String, String> updateMapState(Map<String, String> customMap, Row row) throws Exception {
        List<StateValMapEntry> stateValMapEntrys = stateDefine.getStateValMapEntrys();

        for (StateValMapEntry svme : stateValMapEntrys) {
            String keyName = svme.getKeyName();
            Integer sfPos = svme.getSourceFieldPos();
            String fieldVal = String.valueOf(
                    row.getField(sfPos)
            );
            String targetSrcVal = svme.getTargetSrcValue();
            String actionType = svme.getActionType();

            String timeRange = svme.getTimeRange();
            TimeRangeEnum timeRangeEnum = TimeRangeEnum.valueOf(timeRange.toUpperCase());
            if (timeRangeEnum == null) {
                throw new Exception(StringUtils.join(
                        "unknown timeRange: ", timeRange
                ));
            }
            if (ActionTypeEnum.FIRST.toString().equalsIgnoreCase(actionType)) {
                actionTypeIsFirst(customMap, keyName, fieldVal, timeRangeEnum);

            } else if (ActionTypeEnum.LAST.toString().equalsIgnoreCase(actionType)) {
                customMap.put(keyName, fieldVal);

            } else if (ActionTypeEnum.COUNT.toString().equalsIgnoreCase(actionType)) {
                if (StringUtils.isNotBlank(targetSrcVal)) {
                    if (targetSrcVal.equalsIgnoreCase(fieldVal)) {
                        MapUtils.fillStrStrMapAddUpVal(customMap, keyName, "1");
                    }
                } else {
                    MapUtils.fillStrStrMapAddUpVal(customMap, keyName, "1");
                }

            } else if (ActionTypeEnum.COUNT_ALL.toString().equalsIgnoreCase(actionType)) {
                MapUtils.fillStrStrMapAddUpVal(customMap, keyName, "1");

            } else if (ActionTypeEnum.STATE_VAL_MAP_KEY.toString().equalsIgnoreCase(actionType)) {
                customMap.put(keyName, fieldVal);
            }
        }
        return customMap;
    }

    private void actionTypeIsFirst(Map<String, String> customMap, String keyName,
                                   String fieldVal, TimeRangeEnum timeRangeEnum) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat(timeRangeEnum.getFormat());
        Calendar nowCal = Calendar.getInstance();
        String currDt = dateFormat.format(nowCal.getTime());
        nowCal.setTime(dateFormat.parse(currDt));
        String existValAndDt = customMap.get(keyName);

        if (StringUtils.isBlank(existValAndDt)) {
            customMap.put(keyName, StringUtils.join(fieldVal, TIME_SEPARATE, currDt));
        } else {
            int idx = existValAndDt.lastIndexOf(TIME_SEPARATE);
            String existVal = existValAndDt.substring(0, idx);
            String existDt = existValAndDt.substring(idx + TIME_SEPARATE_LEN);

            Calendar existCal = Calendar.getInstance();
            existCal.setTime(
                    dateFormat.parse(existDt)
            );
            if (nowCal.after(existCal)) {
                customMap.put(keyName, StringUtils.join(fieldVal, TIME_SEPARATE, currDt));
            }
        }
    }


    private List<Row> calculateOuput(Map<String, String> customMap, String mapStateKey) throws Exception {
        StateOuput stateOuput = stateDefine.getStateOuput();
        OutputCondition outputCondi = stateOuput.getOutputCondition();
        String outputTriggerCalcExpre = outputCondi.getCalculateExpre();

        String calcResult = getCalcResultFrom(customMap, outputTriggerCalcExpre);
        List<Row> outputRows = new ArrayList<>();

        if (calcResult != null && calcResult.equalsIgnoreCase(outputCondi.getWhatCalcResultThenOutput())) {
            List<OutputField> outputFields = stateOuput.getOutputFields();
            Row row = new Row(outputFields.size());

            for (int i = 0; i < outputFields.size(); i++) {
                try {
                    OutputField of = outputFields.get(i);
                    String fieldCalcExpre = of.getCalculateExpre();
                    if (StringUtils.isBlank(fieldCalcExpre)) {
                        row.setField(i, null);
                        continue;
                    }
                    if (ActionTypeEnum.BE_STATE_KEY.toString().equalsIgnoreCase(fieldCalcExpre)) {
                        row.setField(i, FieldUtils.convertStr(mapStateKey, of.getDataType()));

                    } else if (ActionTypeEnum.STATE_VAL_MAP_KEY.toString().equalsIgnoreCase(fieldCalcExpre)) {
                        String fieldVal = customMap.get(of.getFieldName());
                        row.setField(i, FieldUtils.convertStr(fieldVal, of.getDataType()));

                    } else {
                        String currFieldCalcVal = getCalcResultFrom(customMap, fieldCalcExpre);
                        row.setField(i, FieldUtils.convertStr(currFieldCalcVal, of.getDataType()));
                    }
                } catch (Exception e) {
                    row.setField(i, null);
                    LOG.error(StringUtils.join(
                            "----- calculateOuput error when output row.setField"
                    ), e);
                }
            }

            outputRows.add(row);
        }
        return outputRows;
    }

    private String getCalcResultFrom(Map<String, String> customMap, String calcExpre) throws Exception {
        for (Map.Entry<String, String> entry : customMap.entrySet()) {
            String entryVal = entry.getValue();
            int idx = entryVal.lastIndexOf(TIME_SEPARATE);
            if (idx > 0) {
                entryVal = entryVal.substring(0, idx);
            }
            calcExpre = calcExpre.replace(entry.getKey(), entryVal);
        }
        String calcResult = null;
        try {
            calcResult = String.valueOf(
                    ENGINE.eval(calcExpre)
            );
            if (NumberUtils.isNumber(calcResult) && calcExpre.contains(".")) {
                BigDecimal bd = new BigDecimal(calcResult);
                calcResult = bd.setScale(5, BigDecimal.ROUND_HALF_UP).toPlainString();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
        return calcResult;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Map<String, String>> mapStateDescriptor =
                new MapStateDescriptor<String, Map<String, String>>(
                        "customMapState",
                        TypeInformation.of(new TypeHint<String>() {
                        }),
                        TypeInformation.of(new TypeHint<Map<String, String>>() {
                        })
                );
        customMapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }


}
