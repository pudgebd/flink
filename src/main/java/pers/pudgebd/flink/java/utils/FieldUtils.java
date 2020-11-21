package pers.pudgebd.flink.java.utils;

import org.apache.commons.lang3.StringUtils;

public class FieldUtils {

    public static Object convertStr(String currFieldCalcVal, String dataType) {
        if (StringUtils.isBlank(currFieldCalcVal)) {
            return currFieldCalcVal;
        }

        if ("string".equalsIgnoreCase(dataType)) {
            return currFieldCalcVal;

        } else if ("varchar".equalsIgnoreCase(dataType)) {
            return currFieldCalcVal;

        } else if ("int".equalsIgnoreCase(dataType)) {
            return Integer.parseInt(currFieldCalcVal);

        } else if ("long".equalsIgnoreCase(dataType)) {
            return Long.parseLong(currFieldCalcVal);

        } else if ("double".equalsIgnoreCase(dataType)) {
            return Double.parseDouble(currFieldCalcVal);

        } else if ("boolean".equalsIgnoreCase(dataType)) {
            return Boolean.parseBoolean(currFieldCalcVal);
        }

        return currFieldCalcVal;
    }
}
