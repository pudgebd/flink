package pers.pudgebd.flink.java.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum TimeRangeEnum {

    DAY("yyyyMMdd");

    private String format;

}
