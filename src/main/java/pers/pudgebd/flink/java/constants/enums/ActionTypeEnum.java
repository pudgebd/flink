package pers.pudgebd.flink.java.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ActionTypeEnum {

    FIRST, LAST, COUNT, COUNT_ALL, STATE_VAL_MAP_KEY, BE_STATE_KEY;

}
