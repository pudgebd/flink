package pers.pudgebd.flink.java.pojo.custom.state.config;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class StateValMapEntry implements Serializable {

    private String keyName;
    private String valDataType;
    private Integer sourceFieldPos;
    private String targetSrcValue;
    private String timeRange;
    private String actionType;


}
