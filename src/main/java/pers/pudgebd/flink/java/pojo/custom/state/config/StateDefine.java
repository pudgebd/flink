package pers.pudgebd.flink.java.pojo.custom.state.config;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Data
@ToString(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class StateDefine implements Serializable {

    private Integer keyByFieldPos;
    private String stateType;
    private String stateValType;
    private Long windowSizeSeconds;
    private List<StateValMapEntry> stateValMapEntrys;
    private StateOuput stateOuput;

}
