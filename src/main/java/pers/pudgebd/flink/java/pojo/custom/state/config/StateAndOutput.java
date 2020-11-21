package pers.pudgebd.flink.java.pojo.custom.state.config;

import lombok.*;

import java.io.Serializable;

@Data
@ToString(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class StateAndOutput implements Serializable {

    private String querySql;
    private String outputTableName;
    private StateDefine stateDefine;

}
