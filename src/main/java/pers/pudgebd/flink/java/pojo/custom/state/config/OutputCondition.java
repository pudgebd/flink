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
public class OutputCondition implements Serializable {

    private String whatCalcResultThenOutput;
    private String calculateExpre;

}
