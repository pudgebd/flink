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
public class CustomStateConfig implements Serializable {

    private String rawSqls;
    private String createTableSqls;
    private List<StateAndOutput> stateAndOutputs;


}
