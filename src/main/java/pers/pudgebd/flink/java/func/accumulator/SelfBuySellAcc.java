package pers.pudgebd.flink.java.func.accumulator;

import lombok.Data;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.HashMap;
import java.util.Map;

@Data
public class SelfBuySellAcc {

    //所有买卖总金额
    private MutableLong sum = new MutableLong(0L);

    private Map<String, Long> acctIdBuyMap = new HashMap<>();
    private Map<String, Long> acctIdSellMap = new HashMap<>();

}
