package pers.pudgebd.flink.java.func;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.func.accumulator.SelfBuySellAcc;

import java.math.BigDecimal;
import java.util.Map;


public class AlertSelfBuySellUdaf extends AggregateFunction<Double, SelfBuySellAcc> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertSelfBuySellUdaf.class);

    @Override
    public Double getValue(SelfBuySellAcc accumulate) {
        long sum = accumulate.getSum().longValue();
        Map<String, Long> acctIdBuyMap = accumulate.getAcctIdBuyMap();
        Map<String, Long> acctIdSellMap = accumulate.getAcctIdSellMap();
        long selfBsMoney = 0L;

        for (Map.Entry<String, Long> entry : acctIdBuyMap.entrySet()) {
            String accId = entry.getKey();
            Long buyMoney = entry.getValue();
            Long sellMoney = acctIdSellMap.get(accId);
            if (buyMoney != null && sellMoney != null) {
                selfBsMoney += buyMoney;
                selfBsMoney += sellMoney;
            }
        }

        if (sum <= 0) {
           return 0D;
        }
        return new BigDecimal(selfBsMoney)
                .divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP)
                .doubleValue();
    }

    @Override
    public SelfBuySellAcc createAccumulator() {
        return new SelfBuySellAcc();
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }


    private void accOrRetract(SelfBuySellAcc accumulate, boolean isAcc, Long orderType, String acctId,
                              String tradeDir, Long tradePrice, Long tradeVol) {
        if (orderType == null || tradePrice == null || tradeVol == null) {
            //上游没join上
            //LOG.error(StringUtils.join("存在null，orderType: ", orderType,
            //        ", tradePrice: ", tradePrice, ", tradeVol: ", tradeVol));
            return;
        }
        if (StringUtils.isAnyBlank(acctId, tradeDir)) {
            LOG.error(StringUtils.join("存在null或空，acctId: ", acctId,
                    ", tradeDir: ", tradeDir));
            return;
        }
        //0:订单；1：撤单
        boolean add = orderType == 0L;
        tradeVol = tradeVol / 1000;
        if (tradeVol <= 0) {
            LOG.error(StringUtils.join("tradeVol: ", tradeVol, ", tradeVol / 1000 <= 0"));
            return;
        }
        long curMoney = tradePrice * tradeVol;
        if (!add) {
            curMoney = - curMoney;
        }
        accumulate.getSum().add(curMoney);

        Map<String, Long> acctIdBuyMap = accumulate.getAcctIdBuyMap();
        Map<String, Long> acctIdSellMap = accumulate.getAcctIdSellMap();
        if (!isAcc) {
            //不是收集，就是撤回，撤回上次的累加
            curMoney = - curMoney;
            accumulate.getSum().add(curMoney);
        }
        if ("b".equalsIgnoreCase(tradeDir)) {
//            MapUtils.fillKeyLongMapAddUpVal(acctIdBuyMap, acctId, curMoney);
        } else if ("s".equalsIgnoreCase(tradeDir)) {
//            MapUtils.fillKeyLongMapAddUpVal(acctIdSellMap, acctId, curMoney);
        } else {
            LOG.error("无效的 tradeDir：" + tradeDir);
        }
    }


    public void accumulate(SelfBuySellAcc accumulate, Long orderType, String acctId,
                           String tradeDir, Long tradePrice, Long tradeVol, boolean isAcc) {
        accOrRetract(accumulate, isAcc, orderType, acctId, tradeDir,
                tradePrice, tradeVol);
    }


    public void retract(SelfBuySellAcc accumulate, Long orderType, String acctId,
                        String tradeDir, Long tradePrice, Long tradeVol, boolean isAcc) {
        accOrRetract(accumulate, isAcc, orderType, acctId, tradeDir,
                tradePrice, tradeVol);
    }

    public void merge(SelfBuySellAcc acc, Iterable<SelfBuySellAcc> it) {
        MutableLong sum = acc.getSum();
        Map<String, Long> acctIdBuyMap = acc.getAcctIdBuyMap();
        Map<String, Long> acctIdSellMap = acc.getAcctIdSellMap();

        for (SelfBuySellAcc otherAcc : it) {
            MutableLong otherSum = otherAcc.getSum();
            Map<String, Long> otherAcctIdBuyMap = otherAcc.getAcctIdBuyMap();
            Map<String, Long> otherAcctIdSellMap = otherAcc.getAcctIdSellMap();

            sum.add(otherSum.getValue());
            foreachStrLongMap(acctIdBuyMap, otherAcctIdBuyMap);
            foreachStrLongMap(acctIdSellMap, otherAcctIdSellMap);
        }
    }

    private void foreachStrLongMap(Map<String, Long> toMap, Map<String, Long> fromMap) {
        for (Map.Entry<String, Long> entry : fromMap.entrySet()) {
//            MapUtils.fillKeyLongMapAddUpVal(toMap, entry.getKey(), entry.getValue());
        }
    }

    public void resetAccumulator(SelfBuySellAcc acc) {
        acc.getSum().setValue(0L);
        acc.getAcctIdBuyMap().clear();
        acc.getAcctIdSellMap().clear();
    }
}
